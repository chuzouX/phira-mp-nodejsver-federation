/*
 * MIT License
 * Copyright (c) 2024
 *
 * 对等联邦节点管理器 - 实现多服务器无中心化联机
 * 
 * 设计原则：所有节点完全对等，没有主/从、中心/代理的区分
 * 
 * 核心功能：
 * 1. 双向握手（A连B时，B也会主动回连A，形成对等连接）
 * 2. Gossip节点发现（每个节点分享自己已知的全部节点列表）
 * 3. 节点缓存（持久化到 data/federation_nodes.json，重启后自动恢复连接）
 * 4. 健康检查（定期 ping 所有已知节点，离线恢复后立即重新同步）
 * 5. 双向房间同步（所有节点的房间对所有其他节点可见）
 * 6. 跨服代理（任意节点的玩家可以加入任意其他节点的房间）
 * 7. 事件回调（房间所在服务器向玩家所在服务器推送实时事件）
 * 8. 实时广播（房间创建/删除/更新时立即通知所有节点）
 */

import * as fs from 'fs';
import * as path from 'path';
import type { Logger, RoomManager, UserInfo, ServerCommand, ClientCommand, RoomState } from 'phira-plugin-api';

const ServerCommandType = {
  Pong: 0, Authenticate: 1, Chat: 2, Touches: 3, Judges: 4,
  Message: 5, ChangeState: 6, ChangeHost: 7, CreateRoom: 8,
  JoinRoom: 9, OnJoinRoom: 10, LeaveRoom: 11, LockRoom: 12,
  CycleRoom: 13, SelectChart: 14, RequestStart: 15, Ready: 16,
  CancelReady: 17, Played: 18, Abort: 19,
};

const ClientCommandType = {
  Ping: 0, Authenticate: 1, Chat: 2, Touches: 3, Judges: 4,
  CreateRoom: 5, JoinRoom: 6, LeaveRoom: 7, LockRoom: 8,
  CycleRoom: 9, SelectChart: 10, RequestStart: 11, Ready: 12,
  CancelReady: 13, Played: 14, Abort: 15, GameResult: 16,
};

// ====================== 类型定义 ======================

export interface FederationConfig {
  enabled: boolean;
  seedNodes: string[];
  secret: string;
  nodeId: string;
  nodeUrl: string;
  instanceId?: string; // 运行时唯一ID，重启后改变
  healthInterval: number;
  syncInterval: number;
  serverName: string;
  allowLocal: boolean;
}

export interface FederationNode {
  id: string;
  url: string;
  instanceId?: string; // 对方节点的实例ID
  serverName: string;
  lastSeen: number;
  status: 'online' | 'offline' | 'unknown';
  addedAt: number;
  lastHealthCheck?: number; // 上次健康检查的时间（运行时，不持久化）
}

export interface FederationRoomInfo {
  id: string;
  name: string;
  nodeId: string;
  nodeUrl: string;
  nodeName: string;
  playerCount: number;
  maxPlayers: number;
  state: RoomState;
  locked: boolean;
  cycle: boolean;
  ownerId: number;
  selectedChart?: any; // 当前选中的谱面信息
  messages?: any[]; // 房间事件消息（用于公共屏幕）
  players: { id: number; name: string; avatar?: string }[];
}

/** 本地玩家通过代理加入远程房间的信息 */
interface ProxyPlayerInfo {
  userId: number;
  userInfo: UserInfo;
  roomId: string;
  remoteNodeId: string;
  remoteNodeUrl: string;
}

/** 远程玩家通过联邦加入本地房间的信息 */
interface FederatedPlayerInfo {
  userId: number;
  sourceNodeId: string;
  sourceNodeUrl: string;
  virtualConnectionId: string;
}

// ====================== 联邦管理器 ======================

export class FederationManager {
  private nodes = new Map<string, FederationNode>();
  private remoteRooms = new Map<string, FederationRoomInfo>();
  private proxyPlayers = new Map<number, ProxyPlayerInfo>();       // 本地玩家 -> 远程房间
  private federatedPlayers = new Map<number, FederatedPlayerInfo>(); // 远程玩家 -> 本地房间
  private lastNodeRoomCounts = new Map<string, number>();           // 每节点上次同步的房间数（防止日志刷屏）
  private lastTotalRemoteRoomCount = -1;                            // 上次远程房间总数

  private healthTimer: NodeJS.Timeout | null = null;
  private syncTimer: NodeJS.Timeout | null = null;

  private readonly nodesFile: string;
  private readonly nodeIdFile: string;
  private readonly instanceId: string;

  // 通过 setter 注入，避免循环依赖
  private protocolHandler: any = null;

  constructor(
    private readonly config: FederationConfig,
    private readonly logger: Logger,
    private readonly roomManager: RoomManager,
  ) {
    this.instanceId = `inst_${Date.now()}_${Math.random().toString(36).substring(2, 10)}`;
    // 基于 nodeUrl 生成唯一的ID文件名，避免同目录多服务共享ID
    const urlSuffix = this.config.nodeUrl
      ? '_' + this.config.nodeUrl.replace(/[^a-zA-Z0-9]/g, '_')
      : '';
    this.nodesFile = path.join(process.cwd(), 'data', `federation_nodes${urlSuffix}.json`);
    this.nodeIdFile = path.join(process.cwd(), 'data', `federation_id${urlSuffix}.txt`);

    // 自动生成节点ID（如果未配置）
    if (!this.config.nodeId) {
      this.config.nodeId = this.loadOrCreateNodeId();
    }
  }

  // ==================== 生命周期 ====================

  setProtocolHandler(handler: any): void {
    this.protocolHandler = handler;
  }

  async start(): Promise<void> {
    if (!this.config.enabled) {
      this.logger.info('[联邦] 联邦功能未启用');
      return;
    }

    this.logger.info(`[联邦] 正在启动对等联邦节点 (ID: ${this.config.nodeId}, URL: ${this.config.nodeUrl})`);

    // 加载缓存的节点列表
    this.loadNodes();

    // 从种子节点发现网络
    await this.discoverFromSeeds();

    // 尝试重连缓存中的所有节点（种子节点之外的）
    const seedUrls = new Set(this.config.seedNodes.map(s => s.trim()));
    for (const node of this.nodes.values()) {
      if (!seedUrls.has(node.url) && node.status !== 'online') {
        this.handshakeWithNode(node.url).catch(() => {});
      }
    }

    // 启动定时任务
    this.startHealthChecks();
    this.startRoomSync();

    this.logger.info(`[联邦] 对等联邦节点已启动，已知节点数: ${this.nodes.size}, 在线: ${this.getOnlineNodes().length}`);
  }

  async stop(): Promise<void> {
    if (this.healthTimer) {
      clearInterval(this.healthTimer);
      this.healthTimer = null;
    }
    if (this.syncTimer) {
      clearInterval(this.syncTimer);
      this.syncTimer = null;
    }

    // 清理所有联邦玩家（通知远程服务器）
    for (const [userId] of this.proxyPlayers) {
      await this.proxyLeaveRoom(userId).catch(() => {});
    }

    this.logger.info('[联邦] 联邦节点已停止');
  }

  // ==================== 节点 ID 管理 ====================

  private loadOrCreateNodeId(): string {
    try {
      if (fs.existsSync(this.nodeIdFile)) {
        return fs.readFileSync(this.nodeIdFile, 'utf8').trim();
      }
    } catch { /* 忽略 */ }

    const id = `node_${Date.now()}_${Math.random().toString(36).substring(2, 10)}`;
    try {
      const dir = path.dirname(this.nodeIdFile);
      if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
      fs.writeFileSync(this.nodeIdFile, id);
    } catch (e) {
      this.logger.error(`[联邦] 保存节点ID失败: ${e}`);
    }
    return id;
  }

  getNodeId(): string { return this.config.nodeId; }
  getNodeUrl(): string { return this.config.nodeUrl; }
  getInstanceId(): string { return this.instanceId; }
  getConfig(): FederationConfig { return this.config; }

  // ==================== 节点发现 ====================

  async discoverFromSeeds(): Promise<void> {
    for (const seedUrl of this.config.seedNodes) {
      const trimmed = seedUrl.trim();
      if (!trimmed) continue;

      this.logger.info(`[联邦] 正在从种子节点发现: ${trimmed}`);
      try {
        await this.handshakeWithNode(trimmed);
      } catch (error) {
        this.logger.error(`[联邦] 连接种子节点失败 ${trimmed}: ${error instanceof Error ? error.message : String(error)}`);
      }
    }
  }

  async handshakeWithNode(nodeUrl: string): Promise<boolean> {
    this.logger.info(`[联邦] ⮕ 主动握手: 正在连接 ${nodeUrl} (本节点: ${this.config.nodeId}, URL: ${this.config.nodeUrl})`);
    try {
      const response = await fetch(`${nodeUrl}/api/federation/handshake`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Federation-Secret': this.config.secret,
        },
        body: JSON.stringify({
          nodeId: this.config.nodeId,
          nodeUrl: this.config.nodeUrl,
          serverName: this.config.serverName,
          instanceId: this.instanceId,
        }),
        signal: AbortSignal.timeout(10000),
      });

      if (!response.ok) {
        const body = await response.text().catch(() => '');
        this.logger.warn(`[联邦] ⮕ 握手失败 ${nodeUrl}: HTTP ${response.status} - ${body}`);
        return false;
      }

      const data = await response.json() as any;
      this.logger.info(`[联邦] ⮕ 握手响应: 对方节点 ${data.serverName} (ID: ${data.nodeId}, 实例: ${data.instanceId}), 返回了 ${data.peers?.length ?? 0} 个peers`);

      // 检查实例变化：如果 ID 相同但 instanceId 变了，说明对方重启了
      const existing = this.nodes.get(data.nodeId);
      if (existing && existing.instanceId && data.instanceId && existing.instanceId !== data.instanceId) {
        this.logger.warn(`[联邦] ⮕ 检测到节点 ${data.serverName} 重启 (实例ID从 ${existing.instanceId} 变为 ${data.instanceId})，清理旧会话`);
        this.handleNodeOffline(data.nodeId);
      }

      // 添加该节点
      this.addNode({
        id: data.nodeId,
        url: nodeUrl,
        instanceId: data.instanceId,
        serverName: data.serverName || 'Unknown',
        lastSeen: Date.now(),
        status: 'online',
        addedAt: Date.now(),
      });

      // 从该节点学习其已知的其他节点（gossip）
      if (data.peers && Array.isArray(data.peers)) {
        for (const peer of data.peers) {
          if (peer.id !== this.config.nodeId && !this.nodes.has(peer.id)) {
            this.logger.info(`[联邦] ⮕ 从 ${data.serverName} 发现新节点: ${peer.serverName} (${peer.url})`);
            this.handshakeWithNode(peer.url).catch(err => {
              this.logger.warn(`[联邦] 无法连接新发现的节点 ${peer.url}: ${err instanceof Error ? err.message : String(err)}`);
            });
          }
        }
      }

      this.logger.info(`[联邦] ✅ 主动握手成功: ${data.serverName} (${nodeUrl})`);
      this.saveNodes();

      // ★ 握手成功后立即同步该节点的房间
      const newNode = this.nodes.get(data.nodeId);
      if (newNode) {
        this.logger.info(`[联邦] ⮕ 握手后立即同步 ${data.serverName} 的房间...`);
        await this.syncRoomsFromNode(newNode);
      }

      return true;
    } catch (error) {
      this.logger.error(`[联邦] ⮕ 握手异常 ${nodeUrl}: ${error instanceof Error ? error.message : String(error)}`);
      return false;
    }
  }

  /** 处理来自其他节点的握手请求（被动方） */
  handleIncomingHandshake(data: { nodeId: string; nodeUrl: string; serverName: string; instanceId?: string; isReverse?: boolean }): any {
    const { nodeId, nodeUrl, serverName, instanceId, isReverse } = data;

    this.logger.info(`[联邦] ⬅ 收到握手: 来自 ${serverName} (ID: ${nodeId}, 实例: ${instanceId}, URL: ${nodeUrl}, 反向: ${!!isReverse})`);

    if (nodeId === this.config.nodeId) {
      this.logger.error(`[联邦] ⬅ 握手拒绝: 对方nodeId "${nodeId}" 与本节点相同！` +
        `两个服务器不能使用相同的nodeId。` +
        `请检查是否共享了同一个 data/federation_id 文件或设置了相同的 FEDERATION_NODE_ID。` +
        `(本节点URL: ${this.config.nodeUrl}, 对方URL: ${nodeUrl})`);
      return { error: 'Node ID 冲突: 对方nodeId与本节点相同，请检查配置' };
    }

    // 检查实例变化
    const existing = this.nodes.get(nodeId);
    if (existing && existing.instanceId && instanceId && existing.instanceId !== instanceId) {
      this.logger.warn(`[联邦] ⬅ 检测到节点 ${serverName} 重启 (实例ID从 ${existing.instanceId} 变为 ${instanceId})，清理旧会话`);
      this.handleNodeOffline(nodeId);
    }

    const isNew = !this.nodes.has(nodeId);
    this.logger.info(`[联邦] ⬅ 节点 ${serverName} 是${isNew ? '新' : '已知'}节点`);

    this.addNode({
      id: nodeId,
      url: nodeUrl,
      instanceId: instanceId,
      serverName,
      lastSeen: Date.now(),
      status: 'online',
      addedAt: Date.now(),
    });

    this.saveNodes();

    // ★ 核心：收到握手后，异步反向握手+立即同步该节点的房间
    if (isNew && !isReverse) {
      this.logger.info(`[联邦] ⬅ 新节点首次连接，触发反向握手: ${serverName} (${nodeUrl})`);
      this.reverseHandshake(nodeUrl, nodeId).catch(err => {
        this.logger.error(`[联邦] ⬅ 反向握手失败 ${nodeUrl}: ${err instanceof Error ? err.message : String(err)}`);
      });
    } else if (isNew && isReverse) {
      this.logger.info(`[联邦] ⬅ 收到反向握手确认，双向连接已建立: ${serverName} (${nodeUrl})`);
    } else {
      this.logger.info(`[联邦] ⬅ 已知节点重新握手: ${serverName} (${nodeUrl})`);
    }

    // 无论新旧，都立即异步同步该节点的房间
    const nodeRef = this.nodes.get(nodeId);
    if (nodeRef) {
      this.logger.info(`[联邦] ⬅ 正在从 ${serverName} (${nodeUrl}) 拉取房间列表...`);
      this.syncRoomsFromNode(nodeRef).catch(err => {
        this.logger.error(`[联邦] ⬅ 从 ${serverName} 同步房间失败: ${err instanceof Error ? err.message : String(err)}`);
      });
    } else {
      this.logger.error(`[联邦] ⬅ 严重错误: addNode 后无法在 nodes map 中找到 ${nodeId}`);
    }

    const myPeers = this.getNodes();
    this.logger.info(`[联邦] ⬅ 返回握手响应: 本节点 ${this.config.serverName} (${this.config.nodeId}), 实例: ${this.instanceId}, 共 ${myPeers.length} 个peers`);

    return {
      nodeId: this.config.nodeId,
      instanceId: this.instanceId,
      serverName: this.config.serverName,
      peers: myPeers.map(n => ({
        id: n.id,
        url: n.url,
        serverName: n.serverName,
        status: n.status,
      })),
    };
  }

  /**
   * 反向握手：当收到对方的握手时，我们也主动去连接对方
   * 与 handshakeWithNode 不同的是，这里不会再触发对方的反向握手（防止无限循环）
   */
  private async reverseHandshake(nodeUrl: string, knownNodeId: string): Promise<void> {
    this.logger.info(`[联邦] ↩ 反向握手: 正在回连 ${nodeUrl} (对方ID: ${knownNodeId})`);
    try {
      const response = await fetch(`${nodeUrl}/api/federation/handshake`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Federation-Secret': this.config.secret,
        },
        body: JSON.stringify({
          nodeId: this.config.nodeId,
          nodeUrl: this.config.nodeUrl,
          serverName: this.config.serverName,
          instanceId: this.instanceId,
          isReverse: true,
        }),
        signal: AbortSignal.timeout(10000),
      });

      if (!response.ok) {
        const body = await response.text().catch(() => '');
        this.logger.error(`[联邦] ↩ 反向握手HTTP失败 ${nodeUrl}: ${response.status} - ${body}`);
        return;
      }

      const data = await response.json() as any;
      this.logger.info(`[联邦] ↩ 反向握手成功: 对方 ${data.serverName} (ID: ${data.nodeId}, 实例: ${data.instanceId}), ${data.peers?.length ?? 0} peers`);

      // 检查实例变化
      const existing = this.nodes.get(knownNodeId);
      if (existing && existing.instanceId && data.instanceId && existing.instanceId !== data.instanceId) {
        this.logger.warn(`[联邦] ↩ 检测到节点 ${data.serverName} 重启 (实例ID从 ${existing.instanceId} 变为 ${data.instanceId})，清理旧会话`);
        this.handleNodeOffline(knownNodeId);
      }

      // 更新节点信息
      this.addNode({
        id: data.nodeId,
        url: nodeUrl,
        instanceId: data.instanceId,
        serverName: data.serverName || 'Unknown',
        lastSeen: Date.now(),
        status: 'online',
        addedAt: Date.now(),
      });

      // 从反向握手中也学习新节点
      if (data.peers && Array.isArray(data.peers)) {
        for (const peer of data.peers) {
          if (peer.id !== this.config.nodeId && !this.nodes.has(peer.id)) {
            this.logger.info(`[联邦] ↩ 从反向握手发现新节点: ${peer.serverName} (${peer.url})`);
            this.handshakeWithNode(peer.url).catch(err => {
              this.logger.warn(`[联邦] 连接新发现节点失败 ${peer.url}: ${err instanceof Error ? err.message : String(err)}`);
            });
          }
        }
      }

      this.logger.info(`[联邦] ✅ 与节点 ${knownNodeId} (${nodeUrl}) 双向握手完成`);

      // 反向握手成功后也立即同步对方房间
      const node = this.nodes.get(knownNodeId);
      if (node) {
        this.logger.info(`[联邦] ↩ 反向握手后同步 ${nodeUrl} 的房间...`);
        await this.syncRoomsFromNode(node);
      }
    } catch (error) {
      this.logger.error(`[联邦] ↩ 反向握手异常 ${nodeUrl}: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  /**
   * 从单个节点同步房间（用于新发现节点时立即获取其房间）
   */
  private async syncRoomsFromNode(node: FederationNode): Promise<void> {
    this.logger.debug(`[联邦] 📥 正在从节点 ${node.serverName} (${node.url}) 拉取房间...`);
    try {
      const response = await fetch(`${node.url}/api/federation/rooms`, {
        headers: { 'X-Federation-Secret': this.config.secret },
        signal: AbortSignal.timeout(8000),
      });

      if (!response.ok) {
        const body = await response.text().catch(() => '');
        this.logger.error(`[联邦] 📥 拉取房间失败 ${node.serverName} (${node.url}): HTTP ${response.status} - ${body}`);
        return;
      }

      const data = await response.json() as any;
      if (data.rooms && Array.isArray(data.rooms)) {
        let count = 0;
        for (const room of data.rooms) {
          this.remoteRooms.set(room.id, {
            ...room,
            nodeId: node.id,
            nodeUrl: node.url,
            nodeName: node.serverName,
          });
          count++;
        }
        const lastCount = this.lastNodeRoomCounts.get(node.id) ?? -1;
        if (count !== lastCount) {
          this.logger.info(`[联邦] 📥 从 ${node.serverName} 获取了 ${count} 个房间 (当前远程房间总数: ${this.remoteRooms.size})`);
          if (count > 0) {
            const roomIds = data.rooms.map((r: any) => r.id).join(', ');
            this.logger.info(`[联邦] 📥 房间列表: [${roomIds}]`);
          }
          this.lastNodeRoomCounts.set(node.id, count);
        }
      } else {
        this.logger.warn(`[联邦] 📥 ${node.serverName} 返回了无效的房间数据: ${JSON.stringify(data).substring(0, 200)}`);
      }
    } catch (error) {
      this.logger.error(`[联邦] 📥 从 ${node.serverName} (${node.url}) 拉取房间异常: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  addNode(node: FederationNode): void {
    if (node.id === this.config.nodeId) {
      this.logger.error(`[联邦] ⚠️ 拒绝添加节点: 对方nodeId "${node.id}" 与本节点相同！` +
        `这通常是因为两个服务器共享了同一个 data/federation_id 文件。` +
        `请为每个节点设置不同的 FEDERATION_NODE_ID 或使用不同的工作目录。` +
        `(本节点URL: ${this.config.nodeUrl}, 对方URL: ${node.url})`);
      return;
    }

    const existing = this.nodes.get(node.id);
    if (existing) {
      existing.url = node.url;
      existing.instanceId = node.instanceId || existing.instanceId;
      existing.serverName = node.serverName;
      existing.lastSeen = node.lastSeen;
      existing.status = node.status;
    } else {
      this.nodes.set(node.id, node);
      this.logger.info(`[联邦] 新增节点: ${node.serverName} (${node.url})`);
    }
  }

  removeNode(id: string): void {
    const node = this.nodes.get(id);
    if (node) {
      this.logger.info(`[联邦] 移除节点: ${node.serverName} (${node.url})`);
    }
    this.nodes.delete(id);
    this.lastNodeRoomCounts.delete(id);

    // 清理该节点的远程房间
    for (const [roomId, roomInfo] of this.remoteRooms) {
      if (roomInfo.nodeId === id) {
        this.remoteRooms.delete(roomId);
      }
    }
    this.saveNodes();
  }

  getNodes(): FederationNode[] {
    return Array.from(this.nodes.values());
  }

  getOnlineNodes(): FederationNode[] {
    return this.getNodes().filter(n => n.status === 'online');
  }

  // ==================== 健康检查 ====================

  private startHealthChecks(): void {
    this.healthTimer = setInterval(() => {
      this.checkAllNodes().catch(err => {
        this.logger.error(`[联邦] 健康检查循环出错: ${err}`);
      });
    }, this.config.healthInterval);
    this.logger.info(`[联邦] 健康检查已启动，间隔: ${this.config.healthInterval}ms`);

    // 立即执行一次
    this.checkAllNodes().catch(() => {});
  }

  private async checkAllNodes(): Promise<void> {
    const now = Date.now();
    const THREE_DAYS = 3 * 24 * 60 * 60 * 1000;
    const SEVEN_DAYS = 7 * 24 * 60 * 60 * 1000;
    const FIVE_MINUTES = 5 * 60 * 1000;
    const ONE_HOUR = 60 * 60 * 1000;

    const nodesToRemove: string[] = [];
    const nodesToCheck: FederationNode[] = [];

    for (const node of this.nodes.values()) {
      // 在线或未知状态：每次都检查
      if (node.status === 'online' || node.status === 'unknown') {
        nodesToCheck.push(node);
        continue;
      }

      // 离线节点：分级轮询
      const offlineDuration = now - node.lastSeen;
      const timeSinceLastCheck = now - (node.lastHealthCheck || 0);

      if (offlineDuration >= SEVEN_DAYS) {
        // 离线超过 7 天：自动移除节点记录
        nodesToRemove.push(node.id);
        this.logger.info(`[联邦] 节点 ${node.serverName} (${node.url}) 已离线超过7天，自动移除记录（将在其重新上线或被其他节点广播时重新添加）`);
      } else if (offlineDuration >= THREE_DAYS) {
        // 离线 3-7 天：每小时检查一次
        if (timeSinceLastCheck >= ONE_HOUR) {
          nodesToCheck.push(node);
          this.logger.debug(`[联邦] 节点 ${node.serverName} 离线${Math.floor(offlineDuration / (24 * 60 * 60 * 1000))}天，执行小时级检查`);
        }
      } else {
        // 离线 0-3 天：每 5 分钟检查一次
        if (timeSinceLastCheck >= FIVE_MINUTES) {
          nodesToCheck.push(node);
        }
      }
    }

    // 移除过期节点
    for (const nodeId of nodesToRemove) {
      this.removeNode(nodeId);
    }

    // 执行健康检查并更新 lastHealthCheck
    const promises = nodesToCheck.map(node => {
      node.lastHealthCheck = now;
      return this.checkNode(node);
    });
    await Promise.allSettled(promises);
    this.saveNodes();
  }

  private async checkNode(node: FederationNode): Promise<void> {
    const wasPreviouslyOffline = node.status !== 'online';

    try {
      const response = await fetch(`${node.url}/api/federation/health`, {
        headers: { 'X-Federation-Secret': this.config.secret },
        signal: AbortSignal.timeout(8000),
      });

      if (response.ok) {
        const data = await response.json() as any;
        node.lastSeen = Date.now();
        node.status = 'online';
        node.serverName = data.serverName || node.serverName;

        // 节点恢复上线时立即同步房间
        if (wasPreviouslyOffline) {
          this.logger.info(`[联邦] 节点恢复上线: ${node.serverName} (${node.url})，正在同步房间...`);
          this.syncRoomsFromNode(node).catch(err => {
            this.logger.error(`[联邦] 恢复上线同步房间失败 ${node.serverName}: ${err instanceof Error ? err.message : String(err)}`);
          });
        }

        // 学习新节点（gossip 传播）
        if (data.peers && Array.isArray(data.peers)) {
          for (const peer of data.peers) {
            if (peer.id !== this.config.nodeId && !this.nodes.has(peer.id)) {
              this.logger.info(`[联邦] 从健康检查发现新节点: ${peer.serverName} (${peer.url})`);
              this.handshakeWithNode(peer.url).catch(() => {});
            }
          }
        }
      } else {
        node.status = 'offline';
      }
    } catch {
      if (node.status === 'online') {
        this.logger.warn(`[联邦] 节点离线: ${node.serverName} (${node.url})`);
      }
      node.status = 'offline';
      this.handleNodeOffline(node.id);
    }
  }

  private handleNodeOffline(nodeId: string): void {
    const node = this.nodes.get(nodeId);
    const nodeName = node?.serverName || nodeId;

    // 1. 移除该节点的联邦玩家（在本地房间中的远程玩家）
    for (const [userId, info] of this.federatedPlayers) {
      if (info.sourceNodeId === nodeId) {
        this.logger.info(`[联邦] 节点 ${nodeName} 离线，移除联邦玩家 ${userId}`);
        this.removeIncomingFederatedPlayer(userId);
      }
    }

    // 2. 清理代理玩家（本地玩家在该节点的远程房间中）
    //    当权威服务器意外下线，本地玩家需要被踢出远程房间
    for (const [userId, info] of this.proxyPlayers) {
      if (info.remoteNodeId === nodeId) {
        this.logger.info(`[联邦] 节点 ${nodeName} 离线，清理代理玩家 ${userId} (远程房间: ${info.roomId})`);
        this.proxyPlayers.delete(userId);
        // 通知本地玩家：远程房间已不可用，强制离开
        if (this.protocolHandler) {
          this.protocolHandler.sendCommandToUser(userId, {
            type: ServerCommandType.LeaveRoom,
            result: { ok: true, value: undefined },
          });
        }
      }
    }

    // 3. 移除该节点的远程房间缓存
    let removedRooms = 0;
    for (const [roomId, roomInfo] of this.remoteRooms) {
      if (roomInfo.nodeId === nodeId) {
        this.remoteRooms.delete(roomId);
        removedRooms++;
      }
    }
    if (removedRooms > 0) {
      this.logger.info(`[联邦] 已清理节点 ${nodeName} 的 ${removedRooms} 个远程房间缓存`);
    }
  }

  // ==================== 房间同步 ====================

  private startRoomSync(): void {
    this.syncTimer = setInterval(() => {
      this.syncAllRooms().catch(err => {
        this.logger.error(`[联邦] 房间同步循环出错: ${err}`);
      });
    }, this.config.syncInterval);
    this.logger.info(`[联邦] 房间同步已启动，间隔: ${this.config.syncInterval}ms`);

    // 立即执行一次
    this.syncAllRooms().catch(() => {});
  }

  async syncAllRooms(): Promise<void> {
    const onlineNodes = this.getOnlineNodes();
    const newRemoteRooms = new Map<string, FederationRoomInfo>();

    const promises = onlineNodes.map(async (node) => {
      try {
        const response = await fetch(`${node.url}/api/federation/rooms`, {
          headers: { 'X-Federation-Secret': this.config.secret },
          signal: AbortSignal.timeout(8000),
        });

        if (!response.ok) {
          this.logger.warn(`[联邦] 定时同步: ${node.serverName} 返回 HTTP ${response.status}`);
          // 保留之前缓存的该节点房间，防止临时错误清空
          for (const [roomId, room] of this.remoteRooms) {
            if (room.nodeId === node.id) {
              newRemoteRooms.set(roomId, room);
            }
          }
          return;
        }

        const data = await response.json() as any;
        if (data.rooms && Array.isArray(data.rooms)) {
          for (const room of data.rooms) {
            newRemoteRooms.set(room.id, {
              ...room,
              nodeId: node.id,
              nodeUrl: node.url,
              nodeName: node.serverName,
            });
          }
        }
      } catch (error) {
        this.logger.warn(`[联邦] 定时同步: 从 ${node.serverName} (${node.url}) 拉取失败: ${error instanceof Error ? error.message : String(error)}`);
        // 保留之前缓存的该节点房间
        for (const [roomId, room] of this.remoteRooms) {
          if (room.nodeId === node.id) {
            newRemoteRooms.set(roomId, room);
          }
        }
      }
    });

    await Promise.allSettled(promises);

    const newTotal = newRemoteRooms.size;
    if (newTotal !== this.lastTotalRemoteRoomCount) {
      this.logger.info(`[联邦] 🔄 定时同步完成: 远程房间总数 ${this.lastTotalRemoteRoomCount === -1 ? '初始化' : this.lastTotalRemoteRoomCount} → ${newTotal}`);
      this.lastTotalRemoteRoomCount = newTotal;
    }

    this.remoteRooms = newRemoteRooms;
  }

  getRemoteRooms(): FederationRoomInfo[] {
    return Array.from(this.remoteRooms.values());
  }

  isRemoteRoom(roomId: string): boolean {
    return this.remoteRooms.has(roomId);
  }

  getRemoteRoomInfo(roomId: string): FederationRoomInfo | undefined {
    return this.remoteRooms.get(roomId);
  }

  // ==================== 代理：本地玩家 -> 远程房间 ====================

  isPlayerProxied(userId: number): boolean {
    return this.proxyPlayers.has(userId);
  }

  /**
   * 代理玩家加入远程房间
   * 本地玩家 -> 本服务器 -> HTTP -> 权威服务器
   */
  async proxyJoinRoom(
    userId: number,
    userInfo: UserInfo,
    roomId: string,
    monitor: boolean,
    sendResponse: (cmd: ServerCommand) => void,
  ): Promise<void> {
    const roomInfo = this.remoteRooms.get(roomId);
    if (!roomInfo) {
      sendResponse({
        type: ServerCommandType.JoinRoom,
        result: { ok: false, error: '远程房间不存在或已过期' },
      } as any);
      return;
    }

    try {
      const response = await fetch(`${roomInfo.nodeUrl}/api/federation/proxy/join`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Federation-Secret': this.config.secret,
        },
        body: JSON.stringify({
          roomId,
          userId,
          userInfo: { ...userInfo, monitor },
          sourceNodeId: this.config.nodeId,
          sourceNodeUrl: this.config.nodeUrl,
        }),
        signal: AbortSignal.timeout(15000),
      });

      const data = await response.json() as any;

      if (data.success) {
        // 标记为代理玩家
        this.proxyPlayers.set(userId, {
          userId,
          userInfo,
          roomId,
          remoteNodeId: roomInfo.nodeId,
          remoteNodeUrl: roomInfo.nodeUrl,
        });

        this.logger.info(`[联邦] 玩家 ${userInfo.name} (${userId}) 通过代理加入远程房间 ${roomId} @ ${roomInfo.nodeName}`);

        sendResponse({
          type: ServerCommandType.JoinRoom,
          result: { ok: true, value: data.joinResponse },
        } as any);
      } else {
        sendResponse({
          type: ServerCommandType.JoinRoom,
          result: { ok: false, error: data.error || '加入远程房间失败' },
        } as any);
      }
    } catch (error) {
      this.logger.error(`[联邦] 代理加入房间失败: ${error instanceof Error ? error.message : String(error)}`);
      sendResponse({
        type: ServerCommandType.JoinRoom,
        result: { ok: false, error: '连接远程服务器失败' },
      } as any);
    }
  }

  /**
   * 代理转发命令到远程服务器
   * 本地玩家的命令 -> HTTP -> 权威服务器处理 -> 返回直接响应
   * 广播事件通过 callback 异步推送
   */
  async proxyCommand(
    userId: number,
    command: ClientCommand,
    sendResponse: (cmd: ServerCommand) => void,
  ): Promise<void> {
    const proxyInfo = this.proxyPlayers.get(userId);
    if (!proxyInfo) {
      this.logger.error(`[联邦] 找不到玩家 ${userId} 的代理信息`);
      return;
    }

    // LeaveRoom 特殊处理
    if (command.type === ClientCommandType.LeaveRoom) {
      await this.proxyLeaveRoom(userId, sendResponse);
      return;
    }

    try {
      const response = await fetch(`${proxyInfo.remoteNodeUrl}/api/federation/proxy/command`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Federation-Secret': this.config.secret,
        },
        body: JSON.stringify({
          roomId: proxyInfo.roomId,
          userId,
          command,
          sourceNodeId: this.config.nodeId,
        }),
        signal: AbortSignal.timeout(30000),
      });

      const data = await response.json() as any;

      if (data.success === false && (data.error === '联邦玩家未找到' || data.error === '联邦会话未找到')) {
        this.logger.warn(`[联邦] 远程服务器已丢失玩家 ${userId} 的会话，强制本地退出`);
        this.proxyPlayers.delete(userId);
        sendResponse({
          type: ServerCommandType.LeaveRoom,
          result: { ok: true, value: undefined },
        } as any);
        return;
      }

      if (data.responses && Array.isArray(data.responses)) {
        for (const resp of data.responses) {
          sendResponse(resp);
        }
      }
    } catch (error) {
      this.logger.error(`[联邦] 代理命令转发失败 (玩家 ${userId}): ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  /**
   * 代理玩家离开远程房间
   */
  async proxyLeaveRoom(userId: number, sendResponse?: (cmd: ServerCommand) => void): Promise<void> {
    const proxyInfo = this.proxyPlayers.get(userId);
    if (!proxyInfo) return;

    try {
      await fetch(`${proxyInfo.remoteNodeUrl}/api/federation/proxy/leave`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Federation-Secret': this.config.secret,
        },
        body: JSON.stringify({
          roomId: proxyInfo.roomId,
          userId,
          sourceNodeId: this.config.nodeId,
        }),
        signal: AbortSignal.timeout(10000),
      });
    } catch (error) {
      this.logger.debug(`[联邦] 代理离开房间远程通知失败 (正常现象，可能远程已下线): ${userId}`);
    }

    this.proxyPlayers.delete(userId);
    this.logger.info(`[联邦] 玩家 ${userId} 已清理本地代理状态，离开房间 ${proxyInfo.roomId}`);

    if (sendResponse) {
      sendResponse({
        type: ServerCommandType.LeaveRoom,
        result: { ok: true, value: undefined },
      } as any);
    }
  }

  // ==================== 联邦入站：远程玩家 -> 本地房间 ====================

  /**
   * 处理远程玩家加入本地房间的请求（权威服务器侧）
   */
  handleIncomingJoin(data: {
    roomId: string;
    userId: number;
    userInfo: UserInfo;
    sourceNodeId: string;
    sourceNodeUrl: string;
  }): any {
    const { roomId, userId, userInfo, sourceNodeId, sourceNodeUrl } = data;

    const room = this.roomManager.getRoom(roomId);
    if (!room) return { success: false, error: '房间不存在' };
    if (room.locked) return { success: false, error: '房间已锁定' };
    if (room.state.type !== 'SelectChart') return { success: false, error: '游戏正在进行中' };
    if (room.players.size >= room.maxPlayers) return { success: false, error: '房间已满' };
    if (room.blacklist.includes(userId)) return { success: false, error: '您在该房间的黑名单中' };
    if (room.whitelist.length > 0 && !room.whitelist.includes(userId)) {
      return { success: false, error: '您不在该房间的白名单中' };
    }

    const virtualConnectionId = `federation:${sourceNodeId}:${userId}`;

    // 在 ProtocolHandler 上创建联邦会话
    if (this.protocolHandler) {
      const callbackFn = (cmd: ServerCommand) => {
        this.sendEventCallback(sourceNodeUrl, userId, cmd).catch(err => {
          this.logger.error(`[联邦] 发送事件回调失败 (userId: ${userId}): ${err instanceof Error ? err.message : String(err)}`);
        });
      };

      this.protocolHandler.addVirtualSession(virtualConnectionId, userId, userInfo, callbackFn);
    }

    // 添加玩家到房间
    const added = this.roomManager.addPlayerToRoom(roomId, userId, userInfo, virtualConnectionId);
    if (!added) {
      if (this.protocolHandler) {
        this.protocolHandler.removeVirtualSession(virtualConnectionId);
      }
      return { success: false, error: '加入房间失败' };
    }

    // 记录联邦玩家
    this.federatedPlayers.set(userId, {
      userId,
      sourceNodeId,
      sourceNodeUrl,
      virtualConnectionId,
    });

    // 广播加入事件给房间内所有人
    if (this.protocolHandler) {
      this.protocolHandler.broadcastToRoomById(room.id, {
        type: ServerCommandType.OnJoinRoom,
        user: userInfo,
      } as any);
      this.protocolHandler.broadcastToRoomById(room.id, {
        type: ServerCommandType.Message,
        message: { type: 'JoinRoom', user: userId, name: userInfo.name },
      } as any);
    }

    // 构建加入响应
    const usersInRoom = Array.from(room.players.values()).map(p => p.user);
    const serverUser: UserInfo = {
      id: -1,
      name: this.config.serverName,
      avatar: 'https://phira.5wyxi.com/files/6ad662de-b505-4725-a7ef-72d65f32b404',
      monitor: true,
    };

    this.logger.info(`[联邦] 远程玩家 ${userInfo.name} (${userId}) 从节点 ${sourceNodeId} 加入房间 ${roomId}`);

    // 广播房间变更事件给其他联邦节点
    this.broadcastRoomEvent('room_updated', roomId, this.buildLocalRoomInfo(room)).catch(() => {});

    return {
      success: true,
      joinResponse: {
        state: room.state,
        users: [...usersInRoom, serverUser],
        live: room.live,
      },
    };
  }

  /**
   * 处理远程玩家在本地房间中执行命令（权威服务器侧）
   */
  async handleIncomingCommand(data: {
    roomId: string;
    userId: number;
    command: ClientCommand;
    sourceNodeId: string;
  }): Promise<any> {
    const { userId, command } = data;
    const fedInfo = this.federatedPlayers.get(userId);
    if (!fedInfo) return { success: false, error: '联邦玩家未找到' };

    if (!this.protocolHandler) return { success: false, error: '协议处理器不可用' };

    // 判断是否为异步命令（SelectChart/Played 需要远程获取数据）
    const isAsync =
      command.type === ClientCommandType.SelectChart ||
      command.type === ClientCommandType.Played;

    if (!isAsync) {
      // 同步命令：直接捕获响应
      const responses: ServerCommand[] = [];
      this.protocolHandler.handleMessage(
        fedInfo.virtualConnectionId,
        command,
        (cmd: ServerCommand) => responses.push(cmd),
      );
      return { success: true, responses };
    }

    // 异步命令：使用 Promise 等待响应
    return new Promise<any>((resolve) => {
      const responses: ServerCommand[] = [];
      let resolved = false;

      const timeout = setTimeout(() => {
        if (!resolved) {
          resolved = true;
          resolve({ success: true, responses });
        }
      }, 30000);

      this.protocolHandler.handleMessage(
        fedInfo.virtualConnectionId,
        command,
        (cmd: ServerCommand) => {
          responses.push(cmd);
          if (!resolved) {
            resolved = true;
            clearTimeout(timeout);
            // 短延迟以收集可能的附加响应
            setTimeout(() => resolve({ success: true, responses }), 100);
          }
        },
      );
    });
  }

  /**
   * 处理远程玩家离开本地房间（权威服务器侧）
   */
  handleIncomingLeave(data: { roomId: string; userId: number; sourceNodeId: string }): any {
    const { userId } = data;
    this.removeIncomingFederatedPlayer(userId);
    return { success: true };
  }

  private removeIncomingFederatedPlayer(userId: number): void {
    const fedInfo = this.federatedPlayers.get(userId);
    
    // 即使找不到联邦信息（比如服务器重启了），也要尝试从所有房间中彻底移除该玩家ID
    // 这样做可以确保房主转移逻辑被触发
    if (!fedInfo) {
      this.logger.debug(`[联邦] 尝试强行移除未注册的联邦玩家: ${userId}`);
      const virtualIdPrefix = `federation:`; // 搜索所有联邦虚拟连接
      const connectionId = this.protocolHandler?.getConnectionIdByUserId?.(userId);
      
      if (connectionId && connectionId.startsWith(virtualIdPrefix)) {
        this.logger.info(`[联邦] 发现残留的联邦虚拟连接 ${connectionId}，执行断线处理`);
        this.protocolHandler.handleDisconnection(connectionId);
      } else {
        // 最后保底：直接遍历房间管理器
        this.roomManager.removePlayerFromAllRooms(userId);
      }
    } else {
      // 正常流程：触发断线处理（从房间移除、处理房主迁移等）
      if (this.protocolHandler) {
        // 先获取房间信息，用于广播
        const roomId = this.roomManager.getRoomByUserId(userId)?.id;
        
        this.protocolHandler.handleDisconnection(fedInfo.virtualConnectionId);
        
        // 显式广播玩家离开事件，让其他节点立即同步
        if (roomId) {
          const room = this.roomManager.getRoom(roomId);
          if (room) {
            this.broadcastRoomEvent('player_left', roomId, this.buildLocalRoomInfo(room)).catch(() => {});
          } else {
            // 如果房间也销毁了
            this.broadcastRoomEvent('room_deleted', roomId, { id: roomId }).catch(() => {});
          }
        }
      }
      this.federatedPlayers.delete(userId);
    }
    
    this.logger.info(`[联邦] 远程玩家 ${userId} 已从联邦会话中清理`);
  }

  // ==================== 事件回调 ====================

  /**
   * 向代理服务器发送事件回调（权威服务器 -> 代理服务器）
   * 用于将房间广播事件推送给远程玩家
   */
  private async sendEventCallback(nodeUrl: string, targetUserId: number, command: ServerCommand): Promise<void> {
    try {
      await fetch(`${nodeUrl}/api/federation/proxy/callback`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'X-Federation-Secret': this.config.secret,
        },
        body: JSON.stringify({ targetUserId, command }),
        signal: AbortSignal.timeout(8000),
      });
    } catch (error) {
      this.logger.debug(`[联邦] 事件回调发送失败 (目标用户: ${targetUserId}, 节点: ${nodeUrl})`);
    }
  }

  /**
   * 处理来自权威服务器的事件回调（代理服务器侧）
   * 将事件转发给本地玩家的真实连接
   */
  handleEventCallback(data: { targetUserId: number; command: ServerCommand }): boolean {
    const { targetUserId, command } = data;

    if (!this.protocolHandler) return false;

    return this.protocolHandler.sendCommandToUser(targetUserId, command);
  }

  // ==================== 房间事件广播 ====================

  /**
   * 向所有在线联邦节点广播房间事件（用于远程房间缓存更新）
   */
  async broadcastRoomEvent(eventType: string, roomId: string, data: any): Promise<void> {
    const onlineNodes = this.getOnlineNodes();
    if (onlineNodes.length === 0) {
      this.logger.debug(`[联邦] 📡 无在线节点，跳过广播事件 ${eventType} (房间: ${roomId})`);
      return;
    }

    this.logger.info(`[联邦] 📡 广播事件 ${eventType} (房间: ${roomId}) → ${onlineNodes.length} 个节点: [${onlineNodes.map(n => n.serverName).join(', ')}]`);

    const event = {
      type: eventType,
      sourceNodeId: this.config.nodeId,
      roomId,
      data,
      timestamp: Date.now(),
    };

    const promises = onlineNodes.map(async (node) => {
      try {
        const resp = await fetch(`${node.url}/api/federation/event`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'X-Federation-Secret': this.config.secret,
          },
          body: JSON.stringify(event),
          signal: AbortSignal.timeout(5000),
        });
        if (!resp.ok) {
          this.logger.warn(`[联邦] 📡 事件发送失败 → ${node.serverName}: HTTP ${resp.status}`);
        }
      } catch (err) {
        this.logger.warn(`[联邦] 📡 事件发送异常 → ${node.serverName} (${node.url}): ${err instanceof Error ? err.message : String(err)}`);
      }
    });

    await Promise.allSettled(promises);
  }

  /**
   * 处理来自其他节点的房间事件
   */
  handleIncomingEvent(event: {
    type: string;
    sourceNodeId: string;
    roomId: string;
    data: any;
    timestamp: number;
  }): void {
    this.logger.debug(`[联邦] 📨 收到事件: ${event.type} (房间: ${event.roomId}, 来自节点: ${event.sourceNodeId})`);

    switch (event.type) {
      case 'room_created':
      case 'room_updated': {
        const node = this.nodes.get(event.sourceNodeId);
        if (node && event.data) {
          this.remoteRooms.set(event.roomId, {
            ...event.data,
            nodeId: event.sourceNodeId,
            nodeUrl: node.url,
            nodeName: node.serverName,
          });
          this.logger.debug(`[联邦] 📨 已缓存远程房间 ${event.roomId} (来自 ${node.serverName}), 当前远程房间总数: ${this.remoteRooms.size}`);
        } else {
          this.logger.warn(`[联邦] 📨 无法处理事件: 找不到来源节点 ${event.sourceNodeId} (已知节点: [${Array.from(this.nodes.keys()).join(', ')}])`);
        }
        break;
      }
      case 'room_deleted':
        this.remoteRooms.delete(event.roomId);
        // 如果有代理玩家在这个房间，通知他们
        for (const [userId, info] of this.proxyPlayers) {
          if (info.roomId === event.roomId) {
            this.logger.info(`[联邦] 远程房间 ${event.roomId} 已被销毁，清理代理玩家 ${userId}`);
            this.proxyPlayers.delete(userId);
            // 通知玩家被踢出
            if (this.protocolHandler) {
              this.protocolHandler.sendCommandToUser(userId, {
                type: ServerCommandType.LeaveRoom,
                result: { ok: true, value: undefined },
              });
            }
          }
        }
        break;
      case 'room_state_changed':
      case 'player_joined':
      case 'player_left':
      case 'owner_changed':
      case 'chart_selected':
      case 'game_started':
      case 'game_ended': {
        // 更新远程房间缓存
        const node = this.nodes.get(event.sourceNodeId);
        if (node && event.data) {
          const existing = this.remoteRooms.get(event.roomId);
          if (existing) {
            Object.assign(existing, event.data);
          }
        }
        break;
      }
    }
  }

  // ==================== 辅助方法 ====================

  /** 构建本地房间的联邦信息 */
  buildLocalRoomInfo(room: any): Partial<FederationRoomInfo> {
    const players = Array.from(room.players.values()).map((p: any) => ({
      id: p.user.id,
      name: p.user.name,
      avatar: p.avatar, // 包含头像
    }));

    return {
      id: room.id,
      name: room.name,
      playerCount: room.players.size,
      maxPlayers: room.maxPlayers,
      state: room.state,
      locked: room.locked,
      cycle: room.cycle,
      ownerId: room.ownerId,
      selectedChart: room.selectedChart, // 包含当前谱面信息
      messages: (room.messages || []).slice(-20).map((m: any) => {
        // 填充用户名以便在远程节点显示
        let userName = '';
        if (m.user !== undefined) {
          if (m.user === -1) {
            userName = this.config.serverName;
          } else {
            const p = room.players.get(m.user);
            userName = p ? p.user.name : `ID: ${m.user}`;
          }
        }
        return { ...m, userName };
      }),
      players,
    };
  }

  /** 获取本地所有房间的联邦信息 */
  getLocalRoomsForFederation(): any[] {
    return this.roomManager.listRooms().map(room => this.buildLocalRoomInfo(room));
  }

  // ==================== 持久化 ====================

  private saveNodes(): void {
    try {
      const dir = path.dirname(this.nodesFile);
      if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });

      const data = Array.from(this.nodes.values()).map(n => ({
        id: n.id,
        url: n.url,
        serverName: n.serverName,
        lastSeen: n.lastSeen,
        addedAt: n.addedAt,
      }));

      fs.writeFileSync(this.nodesFile, JSON.stringify(data, null, 2));
    } catch (error) {
      this.logger.error(`[联邦] 保存节点缓存失败: ${error}`);
    }
  }

  private loadNodes(): void {
    try {
      if (!fs.existsSync(this.nodesFile)) return;

      const raw = fs.readFileSync(this.nodesFile, 'utf8');
      const data = JSON.parse(raw);

      if (Array.isArray(data)) {
        for (const node of data) {
          if (node.id && node.url && node.id !== this.config.nodeId) {
            this.nodes.set(node.id, {
              ...node,
              status: 'unknown' as const,
            });
          }
        }
        this.logger.info(`[联邦] 从缓存加载了 ${this.nodes.size} 个节点`);
      }
    } catch (error) {
      this.logger.error(`[联邦] 加载节点缓存失败: ${error}`);
    }
  }

  // ==================== 状态查询 ====================

  getStatus(): any {
    return {
      enabled: this.config.enabled,
      nodeId: this.config.nodeId,
      instanceId: this.instanceId,
      nodeUrl: this.config.nodeUrl,
      serverName: this.config.serverName,
      nodes: this.getNodes().map(n => ({
        id: n.id,
        url: n.url,
        instanceId: n.instanceId,
        serverName: n.serverName,
        status: n.status,
        lastSeen: n.lastSeen,
      })),
      remoteRoomCount: this.remoteRooms.size,
      proxyPlayerCount: this.proxyPlayers.size,
      federatedPlayerCount: this.federatedPlayers.size,
    };
  }
}
