"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const FederationManager_1 = require("./FederationManager");
const defaultPluginConfig = {
    enabled: false,
    seedNodes: [],
    secret: '',
    nodeId: '',
    nodeUrl: '',
    healthInterval: 300,
    syncInterval: 150,
    allowLocal: false,
};
let instance;
function authFederation(req, res, next) {
    if (!instance) {
        res.status(503).json({ error: 'Federation service not initialized yet' });
        return;
    }
    const secret = req.header('X-Federation-Secret');
    const expectedSecret = instance.getConfig().secret;
    if (!expectedSecret || !secret || secret !== expectedSecret) {
        res.status(403).json({ error: 'Invalid federation secret' });
        return;
    }
    next();
}
const plugin = {
    name: 'federation',
    async init(api) {
        const cfg = { ...defaultPluginConfig, ...(api.readPluginConfig() ?? {}) };
        if (!cfg.enabled) {
            api.logger.info('[Federation] 联邦功能未启用，跳过加载');
            return;
        }
        if (!cfg.secret) {
            api.logger.error('[Federation] 未配置共享密钥 secret，联邦功能不会生效');
            return;
        }
        const fedConfig = {
            enabled: true,
            seedNodes: cfg.seedNodes,
            secret: cfg.secret,
            nodeId: cfg.nodeId,
            nodeUrl: cfg.nodeUrl,
            healthInterval: cfg.healthInterval,
            syncInterval: cfg.syncInterval,
            serverName: api.config.serverName,
            allowLocal: cfg.allowLocal,
        };
        instance = new FederationManager_1.FederationManager(fedConfig, api.logger, api.roomManager);
        instance.setProtocolHandler(api.protocolHandler);
        api.protocolHandler.setFederationManager(instance);
        api.registerFederationManager(instance);
        const app = api.getExpressApp();
        if (app) {
            // 直接在 Express app 上注册路由，绕过 api.registerRoute 的插件存活检查
            // 因为联邦通讯必须在插件 init 期间就能响应反向握手请求
            app.post('/api/federation/handshake', authFederation, (req, res) => {
                const { nodeId, nodeUrl, serverName, instanceId, isReverse } = req.body;
                if (!nodeId || !nodeUrl) {
                    return res.status(400).json({ error: 'Missing nodeId or nodeUrl' });
                }
                const result = instance.handleIncomingHandshake({ nodeId, nodeUrl, serverName: serverName || 'Unknown', instanceId, isReverse: !!isReverse });
                return res.json(result);
            });
            app.get('/api/federation/health', authFederation, (_req, res) => {
                const fm = instance;
                return res.json({
                    nodeId: fm.getNodeId(), instanceId: fm.getInstanceId(),
                    serverName: fm.getConfig().serverName, status: 'online', timestamp: Date.now(),
                    peers: fm.getNodes().filter((n) => n.status === 'online').map((n) => ({ id: n.id, url: n.url, instanceId: n.instanceId, serverName: n.serverName })),
                });
            });
            app.get('/api/federation/peers', authFederation, (_req, res) => {
                return res.json({ peers: instance.getNodes().map((n) => ({ id: n.id, url: n.url, serverName: n.serverName, status: n.status, lastSeen: n.lastSeen })) });
            });
            app.get('/api/federation/rooms', authFederation, (_req, res) => {
                return res.json({ rooms: instance.getLocalRoomsForFederation() });
            });
            app.post('/api/federation/proxy/join', authFederation, (req, res) => {
                const { roomId, userId, userInfo, sourceNodeId, sourceNodeUrl } = req.body;
                const result = instance.handleIncomingJoin({ roomId, userId, userInfo, sourceNodeId, sourceNodeUrl });
                return res.json(result);
            });
            app.post('/api/federation/proxy/leave', authFederation, (req, res) => {
                const { roomId, userId, sourceNodeId } = req.body;
                const result = instance.handleIncomingLeave({ roomId, userId, sourceNodeId });
                return res.json(result);
            });
            app.post('/api/federation/proxy/command', authFederation, async (req, res) => {
                const { roomId, userId, command, sourceNodeId } = req.body;
                const result = await instance.handleIncomingCommand({ roomId, userId, command, sourceNodeId });
                return res.json(result);
            });
            app.post('/api/federation/proxy/callback', authFederation, (req, res) => {
                const { targetUserId, command } = req.body;
                const ok = instance.handleEventCallback({ targetUserId, command });
                return res.json({ success: ok });
            });
            app.post('/api/federation/event', authFederation, (req, res) => {
                const { type, sourceNodeId, roomId, data, timestamp } = req.body;
                instance.handleIncomingEvent({ type, sourceNodeId, roomId, data, timestamp });
                return res.json({ success: true });
            });
            api.logger.info('[Federation] 联邦 HTTP 路由已注册');
        }
        else {
            api.logger.warn('[Federation] Web 服务器未启用，联邦 HTTP 路由不可用');
        }
        // 延迟一帧启动，确保 init 结束后插件已被 PluginManager 登记、HTTP 路由已就绪
        await new Promise(resolve => setImmediate(resolve));
        await instance.start();
        api.logger.info('[Federation] 联邦插件已启动');
    },
    async destroy() {
        if (instance) {
            await instance.stop();
            instance = undefined;
        }
    },
};
exports.default = plugin;
