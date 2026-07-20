# phira-mp-nodejsver-federation

去中心化对等联邦网络插件 — 为 phira-mp-nodejsver 提供多服务器无中心化联机。

## 核心功能

- **双向握手** — A 连 B 时 B 主动回连，形成对等连接
- **Gossip 节点发现** — 共享已知节点列表，自动扩散全网
- **节点持久化** — 缓存到 `data/federation_nodes.json`，重启自动恢复
- **健康检查** — 定期 ping，离线恢复后立即同步
- **双向房间同步** — 所有节点的房间对所有其他节点可见
- **跨服代理** — 任意节点玩家可加入任意其他节点的房间
- **事件回调** — 房间所在服务器向玩家所在服务器推送实时事件
- **实时广播** — 房间创建/删除/更新时即时通知所有节点

## 安装

将整个仓库放入主服务器的 `plugins/federation/` 目录：

```
phira-mp-server/plugins/federation/
├── plugin.yaml
├── config.default.yaml
├── README.md
└── res/
    ├── main.ts
    └── FederationManager.ts
```

## 配置

首次加载时 `config.default.yaml` 自动复制到 `config/federation/config.yaml`，编辑它：

```yaml
# 启用联邦
enabled: true

# 种子节点（首次加入网络时从这里发现其他节点）
seedNodes:
  - http://other-server-ip:8080

# 共享密钥，同一网络的所有节点必须一致
secret: "your-shared-secret"

# 本节点对外访问地址
nodeUrl: http://your-server-ip:8080

# 节点唯一 ID，留空自动生成
nodeId: ""

# 健康检查间隔（毫秒）
healthInterval: 300

# 房间同步间隔（毫秒）
syncInterval: 150

# 是否允许连接本地/私有 IP
allowLocal: false
```

## 多节点示例

两台服务器 A（10.0.0.1:8080）和 B（10.0.0.2:8080）互连：

**A 的配置：**
```yaml
enabled: true
secret: "my-network-key"
nodeUrl: http://10.0.0.1:8080
seedNodes:
  - http://10.0.0.2:8080
```

**B 的配置：**
```yaml
enabled: true
secret: "my-network-key"        # 与 A 一致
nodeUrl: http://10.0.0.2:8080
seedNodes:
  - http://10.0.0.1:8080
```

加载后两边的房间会互相可见，玩家可以跨服加入。

## 前置条件

- 主服务器启用 Web 服务器（`ENABLE_WEB_SERVER=true`）
- 主服务器加载 `web-dashboard` 插件（联邦通过 Express 注册 HTTP 路由）
- 主服务器版本 ≥ 0.6.0

## HTTP 端点

联邦插件自动注册以下路由（通过 `X-Federation-Secret` 头认证）：

| 方法 | 路径 | 用途 |
|------|------|------|
| POST | `/api/federation/handshake` | 双向握手 |
| GET | `/api/federation/health` | 健康检查 |
| GET | `/api/federation/peers` | 节点列表 |
| GET | `/api/federation/rooms` | 本地房间列表 |
| POST | `/api/federation/proxy/join` | 远程玩家加入本地房间 |
| POST | `/api/federation/proxy/leave` | 远程玩家离开本地房间 |
| POST | `/api/federation/proxy/command` | 转发命令到权威服务器 |
| POST | `/api/federation/proxy/callback` | 事件回调（权威→代理） |
| POST | `/api/federation/event` | 房间事件广播 |

## 许可证

MIT
