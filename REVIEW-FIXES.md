# ws-synapse 代码审查问题与修复记录

## 第一轮审查 (2026-04-15)

### P0 严重问题

#### 1. broadcastDirect 离线用户 PendingStore 使用未拷贝的 data
- **位置**: `core/hub.go` broadcastDirect 方法
- **问题**: 离线用户的 PendingMessage 使用外层传入的 `data` 而非内部拷贝的 `cp`，如果调用方在返回后修改 data，PendingStore 中数据可能被污染
- **修复**: PendingStore push 时使用独立的 copy
- **状态**: ✅ 已修复

#### 2. writePump 退出后未关闭底层连接，导致 readPump 依赖 TCP keepalive 才能退出
- **位置**: `core/server.go` writePump 方法
- **问题**: Ping 失败或写入错误后 writePump 退出，但未关闭底层 WebSocket 连接。readPump 阻塞在 `c.ws.Read()` 上，只能等 TCP keepalive 超时（默认 2+ 小时）才返回错误。期间连接仍占据 Hub 槽位，不触发 unregister/onDisconnect/token revoke，发给该连接的消息进入 sendCh 无人消费
- **修复**: writePump defer 中在 drainToPending 之后调用 `c.CloseNow()`，强制关闭底层连接使 readPump 立即退出
- **状态**: ✅ 已修复

### P1 中等问题

#### 3. Subscribe 缺少 Hub 关闭保护
- **位置**: `core/hub.go` Subscribe 方法
- **问题**: Hub 关闭后仍可调用 Subscribe，且无返回值告知调用方是否成功
- **修复**: 增加 Hub 关闭检查，返回 bool（关闭时返回 false）
- **状态**: ✅ 已修复

#### 4. Cluster Relay OnSubscribe 计数器缺少负数保护
- **位置**: `cluster/redis_relay.go` OnUnsubscribe 方法
- **问题**: 极端并发下计数器可能为负数
- **修复**: 增加 `<= 0` 保护
- **状态**: ✅ 已修复

#### 5. CloseTopic 对每个连接都传 lastOnNode=true
- **位置**: `core/hub.go` CloseTopic 方法
- **问题**: 导致多次不必要的 Redis SRem 调用
- **修复**: 只对第一个连接传 lastOnNode=true，后续传 false
- **状态**: ✅ 已修复

#### 6. Token 重连时未显式 Revoke 旧 token
- **位置**: `core/server.go` upgrade 方法
- **问题**: 依赖 Generate 覆盖旧 token，自定义 TokenProvider 可能不覆盖导致旧 token 泄漏
- **修复**: Generate 前显式调用 Revoke
- **状态**: ✅ 已修复

### P2 轻微问题

#### 7. readPump defer 嵌套 defer 可读性差
- **位置**: `core/server.go` readPump 方法
- **问题**: defer 内嵌套 defer，执行顺序不直观
- **修复**: 改为明确的顺序调用
- **状态**: ✅ 已修复

#### 8. Logger 接口缺少 Debug 级别
- **位置**: `core/interfaces.go` Logger 接口
- **问题**: 线上排查困难，缺少细粒度日志
- **修复**: Logger 接口增加 Debug 方法，所有实现同步更新
- **状态**: ✅ 已修复

---

## 第二轮审查 (2026-04-15)

### P1 中等问题

#### 9. CloseConn 不 revoke token 且不通知 relay unregister
- **位置**: `core/hub.go` CloseConn 方法
- **问题**: 通过 CloseConn 关闭的连接，readPump 的 unregister 返回 false（因为已被 CloseConn 移除），导致 token 不会被 revoke。同时 relay 不会收到 OnUnregister 通知，Redis 中连接注册残留
- **修复**: CloseConn 中增加 relay.OnUnregister 和 tokenProvider.Revoke 调用
- **状态**: ✅ 已修复

#### 10. Cluster Relay 广播到死节点浪费资源且不自清理
- **位置**: `cluster/redis_relay.go` PublishBroadcast 方法
- **问题**: 节点崩溃后其 nodeID 仍残留在 topic set 中。后续每次 Broadcast 都会尝试 XADD 到死节点的 stream
- **修复**: PublishBroadcast 中广播前检查目标节点存活状态，死节点从 topic set 中移除
- **状态**: ✅ 已修复

#### 11. Cluster Relay fallbackToPending 无 PendingStore 时静默吞消息
- **位置**: `cluster/redis_relay.go` fallbackToPending 方法
- **问题**: 无 PendingStore 时返回 nil（成功），消息静默丢失无任何错误或日志
- **修复**: 返回 `core.ErrMessageDropped` 并记录 warn 日志
- **状态**: ✅ 已修复

#### 12. writePump defer 中 wg.Done 仍使用嵌套 defer
- **位置**: `core/server.go` writePump 方法
- **问题**: 与 readPump 第一轮修复不一致
- **修复**: 移除嵌套 defer，改为顺序调用
- **状态**: ✅ 已修复

---

## 第三轮审查 (2026-04-15)

### P1 中等问题

#### 13. 连接替换时旧连接的订阅未被清理
- **位置**: `core/hub.go` register 方法
- **问题**: 同 connID 重连时，register 中 LoadAndDelete 移除旧连接后没有调用 unsubscribeAll。旧 readPump 退出时 unregister 返回 false（CompareAndDelete 失败），也不会清理订阅。导致旧连接的 topic 订阅残留，新连接会收到旧连接订阅的 topic 的广播消息
- **修复**: register 中连接替换后立即调用 unsubscribeAll(connID) 清理旧订阅
- **状态**: ✅ 已修复

#### 14. heartbeat 只 Expire 不 Set，被清理的 connKey 无法恢复
- **位置**: `cluster/redis_relay.go` heartbeatLoop 方法
- **问题**: heartbeat 对 nodeAliveKey 和 connKey 使用 Expire 刷新 TTL。如果 Redis 短暂不可用超过 TTL，其他节点会删除这些 key（PublishSend 中的死节点清理逻辑）。Redis 恢复后 Expire 对不存在的 key 无效，导致集群路由永久断裂直到连接重连
- **修复**: heartbeat 中改用 Set（带 TTL）替代 Expire，即使 key 被其他节点删除也能重建
- **状态**: ✅ 已修复

### P2 轻微问题

#### 15. OnConnect 拒绝连接时不 Revoke 已生成的 token
- **位置**: `core/server.go` upgrade 方法
- **问题**: token 在 OnConnect 之前已 Generate。如果 OnConnect 返回 error 拒绝连接，或 register 返回 ErrMaxConnsReached，token 不会被 Revoke，留在 TokenProvider 中直到 TTL 过期
- **修复**: OnConnect 失败和 register 失败的错误分支中增加 tokenProvider.Revoke 调用
- **状态**: ✅ 已修复

### 已知限制（不修复）

#### L1. register 连接数限制的 TOCTOU 竞态
- **位置**: `core/hub.go`
- **问题**: 两个不同 connID 的连接同时注册时可能导致 maxConns 被轻微超出（差 1-2 个连接）
- **影响**: 无锁设计性能优先，误差可接受

#### L2. Shutdown 与并发 register 有理论上的竞态
- **位置**: `core/hub.go`
- **问题**: closed.Store(true) 后，已通过检查的 register 仍可完成注册
- **影响**: 极端边界，Shutdown 时不应有新连接进来

#### L3. writePump 退出到 readPump 退出之间的短窗口内 sendCh 写入可能丢失
- **位置**: `core/server.go`
- **问题**: drainToPending 完成后、readPump 退出前，OnMessage 往自己 sendCh 写入的消息不会被 drain
- **影响**: 需要 OnMessage 往自己发消息的罕见场景

#### L4. OnSubscribe SAdd 失败时 localTopics 与 Redis 不一致
- **位置**: `cluster/redis_relay.go` OnSubscribe 方法
- **问题**: 首次订阅时 localTopics 已记录 count=1，但 Redis SAdd 可能失败。后续订阅不会重试 SAdd
- **影响**: 极端场景（Redis 命令级失败），该 topic 的跨节点广播不会路由到本节点
