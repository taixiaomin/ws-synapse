# ws-synapse

A production-ready WebSocket server library for Go, built on [coder/websocket](https://github.com/coder/websocket).

Provides connection lifecycle management, topic-based pub/sub, offline message buffering, reconnect token rotation, rate limiting, and pluggable metrics — all out of the box.

## Features

| Feature | Description |
|---------|-------------|
| **Event-driven** | `OnConnect` / `OnMessage` / `OnDisconnect` / `OnError` callbacks |
| **Topic pub/sub** | `Subscribe` / `Unsubscribe` / `Broadcast` / `BroadcastExclude` |
| **Offline buffering** | `PendingStore` interface — built-in Redis implementation |
| **Reconnect tokens** | `TokenProvider` with automatic rotation on reconnect |
| **Rate limiting** | Per-connection inbound message throttling |
| **Connection limit** | Configurable max concurrent connections |
| **Graceful shutdown** | Drain pending messages, fire callbacks, revoke tokens |
| **Zero-copy broadcast** | Data copied once, shared across all subscribers |
| **Panic recovery** | All event callbacks are wrapped with `recover()` |
| **Pluggable** | Logger, Metrics, MessageParser — all interfaces |

## Install

```bash
go get github.com/taixiaomin/ws-synapse
```

## Quick Start

```go
package main

import (
    "context"
    "log"
    "net/http"
    ws "github.com/taixiaomin/ws-synapse"
)

type MyHandler struct{}

func (h *MyHandler) OnConnect(ctx context.Context, conn *ws.Conn) error {
    log.Printf("connected: %s", conn.ID())
    // conn.Hub() is available here for Subscribe/Send etc.
    return conn.SendJSON(map[string]string{"type": "welcome", "token": conn.Token()})
}

func (h *MyHandler) OnMessage(ctx context.Context, conn *ws.Conn, msg *ws.Message) error {
    log.Printf("message from %s: %s", conn.ID(), msg.Type)
    return nil
}

func (h *MyHandler) OnDisconnect(ctx context.Context, conn *ws.Conn) {
    log.Printf("disconnected: %s", conn.ID())
}

func (h *MyHandler) OnError(ctx context.Context, conn *ws.Conn, err error) {
    log.Printf("error from %s: %v", conn.ID(), err)
}

func main() {
    server := ws.NewServer(&MyHandler{},
        ws.WithOnUpgrade(func(r *http.Request) (*ws.UpgradeInfo, error) {
            return &ws.UpgradeInfo{ConnID: r.URL.Query().Get("id")}, nil
        }),
        ws.WithTokenProvider(ws.NewMemoryTokenProvider()),
        ws.WithMaxConnections(10000),
        ws.WithRateLimit(100, 20),
    )

    http.HandleFunc("/ws", server.HandleHTTP)
    log.Fatal(http.ListenAndServe(":8080", nil))
}
```

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│                        Server                            │
│                                                          │
│  HTTP Request                                            │
│      │                                                   │
│      ▼                                                   │
│  ┌────────┐    ┌───────────┐    ┌──────────────────┐    │
│  │Upgrade │───▶│ OnConnect │───▶│   Hub.register   │    │
│  │        │    │(Hub ready)│    │ (flush pending)  │    │
│  └────────┘    └───────────┘    └──────────────────┘    │
│                                          │               │
│                              ┌───────────┼───────────┐   │
│                              ▼                       ▼   │
│                        ┌──────────┐          ┌──────────┐│
│                        │readPump  │          │writePump ││
│                        │ws.Read() │          │sendCh→ws ││
│                        │→OnMessage│          │+ ping    ││
│                        └──────────┘          └──────────┘│
│                              │                       │   │
│                              ▼                       ▼   │
│                        ┌──────────────────────────────┐  │
│                        │       OnDisconnect           │  │
│                        │  unregister → unsubscribeAll │  │
│                        │  drainToPending → Revoke     │  │
│                        └──────────────────────────────┘  │
└──────────────────────────────────────────────────────────┘
```

### Connection Lifecycle (9 Steps)

1. **Pre-check** — connection limit guard
2. **Authenticate** — optional `authenticator` or `OnUpgrade` hook
3. **WebSocket upgrade** — via `coder/websocket`
4. **Token handling** — validate (reconnect) or generate (new); rotate on reconnect
5. **Create Conn** — allocate sendCh, rate limiter
6. **Inject Hub** — `conn.Hub()` becomes available
7. **OnConnect** — fire callback; reject by returning error
8. **Register** — replace old conn if same ID; flush pending (order-safe)
9. **Start pumps** — readPump (blocking) + writePump (goroutine)

## API Reference

### Server

```go
// Create
server := ws.NewServer(handler, opts...)

// Upgrade methods
server.HandleHTTP(w, r)                          // uses OnUpgrade / ConnIDExtractor
server.Upgrade(w, r, connID, opts...)             // framework-agnostic
server.UpgradeWithMeta(w, r, connID, meta, opts...)

// Access Hub
hub := server.Hub()

// Graceful shutdown
server.Shutdown(ctx)
server.Shutdown(ctx, []byte(`{"type":"bye"}`))    // with final message
```

### Hub

```go
// Point-to-point
hub.Send(ctx, connID, data)         // text, offline → PendingStore
hub.SendJSON(ctx, connID, v)
hub.SendBinary(ctx, connID, data)   // binary frame

// Topic pub/sub
hub.Subscribe(ctx, connID, topic)
hub.Unsubscribe(ctx, connID, topic)
hub.Broadcast(ctx, topic, data)
hub.BroadcastJSON(ctx, topic, v)
hub.BroadcastExclude(ctx, topic, data, excludeIDs...)

// Pre-serialized broadcast (avoid repeated Marshal)
msg, _ := ws.NewPreparedMessage(payload)
hub.BroadcastPrepared(ctx, topic, msg)

// Management
hub.GetConn(connID)         // *Conn or nil
hub.ConnCount()             // O(1)
hub.TopicCount()
hub.TopicSubscribers(topic)
hub.ConnTopics(connID)
hub.Range(func(id string, c *Conn) bool { ... })
hub.CloseConn(connID, reason)
hub.CloseTopic(ctx, topic, notifyData)
```

### Conn

```go
conn.ID()                   // connection identifier
conn.Token()                // reconnect token (send to client)
conn.IsReconnect()          // true if reconnected via token
conn.Hub()                  // available in OnConnect and later

conn.Send(data)             // text, non-blocking, returns bool
conn.SendBinary(data)       // binary
conn.SendJSON(v)            // returns error

conn.Set(key, value)        // metadata
conn.Get(key)
conn.GetString(key)
conn.GetInt64(key)
conn.GetBool(key)

conn.RemoteAddr()           // prefers X-Forwarded-For
conn.Header(key)            // handshake HTTP header
conn.Query(key)             // handshake query param

conn.Close(reason)          // graceful close frame
conn.CloseNow()             // force close
conn.Done()                 // <-chan struct{}, closed on lifecycle end
```

## Server Options

| Option | Default | Description |
|--------|---------|-------------|
| `WithOnUpgrade(fn)` | nil | Unified auth + connID extraction hook |
| `WithAuthenticator(fn)` | nil | Pre-upgrade auth (ignored if OnUpgrade set) |
| `WithConnIDExtractor(fn)` | nil | ConnID from request (ignored if OnUpgrade set) |
| `WithTokenProvider(tp)` | nil | Reconnect token management |
| `WithPendingStore(store)` | nil | Offline message buffering |
| `WithMaxConnections(n)` | 0 (unlimited) | Max concurrent connections |
| `WithRateLimit(perSec, burst)` | 0 (unlimited) | Per-connection inbound rate limit |
| `WithSendChSize(n)` | 256 | Outbound channel buffer size |
| `WithWriteTimeout(d)` | 10s | Per-write timeout |
| `WithPingInterval(d)` | 54s | Heartbeat ping interval |
| `WithDrainTimeout(d)` | 5s | Max time to drain pending on disconnect |
| `WithMaxMessageSize(n)` | 32KB | Max inbound message size |
| `WithInsecureSkipVerify(b)` | false | Skip WebSocket origin check |
| `WithAllowedOrigins(...)` | nil | Allowed origin patterns |
| `WithTokenParam(name)` | `"token"` | Query param name for reconnect token |
| `WithMessageParser(p)` | JSON `type` | Custom inbound message type parser |
| `WithLogger(l)` | slog/JSON | Structured logger |
| `WithMetrics(m)` | no-op | Metrics collector |

## Interfaces

### EventHandler (required)

```go
type EventHandler interface {
    OnConnect(ctx context.Context, conn *Conn) error
    OnMessage(ctx context.Context, conn *Conn, msg *Message) error
    OnDisconnect(ctx context.Context, conn *Conn)
    OnError(ctx context.Context, conn *Conn, err error)
}
```

### TopicEventHandler (optional)

Implement alongside `EventHandler` to get topic lifecycle callbacks:

```go
type TopicEventHandler interface {
    OnSubscribe(ctx context.Context, connID, topic string)
    OnUnsubscribe(ctx context.Context, connID, topic string)
}
```

### PendingStore

```go
type PendingMessage struct {
    Data    []byte
    MsgType int // MsgTypeText (1) or MsgTypeBinary (2)
}

type PendingStore interface {
    // Push stores a text message (backward-compatible convenience method).
    Push(ctx context.Context, connID string, data []byte) error
    // PushEnvelope stores a message with explicit type (text or binary).
    PushEnvelope(ctx context.Context, connID string, msg PendingMessage) error
    // PushFront puts popped-but-unsent messages back at the head, in order.
    PushFront(ctx context.Context, connID string, msgs []PendingMessage) error
    // PopAll atomically retrieves and removes all pending messages.
    PopAll(ctx context.Context, connID string) ([]PendingMessage, error)
}
```

Built-in: `store.NewRedisPendingStore(redisClient, opts...)`

### TokenProvider

```go
type TokenProvider interface {
    Generate(ctx context.Context, connID string) (string, error)
    Validate(ctx context.Context, connID, token string) (bool, error)
    Revoke(ctx context.Context, connID string) error
}
```

Built-in: `ws.NewMemoryTokenProvider()` (single-node only)

### MetricsCollector

```go
type MetricsCollector interface {
    IncConnections()
    DecConnections()
    IncMessagesIn()
    IncMessagesOut()
    IncDrops()
    IncErrors()
    IncReconnects()
}
```

### Logger

```go
type Logger interface {
    Info(msg string, keysAndValues ...any)
    Warn(msg string, keysAndValues ...any)
    Error(msg string, keysAndValues ...any)
}
```

Built-in: `ws.NewSlogAdapter(slogLogger)`, `ws.NopLogger{}`

## Redis PendingStore

```go
import wsstore "github.com/taixiaomin/ws-synapse/store"

rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
pendingStore := wsstore.NewRedisPendingStore(rdb,
    wsstore.WithKeyPrefix("myapp:ws:pending:"),
    wsstore.WithMaxMessages(1000),    // default 500
    wsstore.WithTTL(12 * time.Hour),  // default 24h
)

server := ws.NewServer(handler, ws.WithPendingStore(pendingStore))
```

Storage model: Redis List per connection (`RPUSH` + `LTRIM` + `EXPIRE`). PopAll uses `MULTI/EXEC` for atomicity.

## Reconnect Token Flow

```
Client                          Server
  │                               │
  │── GET /ws?id=user1 ──────────▶│  Generate token → store
  │◀── welcome {token: "abc"} ───│
  │                               │
  │   ~~~ connection drops ~~~    │
  │                               │  Messages → PendingStore
  │── GET /ws?id=user1&token=abc ▶│  Validate → rotate → new token "xyz"
  │◀── welcome {token: "xyz"} ───│  Flush pending → sendCh
  │                               │
```

Key behaviors:
- **Token rotation**: on successful reconnect, old token is immediately invalidated and a new one is generated
- **Constant-time validation**: `crypto/subtle.ConstantTimeCompare` prevents timing attacks
- **Pending flush ordering**: pending messages are flushed to `sendCh` before the connection becomes visible, guaranteeing correct message order
- **Token revocation**: on permanent disconnect (not replaced), the token is revoked

## Project Structure

```
ws-synapse/
├── ws.go                   # Public API — re-exports from core/, token/, cluster/
├── go.mod
├── core/
│   ├── conn.go             # Conn type, Send/SendJSON/metadata/lifecycle
│   ├── hub.go              # Hub — connections, topics, broadcast, pending flush
│   ├── server.go           # Server — upgrade, readPump, writePump, shutdown
│   ├── server_options.go   # ServerOption, UpgradeOption, jsonTypeParser
│   ├── interfaces.go       # EventHandler, PendingStore, TokenProvider, ClusterRelay, etc.
│   ├── logger.go           # slogAdapter, NopLogger
│   └── metrics.go          # nilMetrics (no-op)
├── token/
│   ├── token.go            # MemoryTokenProvider (sync.Map + crypto/rand)
│   └── redis_token.go      # RedisTokenProvider (go-redis, multi-node)
├── store/
│   └── redis_pending.go    # RedisPendingStore (go-redis, preserves text/binary type)
├── cluster/
│   ├── redis_relay.go      # RedisClusterRelay (Redis Streams, cross-node messaging)
│   └── options.go          # Relay configuration options
└── examples/
    └── basic-http/
        └── main.go
```

## Distributed Deployment

The library supports both single-node and multi-node modes. Multi-node is activated by injecting a `ClusterRelay` via `WithClusterRelay()`.

| Component | Single-node | Multi-node |
|-----------|:-----------:|:----------:|
| Connection management (Hub) | ✅ sync.Map | ✅ local + cross-node relay |
| Topic subscriptions | ✅ in-memory | ✅ local + Redis topic registry |
| PendingStore (Redis) | ✅ | ✅ shared Redis |
| TokenProvider (Memory) | ✅ | — |
| TokenProvider (Redis) | — | ✅ `token.NewRedisTokenProvider` |
| ClusterRelay (Redis Streams) | — | ✅ `cluster.NewRedisClusterRelay` |

### Multi-node example

```go
rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})

server := ws.NewServer(handler,
    ws.WithTokenProvider(wstoken.NewRedisTokenProvider(rdb)),
    ws.WithPendingStore(wsstore.NewRedisPendingStore(rdb)),
    ws.WithClusterRelay(cluster.NewRedisClusterRelay(rdb, pendingStore,
        cluster.WithHeartbeatInterval(30 * time.Second),
        cluster.WithConnTTL(120 * time.Second),
        cluster.WithStreamMaxLen(1000),
    )),
)
```

### How it works

- **Cross-node Send**: Hub.Send looks up `ws:conn:{connID}` → target nodeID, writes to `ws:stream:{nodeID}` via Redis Streams (XADD NOMKSTREAM / XREADGROUP). Falls back to PendingStore if target node is dead.
- **Cross-node Broadcast**: Hub.Broadcast reads the `ws:topicz:{topic}` ZSET (member = nodeID, score = last heartbeat on the Redis server clock) for nodes refreshed within NodeTTL, then writes to their streams in one pipeline (targeted, not global fan-out).
- **Dead-node cleanup**: no keyspace SCAN. Each node's heartbeat re-scores its topics and prunes members older than NodeTTL in the same Lua call; a topic whose members are all dead expires via TopicTTL. Scores come from `redis.call('TIME')`, so application clock skew can't evict live nodes.
- **Connection registry**: Each connection registered as `ws:conn:{connID}` String with TTL, renewed by the heartbeat only while it still names this node; unregister deletes it compare-and-set.
- **Node alive**: Each node maintains `ws:node:alive:{nodeID}` with TTL. Dead nodes' connections auto-expire.
- **Requirements**: Redis ≥ 6.2 (`XADD NOMKSTREAM`). Every script touches a single key, so Redis Cluster is supported.

## Known Limitations

| # | Description | Impact | Workaround |
|---|-------------|--------|------------|
| 1 | `Subscribe` does not verify `connID` exists | Orphan subscriptions possible if called with stale IDs | Always call from within event callbacks where conn is guaranteed alive |
| 2 | No per-callback timeout enforcement | A blocking `OnMessage` handler stalls the readPump | Implement timeouts inside your handler |

## License

MIT
