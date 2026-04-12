# WebSocket Server Toolkit

A production-ready, **zero-framework-dependency** WebSocket server library built on top of [coder/websocket](https://github.com/coder/websocket). Provides connection lifecycle management, topic-based pub/sub, offline message buffering, reconnect token validation, rate limiting, and pluggable metrics — all out of the box.

Designed as a **standalone Go module** that can be consumed by any project (gateway, API server, etc.) without pulling in framework-specific dependencies.

## Architecture

```
┌──────────────────────────────────────────────────────────┐
│                        Server                            │
│  ┌──────────────┐  ┌────────────┐  ┌────────────────┐   │
│  │  HandleHTTP  │  │  Upgrade   │  │  EventHandler   │   │
│  │  (net/http)  │→ │ (net/http) │→ │  (callbacks)    │   │
│  └──────────────┘  └─────┬──────┘  └────────────────┘   │
│                          │                               │
│              ┌───────────▼──────────┐                    │
│              │        Hub           │                    │
│              │  ┌──────────────┐    │   ┌─────────────┐  │
│              │  │ Conns/Topics │    │──→│PendingStore  │  │
│              │  │ Metrics      │    │   │(Redis/Memory)│  │
│              │  └──────────────┘    │   └─────────────┘  │
│              └──────────────────────┘                    │
│         ┌──────────┐    ┌──────────┐                     │
│         │ readPump │    │writePump │                     │
│         │(per conn)│    │(per conn)│                     │
│         └──────────┘    └──────────┘                     │
└──────────────────────────────────────────────────────────┘
```

## Core Dependencies

The ws package only depends on:
- `github.com/coder/websocket` — WebSocket protocol
- `golang.org/x/time/rate` — per-connection rate limiting

**No framework dependency** (Gin, Echo, etc.), **no logger dependency**, **no metrics dependency**.

## Quick Start

### 1. Implement `EventHandler`

```go
package main

import (
	"context"
	"encoding/json"
	"log/slog"

	"your-module/ws"
)

type MyHandler struct {
	hub *ws.Hub
}

func (h *MyHandler) OnConnect(ctx context.Context, conn *ws.Conn) error {
	slog.Info("connected", "id", conn.ID(), "reconnect", conn.IsReconnect())
	return conn.SendJSON(map[string]interface{}{
		"type":  "welcome",
		"token": conn.Token(),
	})
}

func (h *MyHandler) OnMessage(ctx context.Context, conn *ws.Conn, msg *ws.Message) error {
	switch msg.Type {
	case "subscribe":
		var req struct {
			Topic string `json:"topic"`
		}
		if err := json.Unmarshal(msg.Raw, &req); err != nil {
			return err
		}
		h.hub.Subscribe(ctx, conn.ID(), req.Topic)

	case "ping":
		return conn.SendJSON(map[string]string{"type": "pong"})
	}
	return nil
}

func (h *MyHandler) OnDisconnect(ctx context.Context, conn *ws.Conn) {
	slog.Info("disconnected", "id", conn.ID())
}

func (h *MyHandler) OnError(ctx context.Context, conn *ws.Conn, err error) {
	slog.Error("ws error", "id", conn.ID(), "error", err)
}

// Optional: implement TopicEventHandler for topic lifecycle callbacks
func (h *MyHandler) OnSubscribe(ctx context.Context, connID, topic string) {
	slog.Info("subscribed", "connID", connID, "topic", topic)
}

func (h *MyHandler) OnUnsubscribe(ctx context.Context, connID, topic string) {
	slog.Info("unsubscribed", "connID", connID, "topic", topic)
}
```

### 2. Create Server and Mount Routes

#### With standard `net/http`

```go
func main() {
	handler := &MyHandler{}

	server := ws.NewServer(handler,
		ws.WithTokenProvider(ws.NewMemoryTokenProvider()),
		ws.WithPingInterval(30*time.Second),
		ws.WithMaxConnections(10000),
		ws.WithRateLimit(100, 20),
		ws.WithInsecureSkipVerify(true), // dev only!
	)

	// Inject Hub reference
	handler.hub = server.Hub()

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		connID := r.URL.Query().Get("id")
		server.Upgrade(w, r, connID)
	})

	http.ListenAndServe(":8080", mux)
}
```

#### With Gin (ws package has NO Gin dependency)

```go
r := gin.Default()
r.GET("/ws", func(c *gin.Context) {
    connID := c.Query("userId")
    server.Upgrade(c.Writer, c.Request, connID)
})
```

#### With HandleHTTP (convenience, uses ConnIDExtractor)

```go
server := ws.NewServer(handler,
    ws.WithConnIDExtractor(func(r *http.Request) (string, error) {
        return r.URL.Query().Get("id"), nil
    }),
)

mux := http.NewServeMux()
mux.HandleFunc("/ws", server.HandleHTTP)
```

### 3. Hub API — Sending Messages & Managing Topics

```go
hub := server.Hub()
ctx := context.Background()

// Send to a specific connection
hub.Send(ctx, "user-123", []byte(`{"type":"update","price":5000}`))
hub.SendJSON(ctx, "user-123", map[string]interface{}{"type": "update", "price": 5000})
hub.SendBinary(ctx, "user-123", protobufBytes) // binary message

// Topic subscription (typically called from OnMessage handler)
hub.Subscribe(ctx, "user-123", "product:ABC-001")
hub.Subscribe(ctx, "user-456", "product:ABC-001")

// Broadcast to all subscribers of a topic
hub.Broadcast(ctx, "product:ABC-001", []byte(`{"type":"bid_update","amount":6000}`))
hub.BroadcastJSON(ctx, "product:ABC-001", map[string]interface{}{"type": "bid_update", "amount": 6000})

// Unsubscribe / close topic
hub.Unsubscribe(ctx, "user-123", "product:ABC-001")
hub.CloseTopic(ctx, "product:ABC-001", []byte(`{"type":"topic_closed"}`))

// Query
conn := hub.GetConn("user-123")          // *Conn or nil
count := hub.ConnCount()                    // active connection count
tCount := hub.TopicCount()                  // active topic count
subs := hub.TopicSubscribers("product:ABC-001") // []string of connIDs
topics := hub.ConnTopics("user-123")      // []string of topics

// Iterate all connections
hub.Range(func(connID string, conn *ws.Conn) bool {
    // ...
    return true // continue iteration
})

// Connection management
hub.CloseConn("user-123", "kicked")
```

## Configuration Options

| Option | Default | Description |
|---|---|---|
| `WithConnIDExtractor(fn)` | — | `func(*http.Request) (string, error)` for HandleHTTP |
| `WithAuthenticator(fn)` | — | `func(*http.Request) error` pre-upgrade auth |
| `WithTokenParam(name)` | `"token"` | Query parameter name for reconnect token |
| `WithTokenProvider(tp)` | `nil` | Token generation/validation; nil = auto-generate |
| `WithSendChSize(n)` | `256` | Outbound channel buffer per connection |
| `WithWriteTimeout(d)` | `10s` | Per-message write timeout |
| `WithPingInterval(d)` | `54s` | Heartbeat ping interval |
| `WithMaxMessageSize(n)` | `32KB` | Max inbound message size (bytes) |
| `WithMaxConnections(n)` | `0` | Max concurrent connections (0 = unlimited) |
| `WithRateLimit(perSec, burst)` | `0, 10` | Per-connection inbound rate limit (0 = disabled) |
| `WithPendingStore(store)` | `nil` | Offline message buffer (e.g., Redis) |
| `WithMessageParser(p)` | JSON `"type"` | Custom message type extractor |
| `WithDrainTimeout(d)` | `5s` | Time to drain pending messages on disconnect |
| `WithInsecureSkipVerify(bool)` | `false` | Skip WebSocket origin check |
| `WithAllowedOrigins(patterns...)` | — | Allowed origin patterns |
| `WithLogger(logger)` | slog JSON | Structured logger (ws.Logger interface) |
| `WithMetrics(collector)` | no-op | Metrics collector (ws.MetricsCollector interface) |

## Pluggable Interfaces

### Logger

```go
type Logger interface {
    Info(msg string, keysAndValues ...any)
    Warn(msg string, keysAndValues ...any)
    Error(msg string, keysAndValues ...any)
}

// Use the built-in slog adapter:
ws.WithLogger(ws.NewSlogAdapter(slog.Default()))

// Or implement your own (zap, logrus, etc.)
```

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

// Example Prometheus adapter (implement in your project):
type PromMetrics struct { /* prometheus counters/gauges */ }
func (p *PromMetrics) IncConnections() { p.connGauge.Inc() }
// ... etc

ws.NewServer(handler, ws.WithMetrics(&PromMetrics{}))
```

### MessageParser

```go
type MessageParser interface {
    ParseType(data []byte) string
}

// Default: parses JSON {"type":"..."} field
// Custom example for a different envelope:
type CustomParser struct{}
func (CustomParser) ParseType(data []byte) string {
    var env struct{ Action string `json:"action"` }
    json.Unmarshal(data, &env)
    return env.Action
}

ws.NewServer(handler, ws.WithMessageParser(CustomParser{}))
```

### TopicEventHandler (optional)

```go
// If your EventHandler also implements TopicEventHandler,
// the Hub will automatically fire callbacks on subscribe/unsubscribe.
type TopicEventHandler interface {
    OnSubscribe(ctx context.Context, connID, topic string)
    OnUnsubscribe(ctx context.Context, connID, topic string)
}
```

## Offline Message Buffering

```go
type PendingStore interface {
    Push(ctx context.Context, connID string, data []byte) error
    PopAll(ctx context.Context, connID string) ([][]byte, error)
}

// Use with a Redis implementation:
store := redisStore.NewRedisPendingStore(redisClient)
server := ws.NewServer(handler, ws.WithPendingStore(store))
```

## Reconnect Token Flow

```
Client                              Server
  │                                    │
  │──── GET /ws?id=user-1 ──────────→│  (no token → new connection)
  │                                    │  Generate token, store
  │←─── {"type":"welcome","token":"x"} │
  │                                    │
  │  ... connection drops ...          │
  │                                    │
  │──── GET /ws?id=user-1&token=x ──→│  (token provided → validate)
  │                                    │  Valid → isReconnect=true
  │←─── {"type":"welcome","token":"x"} │  Flush pending messages
  │←─── {pending msg 1}               │
  │←─── {pending msg 2}               │
```

Token can also be provided via `UpgradeOption`:
```go
server.Upgrade(w, r, connID, ws.WithReconnectToken(tokenFromHeader))
```

## Standalone Hub

The Hub can be used independently of the Server:

```go
hub := ws.NewHub(
    ws.WithHubLogger(myLogger),
    ws.WithHubMaxConns(5000),
    ws.WithHubMetrics(myMetrics),
    ws.WithHubPendingStore(myStore),
    ws.WithHubTopicHandler(myTopicHandler),
)
```

## Binary Message Support

```go
// Send text (JSON, etc.)
conn.Send([]byte(`{"type":"hello"}`))
conn.SendJSON(map[string]string{"type": "hello"})

// Send binary (protobuf, msgpack, etc.)
conn.SendBinary(protobufBytes)
```

## Graceful Shutdown

```go
// Shutdown with optional final message to all clients
shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()
server.Shutdown(shutdownCtx, []byte(`{"type":"server_shutdown"}`))
```

## File Structure

```
ws-synapse/
├── ws.go                      # Unified export point for all public API
│
├── core/                      # Core package containing main logic
│   ├── conn.go               # Conn — per-connection state, Send/SendBinary, metadata
│   ├── hub.go                # Hub — connection registry, topics, pub/sub
│   ├── server.go             # Server — Upgrade, HandleHTTP, read/write pumps
│   ├── server_options.go     # ServerOption/UpgradeOption pattern
│   ├── interfaces.go         # All public interfaces (EventHandler, Logger, etc.)
│   ├── logger.go             # Logger interface, slog adapter
│   └── metrics.go            # MetricsCollector interface
│
├── token/                     # Token provider package
│   └── token.go              # TokenProvider interface, MemoryTokenProvider
│
├── examples/                  # Example code
│   └── basic-http/main.go    # Basic HTTP server example
│
├── go.mod & go.sum           # Go module definition
├── LICENSE                   # MIT License
└── README.md                 # This file
```
