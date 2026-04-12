package core

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/coder/websocket"
	"golang.org/x/time/rate"
)

// ErrSendChannelFull is returned when the outbound channel is full.
var ErrSendChannelFull = errors.New("ws: send channel full")

// MessageEnvelope wraps outbound data with its WebSocket message type.
type MessageEnvelope struct {
	Data    []byte
	MsgType websocket.MessageType
}

// Conn represents a single WebSocket client connection.
// Business code receives *Conn in event callbacks and can call Send, Set/Get
// metadata, and inspect connection properties via getter methods.
type Conn struct {
	id    string // unique client identifier (e.g., userID)
	token string // reconnection token — server-generated

	ws          *websocket.Conn
	sendCh      chan MessageEnvelope
	meta        sync.Map
	mu          sync.Mutex
	once        sync.Once     // ensures finish() runs only once
	done        chan struct{} // closed when the connection lifecycle ends
	isReconnect bool          // true if connection was established via token reconnect
	limiter     *rate.Limiter // per-connection inbound rate limiter; nil = unlimited
}

func NewConn(id, token string, ws *websocket.Conn, chSize int, limiter *rate.Limiter) *Conn {
	return &Conn{
		id:      id,
		token:   token,
		ws:      ws,
		sendCh:  make(chan MessageEnvelope, chSize),
		done:    make(chan struct{}),
		limiter: limiter,
	}
}

// ID returns the unique client identifier for this connection.
func (c *Conn) ID() string { return c.id }

// Token returns the server-generated reconnect token.
// The client should persist this and send it back on reconnect.
func (c *Conn) Token() string { return c.token }

// IsReconnect reports whether this connection was established via a reconnect token.
func (c *Conn) IsReconnect() bool { return c.isReconnect }

// setReconnect sets the reconnect flag (internal use).
func (c *Conn) setReconnect(reconnect bool) { c.isReconnect = reconnect }

// Send pushes raw text bytes to the outbound queue.
// The data slice is copied internally to prevent data races.
// Non-blocking; returns false if the channel is full or the connection has ended.
func (c *Conn) Send(data []byte) bool {
	return c.enqueue(data, websocket.MessageText)
}

// SendBinary pushes raw binary bytes to the outbound queue.
// Useful for protobuf or other binary protocols.
// Non-blocking; returns false if the channel is full or the connection has ended.
func (c *Conn) SendBinary(data []byte) bool {
	return c.enqueue(data, websocket.MessageBinary)
}

// enqueue copies data and pushes it to the outbound channel.
func (c *Conn) enqueue(data []byte, msgType websocket.MessageType) bool {
	select {
	case <-c.done:
		return false
	default:
	}
	cp := make([]byte, len(data))
	copy(cp, data)
	env := MessageEnvelope{Data: cp, MsgType: msgType}
	select {
	case c.sendCh <- env:
		return true
	case <-c.done:
		return false
	default:
		return false
	}
}

// SendJSON marshals v to JSON and pushes it as a text message.
// Returns ErrSendChannelFull if the outbound channel is full.
func (c *Conn) SendJSON(v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	env := MessageEnvelope{Data: data, MsgType: websocket.MessageText}
	select {
	case <-c.done:
		return ErrSendChannelFull
	default:
	}
	select {
	case c.sendCh <- env:
		return nil
	case <-c.done:
		return ErrSendChannelFull
	default:
		return ErrSendChannelFull
	}
}

// Set stores an arbitrary key-value pair on this connection (goroutine-safe).
func (c *Conn) Set(key string, value interface{}) { c.meta.Store(key, value) }

// Get retrieves a metadata value by key.
func (c *Conn) Get(key string) (interface{}, bool) { return c.meta.Load(key) }

// Done returns a channel that is closed when the connection lifecycle ends.
func (c *Conn) Done() <-chan struct{} { return c.done }

// WriteEnvelope writes a single message envelope to the underlying WebSocket.
func (c *Conn) WriteEnvelope(ctx context.Context, env MessageEnvelope, timeout time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	wCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return c.ws.Write(wCtx, env.MsgType, env.Data)
}

// Close sends a close frame.
func (c *Conn) Close(reason string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	_ = c.ws.Close(websocket.StatusNormalClosure, reason)
}

// CloseNow forcefully closes without a close frame.
func (c *Conn) CloseNow() { _ = c.ws.CloseNow() }

// Finish signals that this connection's lifecycle has ended.
// Safe to call multiple times; only the first call has effect.
func (c *Conn) Finish() {
	c.once.Do(func() {
		close(c.done)
	})
}

// AllowMessage checks the per-connection rate limiter.
func (c *Conn) AllowMessage() bool {
	if c.limiter == nil {
		return true
	}
	return c.limiter.Allow()
}

// Internal access methods for use by other core packages

// writeEnvelope is exported for internal use by server package
func (c *Conn) writeEnvelope(ctx context.Context, env MessageEnvelope, timeout time.Duration) error {
	return c.WriteEnvelope(ctx, env, timeout)
}

// close is exported for internal use
func (c *Conn) close(reason string) {
	c.Close(reason)
}

// closeNow is exported for internal use
func (c *Conn) closeNow() {
	c.CloseNow()
}

// finish is exported for internal use
func (c *Conn) finish() {
	c.Finish()
}

// allowMessage is exported for internal use
func (c *Conn) allowMessage() bool {
	return c.AllowMessage()
}
