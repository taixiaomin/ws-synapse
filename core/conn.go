package core

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"golang.org/x/time/rate"
)

// ConnInfo holds a snapshot of the HTTP request at WebSocket handshake time (read-only).
type ConnInfo struct {
	RemoteAddr string
	Header     http.Header
	Query      url.Values
}

// ErrSendChannelFull is returned when the outbound channel is full.
var ErrSendChannelFull = errors.New("ws: send channel full")

// ErrConnClosed is returned when attempting to send on a closed connection.
var ErrConnClosed = errors.New("ws: connection closed")

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
	drained     chan struct{} // closed when writePump has drained sendCh to PendingStore
	drainOnce   sync.Once     // ensures drained is closed only once
	isReconnect bool          // true if connection was established via token reconnect
	limiter     *rate.Limiter // per-connection inbound rate limiter; nil = unlimited

	hub  *Hub      // back-reference injected by Hub.register()
	info *ConnInfo // HTTP handshake snapshot; nil if not set
}

func NewConn(id, token string, ws *websocket.Conn, chSize int, limiter *rate.Limiter) *Conn {
	return &Conn{
		id:      id,
		token:   token,
		ws:      ws,
		sendCh:  make(chan MessageEnvelope, chSize),
		done:    make(chan struct{}),
		drained: make(chan struct{}),
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

// Hub returns the Hub that this connection is registered with.
// Available inside OnConnect and all subsequent callbacks.
func (c *Conn) Hub() *Hub { return c.hub }

// RemoteAddr returns the client address, preferring X-Forwarded-For if present.
func (c *Conn) RemoteAddr() string {
	if c.info == nil {
		return ""
	}
	if xff := c.info.Header.Get("X-Forwarded-For"); xff != "" {
		return strings.TrimSpace(strings.SplitN(xff, ",", 2)[0])
	}
	return c.info.RemoteAddr
}

// Header returns the value of the specified HTTP header from the handshake request.
func (c *Conn) Header(key string) string {
	if c.info == nil {
		return ""
	}
	return c.info.Header.Get(key)
}

// Query returns the value of the specified URL query parameter from the handshake request.
func (c *Conn) Query(key string) string {
	if c.info == nil {
		return ""
	}
	return c.info.Query.Get(key)
}

// QueryValues returns the complete URL query parameters from the handshake request.
func (c *Conn) QueryValues() url.Values {
	if c.info == nil {
		return nil
	}
	return c.info.Query
}

// GetString retrieves a metadata value by key and asserts it to string.
func (c *Conn) GetString(key string) (string, bool) {
	v, ok := c.meta.Load(key)
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}

// GetInt64 retrieves a metadata value by key and asserts it to int64.
func (c *Conn) GetInt64(key string) (int64, bool) {
	v, ok := c.meta.Load(key)
	if !ok {
		return 0, false
	}
	i, ok := v.(int64)
	return i, ok
}

// GetBool retrieves a metadata value by key and asserts it to bool.
func (c *Conn) GetBool(key string) (bool, bool) {
	v, ok := c.meta.Load(key)
	if !ok {
		return false, false
	}
	b, ok := v.(bool)
	return b, ok
}

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

// enqueueShared pushes a pre-copied envelope without additional copy.
// Used by Hub.broadcastDirect where the caller has already made one copy
// that is shared (read-only) across all subscribers.
func (c *Conn) enqueueShared(env MessageEnvelope) bool {
	select {
	case <-c.done:
		return false
	default:
	}
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
// Returns ErrConnClosed if the connection has ended, or ErrSendChannelFull
// if the outbound channel is full.
func (c *Conn) SendJSON(v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	env := MessageEnvelope{Data: data, MsgType: websocket.MessageText}
	select {
	case <-c.done:
		return ErrConnClosed
	default:
	}
	select {
	case c.sendCh <- env:
		return nil
	case <-c.done:
		return ErrConnClosed
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

// Drained returns a channel that is closed when the writePump has finished
// draining sendCh to PendingStore. Used by Hub.register() to synchronize
// connection replacement with pending message drain.
func (c *Conn) Drained() <-chan struct{} { return c.drained }

// MarkDrained signals that draining is complete. Safe to call multiple times.
func (c *Conn) MarkDrained() {
	c.drainOnce.Do(func() {
		close(c.drained)
	})
}

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
