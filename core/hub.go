package core

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
)

var (
	// ErrMaxConnsReached is returned when the connection limit has been reached.
	ErrMaxConnsReached = errors.New("ws: max connections reached")

	// ErrMessageDropped is returned when a message could not be delivered
	// and no pending store is configured to buffer it.
	ErrMessageDropped = errors.New("ws: message dropped (offline, no pending store)")
)

// Hub manages all active connections and topic subscriptions.
// It is safe for concurrent use.
//
// Hub can be used standalone (via NewHub) or obtained from Server.Hub().
type Hub struct {
	conns     sync.Map // connID → *Conn
	connCount atomic.Int64
	maxConns  int64 // 0 = unlimited

	mu         sync.RWMutex
	topics     map[string]map[string]struct{} // topic → set of connIDs
	connTopics map[string]map[string]struct{} // connID → set of topics (reverse index)

	pendingStore  PendingStore
	logger        Logger
	metrics       MetricsCollector
	topicHandler  TopicEventHandler // optional; may be nil
	tokenProvider TokenProvider     // optional; used by Shutdown to revoke tokens
}

// HubOption configures a Hub created via NewHub.
type HubOption func(*Hub)

// WithHubLogger sets the logger for a standalone Hub.
func WithHubLogger(l Logger) HubOption {
	return func(h *Hub) { h.logger = l }
}

// WithHubMaxConns sets the maximum number of connections for a standalone Hub.
func WithHubMaxConns(n int64) HubOption {
	return func(h *Hub) { h.maxConns = n }
}

// WithHubMetrics sets the MetricsCollector for a standalone Hub.
func WithHubMetrics(m MetricsCollector) HubOption {
	return func(h *Hub) { h.metrics = metricsOrNop(m) }
}

// WithHubPendingStore sets the PendingStore for a standalone Hub.
func WithHubPendingStore(s PendingStore) HubOption {
	return func(h *Hub) { h.pendingStore = s }
}

// WithHubTopicHandler sets the TopicEventHandler for lifecycle callbacks.
func WithHubTopicHandler(th TopicEventHandler) HubOption {
	return func(h *Hub) { h.topicHandler = th }
}

// WithHubTokenProvider sets the TokenProvider so Shutdown can revoke tokens.
func WithHubTokenProvider(tp TokenProvider) HubOption {
	return func(h *Hub) { h.tokenProvider = tp }
}

// NewHub creates a Hub with the given options.
// All options are optional; sensible defaults are used.
func NewHub(opts ...HubOption) *Hub {
	h := &Hub{
		topics:     make(map[string]map[string]struct{}),
		connTopics: make(map[string]map[string]struct{}),
		logger:     defaultLogger(),
		metrics:    nilMetrics{},
	}
	for _, fn := range opts {
		fn(h)
	}
	return h
}

// register adds or replaces a connection and flushes any pending messages.
// Returns ErrMaxConnsReached if the connection limit has been exceeded.
func (h *Hub) register(ctx context.Context, c *Conn) error {
	old, replacing := h.conns.LoadAndDelete(c.id)
	if replacing {
		o := old.(*Conn)
		o.close("replaced")
		o.finish()
		h.connCount.Add(-1)
		h.metrics.DecConnections()
	}

	// Check connection limit (allow replacements even at capacity).
	if !replacing && h.maxConns > 0 && h.connCount.Load() >= h.maxConns {
		return ErrMaxConnsReached
	}

	h.conns.Store(c.id, c)
	h.connCount.Add(1)
	h.metrics.IncConnections()
	h.flushPending(ctx, c)
	return nil
}

// unregister removes a connection only if it is still the active connection for
// its ID. This prevents a replaced connection's cleanup from removing the new one.
// Returns true if the connection was actually removed (permanent disconnect).
func (h *Hub) unregister(c *Conn) bool {
	if h.conns.CompareAndDelete(c.id, c) {
		h.connCount.Add(-1)
		h.metrics.DecConnections()
		h.unsubscribeAll(c.id)
		return true
	}
	return false
}

// Send delivers raw bytes to a specific connection.
// If the connection is offline or the channel is full and a PendingStore is
// configured, the message is persisted for later delivery.
// Returns ErrMessageDropped if the message cannot be delivered and no pending
// store is configured.
func (h *Hub) Send(ctx context.Context, connID string, data []byte) error {
	if val, ok := h.conns.Load(connID); ok {
		c := val.(*Conn)
		if c.Send(data) {
			return nil
		}
		h.logger.Warn("send channel full, buffering", "connID", connID)
	}
	if h.pendingStore != nil {
		return h.pendingStore.Push(ctx, connID, data)
	}
	h.metrics.IncDrops()
	return ErrMessageDropped
}

// SendJSON marshals v and delivers it (convenience wrapper).
func (h *Hub) SendJSON(ctx context.Context, connID string, v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return h.Send(ctx, connID, data)
}

// SendBinary delivers raw binary bytes to a specific connection.
// Uses WebSocket binary message type, suitable for protobuf/msgpack.
// Falls back to PendingStore (as raw bytes) if the connection is offline.
func (h *Hub) SendBinary(ctx context.Context, connID string, data []byte) error {
	if val, ok := h.conns.Load(connID); ok {
		c := val.(*Conn)
		if c.SendBinary(data) {
			return nil
		}
		h.logger.Warn("sendBinary channel full, buffering", "connID", connID)
	}
	if h.pendingStore != nil {
		return h.pendingStore.Push(ctx, connID, data)
	}
	h.metrics.IncDrops()
	return ErrMessageDropped
}

// Broadcast sends raw bytes to every connection subscribed to a topic.
func (h *Hub) Broadcast(ctx context.Context, topic string, data []byte) {
	h.mu.RLock()
	subs, ok := h.topics[topic]
	if !ok {
		h.mu.RUnlock()
		return
	}
	ids := make([]string, 0, len(subs))
	for id := range subs {
		ids = append(ids, id)
	}
	h.mu.RUnlock()

	for _, id := range ids {
		if err := h.Send(ctx, id, data); err != nil && !errors.Is(err, ErrMessageDropped) {
			h.logger.Warn("broadcast: send failed", "topic", topic, "connID", id, "error", err)
		}
	}
}

// BroadcastJSON marshals v and broadcasts it.
func (h *Hub) BroadcastJSON(ctx context.Context, topic string, v interface{}) {
	data, err := json.Marshal(v)
	if err != nil {
		h.logger.Warn("broadcastJSON: marshal failed", "topic", topic, "error", err)
		return
	}
	h.Broadcast(ctx, topic, data)
}

// Subscribe adds a connection to a topic.
// If the Hub has a TopicEventHandler, OnSubscribe is called after the
// subscription is recorded.
func (h *Hub) Subscribe(ctx context.Context, connID, topic string) {
	h.mu.Lock()

	subs := h.topics[topic]
	if subs == nil {
		subs = make(map[string]struct{})
		h.topics[topic] = subs
	}
	subs[connID] = struct{}{}

	ct := h.connTopics[connID]
	if ct == nil {
		ct = make(map[string]struct{})
		h.connTopics[connID] = ct
	}
	ct[topic] = struct{}{}

	h.mu.Unlock()

	if h.topicHandler != nil {
		h.topicHandler.OnSubscribe(ctx, connID, topic)
	}
}

// Unsubscribe removes a connection from a topic.
// If the Hub has a TopicEventHandler, OnUnsubscribe is called after the
// subscription is removed.
func (h *Hub) Unsubscribe(ctx context.Context, connID, topic string) {
	h.mu.Lock()

	if subs, ok := h.topics[topic]; ok {
		delete(subs, connID)
		if len(subs) == 0 {
			delete(h.topics, topic)
		}
	}
	if ct, ok := h.connTopics[connID]; ok {
		delete(ct, topic)
		if len(ct) == 0 {
			delete(h.connTopics, connID)
		}
	}

	h.mu.Unlock()

	if h.topicHandler != nil {
		h.topicHandler.OnUnsubscribe(ctx, connID, topic)
	}
}

// unsubscribeAll removes a connection from all topics using the reverse index.
// Fires OnUnsubscribe for each topic if a TopicEventHandler is configured.
func (h *Hub) unsubscribeAll(connID string) {
	h.mu.Lock()

	ct, ok := h.connTopics[connID]
	if !ok {
		h.mu.Unlock()
		return
	}

	// Collect topics before modifying maps.
	removedTopics := make([]string, 0, len(ct))
	for topic := range ct {
		removedTopics = append(removedTopics, topic)
		if subs, exists := h.topics[topic]; exists {
			delete(subs, connID)
			if len(subs) == 0 {
				delete(h.topics, topic)
			}
		}
	}
	delete(h.connTopics, connID)

	h.mu.Unlock()

	if h.topicHandler != nil {
		ctx := context.Background()
		for _, topic := range removedTopics {
			h.topicHandler.OnUnsubscribe(ctx, connID, topic)
		}
	}
}

// CloseTopic sends a notification to all subscribers, then removes the topic.
func (h *Hub) CloseTopic(ctx context.Context, topic string, data []byte) {
	h.mu.Lock()
	subs, ok := h.topics[topic]
	if !ok {
		h.mu.Unlock()
		return
	}
	ids := make([]string, 0, len(subs))
	for id := range subs {
		ids = append(ids, id)
	}
	delete(h.topics, topic)
	for _, id := range ids {
		if ct, exists := h.connTopics[id]; exists {
			delete(ct, topic)
			if len(ct) == 0 {
				delete(h.connTopics, id)
			}
		}
	}
	h.mu.Unlock()

	if data != nil {
		for _, id := range ids {
			if err := h.Send(ctx, id, data); err != nil && !errors.Is(err, ErrMessageDropped) {
				h.logger.Warn("closeTopic: send failed", "topic", topic, "connID", id, "error", err)
			}
		}
	}

	if h.topicHandler != nil {
		for _, id := range ids {
			h.topicHandler.OnUnsubscribe(ctx, id, topic)
		}
	}
}

// CloseConn gracefully closes a specific connection with a reason.
func (h *Hub) CloseConn(connID, reason string) {
	if val, loaded := h.conns.LoadAndDelete(connID); loaded {
		c := val.(*Conn)
		c.close(reason)
		c.finish()
		h.connCount.Add(-1)
		h.metrics.DecConnections()
	}
	h.unsubscribeAll(connID)
}

// GetConn returns a connection by ID, or nil.
func (h *Hub) GetConn(connID string) *Conn {
	if val, ok := h.conns.Load(connID); ok {
		return val.(*Conn)
	}
	return nil
}

// ConnCount returns the number of active connections (O(1)).
func (h *Hub) ConnCount() int {
	return int(h.connCount.Load())
}

// TopicCount returns the number of active topics.
func (h *Hub) TopicCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.topics)
}

// TopicSubscribers returns the connIDs subscribed to a topic.
func (h *Hub) TopicSubscribers(topic string) []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	subs, ok := h.topics[topic]
	if !ok {
		return nil
	}
	ids := make([]string, 0, len(subs))
	for id := range subs {
		ids = append(ids, id)
	}
	return ids
}

// ConnTopics returns all topics a specific connection is subscribed to.
// Returns nil if the connection has no subscriptions.
func (h *Hub) ConnTopics(connID string) []string {
	h.mu.RLock()
	defer h.mu.RUnlock()
	ct, ok := h.connTopics[connID]
	if !ok {
		return nil
	}
	topics := make([]string, 0, len(ct))
	for t := range ct {
		topics = append(topics, t)
	}
	return topics
}

// Range iterates over all active connections. The callback receives
// the connID and *Conn. Return false from fn to stop iteration.
func (h *Hub) Range(fn func(connID string, conn *Conn) bool) {
	h.conns.Range(func(key, value interface{}) bool {
		return fn(key.(string), value.(*Conn))
	})
}

// Shutdown gracefully closes all connections and clears internal state.
// If finalMessage is provided (non-nil), it is sent to every connection
// before closing, allowing clients to handle the shutdown gracefully.
//
// Shutdown fires OnUnsubscribe callbacks for all active subscriptions
// and revokes tokens via the TokenProvider (if configured via WithHubTokenProvider).
func (h *Hub) Shutdown(finalMessage ...[]byte) {
	var msg []byte
	if len(finalMessage) > 0 {
		msg = finalMessage[0]
	}

	// Collect all connIDs for token revocation later.
	var connIDs []string

	h.conns.Range(func(key, value interface{}) bool {
		c := value.(*Conn)
		connIDs = append(connIDs, c.id)
		if msg != nil {
			c.Send(msg)
		}
		c.close("server shutdown")
		c.finish()
		h.conns.Delete(key)
		return true
	})
	h.connCount.Store(0)

	// Fire OnUnsubscribe callbacks before clearing topic maps.
	if h.topicHandler != nil {
		h.mu.RLock()
		ctx := context.Background()
		for connID, topics := range h.connTopics {
			for topic := range topics {
				h.topicHandler.OnUnsubscribe(ctx, connID, topic)
			}
		}
		h.mu.RUnlock()
	}

	h.mu.Lock()
	h.topics = make(map[string]map[string]struct{})
	h.connTopics = make(map[string]map[string]struct{})
	h.mu.Unlock()

	// Revoke tokens if a provider is configured.
	if h.tokenProvider != nil {
		ctx := context.Background()
		for _, id := range connIDs {
			_ = h.tokenProvider.Revoke(ctx, id)
		}
	}
}

// flushPending delivers buffered messages to a freshly (re)connected client.
// Messages that fail to enqueue are re-pushed to the pending store so they are
// not lost.
func (h *Hub) flushPending(ctx context.Context, c *Conn) {
	if h.pendingStore == nil {
		return
	}
	msgs, err := h.pendingStore.PopAll(ctx, c.id)
	if err != nil {
		h.logger.Warn("flushPending failed", "connID", c.id, "error", err)
		return
	}
	for i, m := range msgs {
		if !c.Send(m) {
			h.logger.Warn("flushPending: channel full, re-buffering remaining", "connID", c.id, "remaining", len(msgs)-i)
			// Re-push the failed message and all subsequent messages.
			for _, remaining := range msgs[i:] {
				if pushErr := h.pendingStore.Push(ctx, c.id, remaining); pushErr != nil {
					h.logger.Error("flushPending: re-push failed", "connID", c.id, "error", pushErr)
				}
			}
			return
		}
	}
}

