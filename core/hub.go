package core

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

var (
	// ErrMaxConnsReached is returned when the connection limit has been reached.
	ErrMaxConnsReached = errors.New("ws: max connections reached")

	// ErrMessageDropped is returned when a message could not be delivered
	// and no pending store is configured to buffer it.
	ErrMessageDropped = errors.New("ws: message dropped (offline, no pending store)")

	// ErrHubShutdown is returned when register is called on a Hub that has been shut down.
	ErrHubShutdown = errors.New("ws: hub is shut down")
)

// Hub manages all active connections and topic subscriptions.
// It is safe for concurrent use.
//
// Hub can be used standalone (via NewHub) or obtained from Server.Hub().
type Hub struct {
	conns     sync.Map // connID → *Conn
	connCount atomic.Int64
	maxConns  int64 // 0 = unlimited
	closed    atomic.Bool

	mu         sync.RWMutex
	topics     map[string]map[string]struct{} // topic → set of connIDs
	connTopics map[string]map[string]struct{} // connID → set of topics (reverse index)

	pendingStore  PendingStore
	logger        Logger
	metrics       MetricsCollector
	topicHandler  TopicEventHandler // optional; may be nil
	tokenProvider TokenProvider     // optional; used by Shutdown to revoke tokens
	relay         ClusterRelay      // optional; nil = single-node mode
	drainTimeout  time.Duration     // max time to wait for old conn drain on replace
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

// WithHubDrainTimeout sets the maximum time to wait for old connection drain
// during connection replacement (default 2s).
func WithHubDrainTimeout(d time.Duration) HubOption {
	return func(h *Hub) { h.drainTimeout = d }
}

// WithHubClusterRelay enables distributed mode with the given relay.
func WithHubClusterRelay(r ClusterRelay) HubOption {
	return func(h *Hub) { h.relay = r }
}

// NewHub creates a Hub with the given options.
// All options are optional; sensible defaults are used.
func NewHub(opts ...HubOption) *Hub {
	h := &Hub{
		topics:       make(map[string]map[string]struct{}),
		connTopics:   make(map[string]map[string]struct{}),
		logger:       defaultLogger(),
		metrics:      nilMetrics{},
		drainTimeout: 2 * time.Second,
	}
	for _, fn := range opts {
		fn(h)
	}
	return h
}

// register adds or replaces a connection and flushes any pending messages.
// Returns ErrMaxConnsReached if the connection limit has been exceeded.
// Returns ErrHubShutdown if the Hub has been shut down.
// NOTE: conn.hub must be set by the caller before calling register.
func (h *Hub) register(ctx context.Context, c *Conn) error {
	if h.closed.Load() {
		return ErrHubShutdown
	}

	old, replacing := h.conns.LoadAndDelete(c.id)
	if replacing {
		o := old.(*Conn)
		o.Close("replaced")
		o.Finish()
		h.connCount.Add(-1)
		h.metrics.DecConnections()

		// Clean up old connection's topic subscriptions so they don't leak.
		// The old readPump's unregister will return false (CompareAndDelete fails)
		// and skip unsubscribeAll, so we must do it here.
		h.unsubscribeAll(c.id)

		// Wait for the old connection's writePump to drain remaining messages
		// to PendingStore before we PopAll for the new connection.
		select {
		case <-o.Drained():
		case <-time.After(h.drainTimeout):
			h.logger.Warn("register: timed out waiting for old conn drain", "connID", c.id)
		}
	}

	// Check connection limit (allow replacements even at capacity).
	if !replacing && h.maxConns > 0 && h.connCount.Load() >= h.maxConns {
		return ErrMaxConnsReached
	}

	// Flush pending BEFORE making the connection visible via conns.Store,
	// so buffered messages land in sendCh before any new Broadcast/Send.
	h.flushPending(ctx, c)
	h.conns.Store(c.id, c)
	h.connCount.Add(1)
	h.metrics.IncConnections()

	if h.relay != nil {
		if err := h.relay.OnRegister(ctx, c.id); err != nil {
			h.logger.Warn("relay: register failed", "connID", c.id, "error", err)
		}
	}
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
		if h.relay != nil {
			if err := h.relay.OnUnregister(context.Background(), c.id); err != nil {
				h.logger.Warn("relay: unregister failed", "connID", c.id, "error", err)
			}
		}
		return true
	}
	return false
}

// Send delivers raw bytes to a specific connection.
// If the connection is offline and a PendingStore is configured, the message
// is persisted for later delivery.
// Returns ErrSendChannelFull if the connection is online but the channel is full.
// Returns ErrMessageDropped if the connection is offline and no pending store
// is configured to buffer it.
func (h *Hub) Send(ctx context.Context, connID string, data []byte) error {
	if val, ok := h.conns.Load(connID); ok {
		c := val.(*Conn)
		if c.Send(data) {
			return nil
		}
		// sendCh full — overflow to PendingStore so writePump can drain later.
		if h.pendingStore != nil {
			c.markOverflow()
			return h.pendingStore.PushEnvelope(ctx, connID, PendingMessage{Data: data, MsgType: MsgTypeText})
		}
		h.logger.Warn("send channel full", "connID", connID)
		h.metrics.IncDrops()
		return ErrSendChannelFull
	}
	// Connection not local — try relay for cross-node delivery.
	if h.relay != nil {
		return h.relay.PublishSend(ctx, connID, data)
	}
	// Single-node mode — push to PendingStore for delivery on reconnect.
	if h.pendingStore != nil {
		return h.pendingStore.PushEnvelope(ctx, connID, PendingMessage{Data: data, MsgType: MsgTypeText})
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
// Falls back to PendingStore (as raw bytes) only if the connection is offline.
// Returns ErrSendChannelFull if the connection is online but the channel is full.
func (h *Hub) SendBinary(ctx context.Context, connID string, data []byte) error {
	if val, ok := h.conns.Load(connID); ok {
		c := val.(*Conn)
		if c.SendBinary(data) {
			return nil
		}
		if h.pendingStore != nil {
			c.markOverflow()
			return h.pendingStore.PushEnvelope(ctx, connID, PendingMessage{Data: data, MsgType: MsgTypeBinary})
		}
		h.logger.Warn("sendBinary channel full", "connID", connID)
		h.metrics.IncDrops()
		return ErrSendChannelFull
	}
	if h.relay != nil {
		return h.relay.PublishSend(ctx, connID, data)
	}
	if h.pendingStore != nil {
		return h.pendingStore.PushEnvelope(ctx, connID, PendingMessage{Data: data, MsgType: MsgTypeBinary})
	}
	h.metrics.IncDrops()
	return ErrMessageDropped
}

// Broadcast sends raw bytes to every connection subscribed to a topic.
// Uses zero-copy optimization: data is copied once and shared across all
// subscribers' sendCh, avoiding N copies for N subscribers.
// In cluster mode, also fans out to other nodes via the relay.
func (h *Hub) Broadcast(ctx context.Context, topic string, data []byte) {
	h.broadcastDirect(ctx, topic, data, nil)
	if h.relay != nil {
		_ = h.relay.PublishBroadcast(ctx, topic, data, nil)
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

// BroadcastExclude sends raw bytes to every connection subscribed to a topic,
// but skips the specified connection IDs. This is equivalent to Socket.IO's
// socket.to(room).emit() which excludes the sender.
func (h *Hub) BroadcastExclude(ctx context.Context, topic string, data []byte, excludeIDs ...string) {
	excludeSet := make(map[string]struct{}, len(excludeIDs))
	for _, eid := range excludeIDs {
		excludeSet[eid] = struct{}{}
	}
	h.broadcastDirect(ctx, topic, data, excludeSet)
	if h.relay != nil {
		_ = h.relay.PublishBroadcast(ctx, topic, data, excludeIDs)
	}
}

// broadcastDirect is the shared broadcast implementation with zero-copy optimization.
// Data is copied once and the same []byte is shared across all subscribers' sendCh.
// This is safe because writePump only reads the data (ws.Write) and never modifies it.
// Connections that are offline fall back to PendingStore via Hub.Send (with copy).
func (h *Hub) broadcastDirect(ctx context.Context, topic string, data []byte, excludeSet map[string]struct{}) {
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

	// Single copy for all online subscribers.
	cp := make([]byte, len(data))
	copy(cp, data)
	env := MessageEnvelope{Data: cp, MsgType: websocket.MessageText}

	for _, id := range ids {
		if excludeSet != nil {
			if _, excluded := excludeSet[id]; excluded {
				continue
			}
		}
		if val, ok := h.conns.Load(id); ok {
			c := val.(*Conn)
			if c.enqueueShared(env) {
				continue
			}
			// Online but channel full — overflow to PendingStore.
			if h.pendingStore != nil {
				c.markOverflow()
				pendingCp := make([]byte, len(data))
				copy(pendingCp, data)
				pm := PendingMessage{Data: pendingCp, MsgType: MsgTypeText}
				if err := h.pendingStore.PushEnvelope(ctx, id, pm); err != nil {
					h.logger.Warn("broadcast: overflow push failed", "topic", topic, "connID", id, "error", err)
					h.metrics.IncDrops()
				}
				continue
			}
			h.logger.Warn("broadcast: channel full", "topic", topic, "connID", id)
			h.metrics.IncDrops()
			continue
		}
		// Offline — fall back to PendingStore (needs its own copy since env.Data is shared).
		if h.pendingStore != nil {
			pendingCp := make([]byte, len(data))
			copy(pendingCp, data)
			pm := PendingMessage{Data: pendingCp, MsgType: MsgTypeText}
			if err := h.pendingStore.PushEnvelope(ctx, id, pm); err != nil {
				h.logger.Warn("broadcast: pending push failed", "topic", topic, "connID", id, "error", err)
				h.metrics.IncDrops()
			}
		} else {
			h.metrics.IncDrops()
		}
	}
}

// PreparedMessage is a pre-serialized message that avoids repeated json.Marshal
// when the same payload is broadcast to multiple topics.
type PreparedMessage struct {
	data []byte
}

// NewPreparedMessage marshals v to JSON and wraps it as a PreparedMessage.
func NewPreparedMessage(v interface{}) (*PreparedMessage, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return &PreparedMessage{data: data}, nil
}

// NewPreparedMessageFromBytes wraps raw bytes as a PreparedMessage.
func NewPreparedMessageFromBytes(data []byte) *PreparedMessage {
	return &PreparedMessage{data: data}
}

// BroadcastPrepared broadcasts a pre-serialized PreparedMessage to a topic.
func (h *Hub) BroadcastPrepared(ctx context.Context, topic string, msg *PreparedMessage) {
	h.Broadcast(ctx, topic, msg.data)
}

// BroadcastPreparedExclude broadcasts a pre-serialized PreparedMessage to a topic,
// excluding the specified connection IDs.
func (h *Hub) BroadcastPreparedExclude(ctx context.Context, topic string, msg *PreparedMessage, excludeIDs ...string) {
	h.BroadcastExclude(ctx, topic, msg.data, excludeIDs...)
}

// Subscribe adds a connection to a topic.
// Returns false only if the Hub has been shut down.
// Subscriptions created during OnConnect (before register) are safe because
// unregister always calls unsubscribeAll to clean up.
// If the Hub has a TopicEventHandler, OnSubscribe is called after the
// subscription is recorded.
func (h *Hub) Subscribe(ctx context.Context, connID, topic string) bool {
	if h.closed.Load() {
		return false
	}

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
	if h.relay != nil {
		if err := h.relay.OnSubscribe(ctx, connID, topic); err != nil {
			h.logger.Warn("relay: subscribe failed", "connID", connID, "topic", topic, "error", err)
		}
	}
	return true
}

// Unsubscribe removes a connection from a topic.
// If the Hub has a TopicEventHandler, OnUnsubscribe is called after the
// subscription is removed.
func (h *Hub) Unsubscribe(ctx context.Context, connID, topic string) {
	h.mu.Lock()

	lastOnNode := false
	if subs, ok := h.topics[topic]; ok {
		delete(subs, connID)
		if len(subs) == 0 {
			delete(h.topics, topic)
			lastOnNode = true
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
	if h.relay != nil {
		if err := h.relay.OnUnsubscribe(ctx, connID, topic, lastOnNode); err != nil {
			h.logger.Warn("relay: unsubscribe failed", "connID", connID, "topic", topic, "error", err)
		}
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

	type topicInfo struct {
		name       string
		lastOnNode bool
	}

	// Collect topics before modifying maps.
	removed := make([]topicInfo, 0, len(ct))
	for topic := range ct {
		last := false
		if subs, exists := h.topics[topic]; exists {
			delete(subs, connID)
			if len(subs) == 0 {
				delete(h.topics, topic)
				last = true
			}
		}
		removed = append(removed, topicInfo{name: topic, lastOnNode: last})
	}
	delete(h.connTopics, connID)

	h.mu.Unlock()

	ctx := context.Background()
	for _, ti := range removed {
		if h.topicHandler != nil {
			h.topicHandler.OnUnsubscribe(ctx, connID, ti.name)
		}
		if h.relay != nil {
			if err := h.relay.OnUnsubscribe(ctx, connID, ti.name, ti.lastOnNode); err != nil {
				h.logger.Warn("relay: unsubscribeAll failed", "connID", connID, "topic", ti.name, "error", err)
			}
		}
	}
}

// CloseTopic sends a notification to all subscribers, then removes the topic.
// In cluster mode, notifies the relay that this node no longer has subscribers
// for the topic (lastOnNode = true since the entire topic is removed).
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
			if err := h.Send(ctx, id, data); err != nil &&
				!errors.Is(err, ErrMessageDropped) && !errors.Is(err, ErrSendChannelFull) {
				h.logger.Warn("closeTopic: send failed", "topic", topic, "connID", id, "error", err)
			}
		}
	}

	for i, id := range ids {
		if h.topicHandler != nil {
			h.topicHandler.OnUnsubscribe(ctx, id, topic)
		}
		if h.relay != nil {
			// Only the first call needs lastOnNode=true to remove the node from
			// the topic set; subsequent calls pass false to avoid redundant SRem.
			if err := h.relay.OnUnsubscribe(ctx, id, topic, i == 0); err != nil {
				h.logger.Warn("closeTopic: relay unsubscribe failed", "topic", topic, "connID", id, "error", err)
			}
		}
	}
}

// CloseConn gracefully closes a specific connection with a reason.
// Only unsubscribes if the connection was actually found and removed,
// preventing a race where a new connection with the same ID could have
// its subscriptions incorrectly cleared.
// Also revokes the reconnect token if a TokenProvider is configured.
func (h *Hub) CloseConn(connID, reason string) {
	if val, loaded := h.conns.LoadAndDelete(connID); loaded {
		c := val.(*Conn)
		c.Close(reason)
		c.Finish()
		h.connCount.Add(-1)
		h.metrics.DecConnections()
		h.unsubscribeAll(connID)
		if h.relay != nil {
			if err := h.relay.OnUnregister(context.Background(), connID); err != nil {
				h.logger.Warn("closeConn: relay unregister failed", "connID", connID, "error", err)
			}
		}
		if h.tokenProvider != nil {
			if err := h.tokenProvider.Revoke(context.Background(), connID); err != nil {
				h.logger.Warn("closeConn: token revoke failed", "connID", connID, "error", err)
			}
		}
	}
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
// The context controls the maximum time for OnUnsubscribe callbacks and token
// revocation. If finalMessage is provided (non-nil), it is sent to every
// connection before closing, allowing clients to handle the shutdown gracefully.
//
// Shutdown fires OnUnsubscribe callbacks for all active subscriptions
// and revokes tokens via the TokenProvider (if configured via WithHubTokenProvider).
func (h *Hub) Shutdown(ctx context.Context, finalMessage ...[]byte) {
	h.closed.Store(true)

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
		c.Close("server shutdown")
		c.Finish()
		h.conns.Delete(key)
		return true
	})
	h.connCount.Store(0)

	// Fire OnUnsubscribe callbacks before clearing topic maps.
	if h.topicHandler != nil {
		h.mu.RLock()
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
		for _, id := range connIDs {
			if err := h.tokenProvider.Revoke(ctx, id); err != nil {
				h.logger.Warn("shutdown: token revoke failed", "connID", id, "error", err)
			}
		}
	}

	// Stop cluster relay.
	if h.relay != nil {
		if err := h.relay.Stop(); err != nil {
			h.logger.Warn("shutdown: relay stop failed", "error", err)
		}
	}
}

// ── RelayHandler implementation (used by ClusterRelay to deliver cross-node messages) ──

// LocalSend delivers data to a locally connected client.
func (h *Hub) LocalSend(ctx context.Context, connID string, data []byte) error {
	if val, ok := h.conns.Load(connID); ok {
		c := val.(*Conn)
		if c.Send(data) {
			return nil
		}
		// sendCh full — overflow to PendingStore.
		if h.pendingStore != nil {
			c.markOverflow()
			return h.pendingStore.Push(ctx, connID, data)
		}
		h.metrics.IncDrops()
		return ErrSendChannelFull
	}
	// Connection migrated away — buffer for next reconnect.
	if h.pendingStore != nil {
		return h.pendingStore.Push(ctx, connID, data)
	}
	h.metrics.IncDrops()
	return ErrMessageDropped
}

// LocalBroadcast delivers data to all local subscribers of a topic.
func (h *Hub) LocalBroadcast(ctx context.Context, topic string, data []byte, excludeIDs []string) {
	var excludeSet map[string]struct{}
	if len(excludeIDs) > 0 {
		excludeSet = make(map[string]struct{}, len(excludeIDs))
		for _, eid := range excludeIDs {
			excludeSet[eid] = struct{}{}
		}
	}
	h.broadcastDirect(ctx, topic, data, excludeSet)
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
		ok := false
		if m.MsgType == MsgTypeBinary {
			ok = c.SendBinary(m.Data)
		} else {
			ok = c.Send(m.Data)
		}
		if !ok {
			h.logger.Warn("flushPending: channel full, re-buffering remaining", "connID", c.id, "remaining", len(msgs)-i)
			// Re-push the failed message and all subsequent messages.
			// If any push fails (e.g. Redis unavailable), stop immediately
			// since subsequent pushes will also fail.
			for _, remaining := range msgs[i:] {
				if pushErr := h.pendingStore.PushEnvelope(ctx, c.id, remaining); pushErr != nil {
					h.logger.Error("flushPending: re-push failed, dropping remaining messages",
						"connID", c.id, "error", pushErr, "dropped", len(msgs)-i)
					return
				}
			}
			return
		}
	}
}
