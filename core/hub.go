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

// emptyConnSlice is a shared empty snapshot used to initialise topicSubs
// without per-topic allocation. Stored via atomic.Pointer so readers can
// always Load a non-nil slice pointer.
var emptyConnSlice = &[]*Conn{}

// topicSubs holds the subscribers of a single topic.
//
// Design — copy-on-write with direct *Conn caching:
//   - members maps connID → *Conn (the authoritative set). Only mutated
//     under writeMu. Storing *Conn rather than just connID lets the
//     broadcast path skip a sync.Map.Load per subscriber.
//   - snapshot is an immutable []*Conn published via atomic.Pointer.
//     Broadcast reads it lock-free with a single atomic.Load, so the hot
//     path never contends with concurrent Subscribe/Unsubscribe.
//   - On every membership change rebuildSnapshot() builds a fresh slice and
//     atomically swaps it in. The previous slice may still be referenced by
//     in-flight broadcasters; Go's GC reclaims it once they finish.
//
// Trade-off: each Subscribe/Unsubscribe pays an O(N) slice copy. This is
// profitable when broadcast frequency dominates subscription churn (typically
// 10:1 or more in real workloads), because the broadcast hot path becomes a
// pure atomic load + slice walk instead of an RLock + map iteration.
//
// Stale *Conn after replacement: when a connection is replaced, the snapshot
// briefly holds the old *Conn. Calls to enqueueShared on it return false
// (its done channel is closed) and the overflow path sends the message to
// PendingStore keyed by connID, where the new connection picks it up via
// flushPending or drainOverflow.
//
// destroyed flag: a topic with zero members is removed from Hub.topics by
// removeTopicIfEmpty. A concurrent Subscribe that has already obtained the
// *topicSubs pointer must detect that the object is now defunct so it can
// re-resolve via getOrCreateTopic. The flag is set under both topicsMu and
// writeMu to make that hand-off race-free.
type topicSubs struct {
	// writeMu serialises Subscribe/Unsubscribe modifications on this topic.
	// It does not protect the read path — that goes through snapshot.Load.
	writeMu sync.Mutex

	// members maps connID to *Conn. Only modified while writeMu is held.
	// We carry *Conn (not just struct{}) so the broadcast hot path can call
	// enqueueShared directly without a sync.Map lookup.
	members map[string]*Conn

	// snapshot is the latest published slice of subscriber *Conn pointers.
	// Always non-nil after construction; reset to emptyConnSlice when empty.
	snapshot atomic.Pointer[[]*Conn]

	// destroyed is set to true after this *topicSubs has been removed from
	// Hub.topics. Subscribers that obtained the pointer before removal will
	// see this flag and retry through getOrCreateTopic.
	destroyed bool
}

// rebuildSnapshot constructs a fresh []*Conn from members and publishes it.
// Caller must hold writeMu.
//
// Why we don't mutate the previous snapshot in place: in-flight broadcasters
// may already hold a reference to it. Mutating that slice would tear their
// iteration. COW guarantees readers always see a complete, frozen view.
func (t *topicSubs) rebuildSnapshot() {
	if len(t.members) == 0 {
		t.snapshot.Store(emptyConnSlice)
		return
	}
	list := make([]*Conn, 0, len(t.members))
	for _, c := range t.members {
		list = append(list, c)
	}
	t.snapshot.Store(&list)
}

// Hub manages all active connections and topic subscriptions.
// It is safe for concurrent use.
//
// Hub can be used standalone (via NewHub) or obtained from Server.Hub().
//
// Concurrency model after the per-conn-subs + COW refactor:
//   - conns: sync.Map for connID → *Conn (lock-free reads).
//   - topicsMu: protects ONLY the topics map's structure (creation/deletion
//     of *topicSubs entries). Once the *topicSubs pointer is obtained the
//     outer lock is released.
//   - per-topic writeMu: serialises subscribers on that topic only; different
//     topics never contend.
//   - Conn.subsMu: each connection owns its own subscription set; mass
//     disconnects no longer fight for a single hub-wide write lock.
type Hub struct {
	conns     sync.Map // connID → *Conn
	connCount atomic.Int64
	maxConns  int64 // 0 = unlimited
	closed    atomic.Bool

	// topicsMu guards topics map mutations: looking up a topic, creating a
	// new *topicSubs entry, or removing an empty one. It does NOT guard
	// modifications to an existing topic's members — that uses per-topic
	// writeMu. Lock order is always: topicsMu → topic.writeMu.
	topicsMu sync.RWMutex
	topics   map[string]*topicSubs

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
		topics:       make(map[string]*topicSubs),
		logger:       defaultLogger(),
		metrics:      nilMetrics{},
		drainTimeout: 2 * time.Second,
	}
	for _, fn := range opts {
		fn(h)
	}
	return h
}

// getTopic returns the *topicSubs for topic, or nil if it does not exist.
// Read-only path: only takes topicsMu.RLock.
func (h *Hub) getTopic(topic string) *topicSubs {
	h.topicsMu.RLock()
	t := h.topics[topic]
	h.topicsMu.RUnlock()
	return t
}

// getOrCreateTopic returns the existing *topicSubs for topic or creates one.
//
// Why a fast/slow path: 99% of Subscribe calls hit an already-existing topic
// and need only an RLock. Topic creation is rare and goes through the slow
// (write-locked) path with a double-check to avoid duplicate construction.
func (h *Hub) getOrCreateTopic(topic string) *topicSubs {
	h.topicsMu.RLock()
	if t, ok := h.topics[topic]; ok {
		h.topicsMu.RUnlock()
		return t
	}
	h.topicsMu.RUnlock()

	h.topicsMu.Lock()
	defer h.topicsMu.Unlock()
	if t, ok := h.topics[topic]; ok {
		return t
	}
	t := &topicSubs{members: make(map[string]*Conn)}
	t.snapshot.Store(emptyConnSlice)
	h.topics[topic] = t
	return t
}

// removeTopicIfEmpty deletes the *topicSubs from h.topics when it has no
// members. The destroyed flag is set so that concurrent Subscribers holding
// the pointer can detect it is no longer canonical and re-resolve.
//
// Lock order: topicsMu → t.writeMu (always outer-to-inner). Any code that
// already holds t.writeMu must release it before calling this function.
func (h *Hub) removeTopicIfEmpty(topic string, t *topicSubs) {
	h.topicsMu.Lock()
	defer h.topicsMu.Unlock()

	t.writeMu.Lock()
	defer t.writeMu.Unlock()

	// Re-check under both locks: another goroutine may have re-subscribed
	// since we observed the topic was empty, or destruction may already have
	// happened on a parallel path.
	if t.destroyed {
		return
	}
	if len(t.members) != 0 {
		return
	}
	if h.topics[topic] != t {
		return
	}
	delete(h.topics, topic)
	t.destroyed = true
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

		// Clean up the old connection's subscriptions before the new one
		// becomes visible. The old readPump's unregister will see the new
		// conn in h.conns and skip its own cleanup, so this is the only
		// chance to drain its subs.
		h.unsubscribeAll(o)

		// Wait for the old connection's writePump to drain remaining messages
		// to PendingStore before we PopAll for the new connection.
		select {
		case <-o.Drained():
		case <-time.After(h.drainTimeout):
			h.logger.Warn("register: timed out waiting for old conn drain", "connID", c.id)
		}
	}

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

// unregister removes a connection only if it is still the active connection
// for its ID. This prevents a replaced connection's cleanup from removing the
// new one. Returns true if the connection was actually removed.
func (h *Hub) unregister(c *Conn) bool {
	if h.conns.CompareAndDelete(c.id, c) {
		h.connCount.Add(-1)
		h.metrics.DecConnections()
		h.unsubscribeAll(c)
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
// If the connection's send channel is full and a PendingStore is configured,
// the message is buffered for later delivery (writePump's drainOverflow path).
// If the connection is offline, the message goes to relay or PendingStore.
func (h *Hub) Send(ctx context.Context, connID string, data []byte) error {
	if val, ok := h.conns.Load(connID); ok {
		c := val.(*Conn)
		if c.Send(data) {
			return nil
		}
		if h.pendingStore != nil {
			err := h.pendingStore.PushEnvelope(ctx, connID, PendingMessage{Data: data, MsgType: MsgTypeText})
			if err == nil {
				c.signalOverflow()
			}
			return err
		}
		h.logger.Warn("send channel full", "connID", connID)
		h.metrics.IncDrops()
		return ErrSendChannelFull
	}
	if h.relay != nil {
		return h.relay.PublishSend(ctx, connID, data)
	}
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
func (h *Hub) SendBinary(ctx context.Context, connID string, data []byte) error {
	if val, ok := h.conns.Load(connID); ok {
		c := val.(*Conn)
		if c.SendBinary(data) {
			return nil
		}
		if h.pendingStore != nil {
			err := h.pendingStore.PushEnvelope(ctx, connID, PendingMessage{Data: data, MsgType: MsgTypeBinary})
			if err == nil {
				c.signalOverflow()
			}
			return err
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
// online subscribers. In cluster mode, also fans out via the relay.
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

// BroadcastExclude is identical to Broadcast but skips the listed connection IDs.
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

// broadcastDirect is the lock-free hot path of broadcast.
//
// Why this is the centerpiece of the COW redesign:
//   - The old implementation took Hub.mu.RLock and iterated a map to copy
//     out connIDs, then did N sync.Map.Load calls to recover *Conn pointers.
//     With 10K subscribers and 10K broadcasts/s that's 1e8 map accesses per
//     second under a contended read-lock plus 1e8 sync.Map lookups.
//   - The new implementation does a single atomic.Pointer.Load to obtain an
//     immutable []*Conn that members are stored as. The hot loop iterates
//     pointers directly and calls enqueueShared with no further indirection.
//     Subscribe/Unsubscribe publish new snapshots on the side without ever
//     blocking the broadcast path.
//
// The snapshot may contain a *Conn whose owner has just disconnected (e.g.
// during connection replacement). enqueueShared returns false in that case
// because c.done is closed; the overflow path then buffers the message in
// PendingStore keyed by c.id, where the next registration picks it up.
func (h *Hub) broadcastDirect(ctx context.Context, topic string, data []byte, excludeSet map[string]struct{}) {
	t := h.getTopic(topic)
	if t == nil {
		return
	}

	// Single atomic load — the entire snapshot read path. Any concurrent
	// Subscribe/Unsubscribe is publishing into a different *[]*Conn and will
	// not affect the slice we just obtained.
	list := *t.snapshot.Load()
	if len(list) == 0 {
		return
	}

	// Single allocation shared across every consumer of this broadcast:
	//   - enqueueShared puts the same MessageEnvelope on each online
	//     subscriber's sendCh; writePump only reads Data before ws.Write.
	//   - The overflow path below also reuses cp as PendingMessage.Data.
	// Sharing is safe because none of the consumers mutate the slice.
	cp := make([]byte, len(data))
	copy(cp, data)
	env := MessageEnvelope{Data: cp, MsgType: websocket.MessageText}

	for _, c := range list {
		if excludeSet != nil {
			if _, excluded := excludeSet[c.id]; excluded {
				continue
			}
		}
		if c.enqueueShared(env) {
			continue
		}
		// sendCh full or conn already closed — buffer to PendingStore so the
		// message survives across reconnects.
		if h.pendingStore != nil {
			pm := PendingMessage{Data: cp, MsgType: MsgTypeText}
			if err := h.pendingStore.PushEnvelope(ctx, c.id, pm); err != nil {
				h.logger.Warn("broadcast: overflow push failed", "topic", topic, "connID", c.id, "error", err)
				h.metrics.IncDrops()
			} else {
				c.signalOverflow()
			}
			continue
		}
		h.logger.Warn("broadcast: channel full", "topic", topic, "connID", c.id)
		h.metrics.IncDrops()
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
// Returns false if the Hub has been shut down or if the connection has not
// yet been registered (Subscribe must be called after register completes,
// typically from OnMessage or after the upgrade returns).
//
// Why no global write lock: only this topic's writeMu is taken. Subscribes to
// different topics run fully in parallel; the only outer-lock acquisition is
// the slow-path topic creation through getOrCreateTopic.
func (h *Hub) Subscribe(ctx context.Context, connID, topic string) bool {
	if h.closed.Load() {
		return false
	}

	val, ok := h.conns.Load(connID)
	if !ok {
		// Subscribing an unregistered conn would create a stale entry that
		// nothing can clean up (no Conn to track it on). Reject explicitly.
		return false
	}
	c := val.(*Conn)

	// Loop guard against a destroy race: getOrCreateTopic may hand us a
	// *topicSubs that removeTopicIfEmpty marked destroyed before we could
	// take its writeMu. Detect and retry — at most one extra iteration in
	// practice, since destroy holds topicsMu while flipping the flag.
	for {
		t := h.getOrCreateTopic(topic)

		t.writeMu.Lock()
		if t.destroyed {
			t.writeMu.Unlock()
			continue
		}
		if _, already := t.members[connID]; already {
			// Idempotent: avoid rebuilding the snapshot or firing a duplicate
			// callback for a re-subscribe.
			t.writeMu.Unlock()
			return true
		}
		t.members[connID] = c
		t.rebuildSnapshot()
		// Add to the conn's subs while we still hold writeMu so the two
		// indexes (topic.members and conn.subs) stay consistent.
		c.addSub(topic)
		t.writeMu.Unlock()
		break
	}

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
//
// Note that Unsubscribe is also called by Hub on disconnect cleanup
// (via unsubscribeAll) so it tolerates the connection being absent from
// h.conns — only the topic side is required to exist.
func (h *Hub) Unsubscribe(ctx context.Context, connID, topic string) {
	t := h.getTopic(topic)
	if t == nil {
		return
	}

	t.writeMu.Lock()
	if _, was := t.members[connID]; !was {
		t.writeMu.Unlock()
		return
	}
	delete(t.members, connID)
	t.rebuildSnapshot()
	isEmpty := len(t.members) == 0
	t.writeMu.Unlock()

	// Mirror the change on the conn's own set if it is still registered.
	// During disconnect cleanup the conn may already be gone from h.conns;
	// that's fine because unsubscribeAll already drained c.subs.
	if val, ok := h.conns.Load(connID); ok {
		val.(*Conn).removeSub(topic)
	}

	if isEmpty {
		h.removeTopicIfEmpty(topic, t)
	}

	if h.topicHandler != nil {
		h.topicHandler.OnUnsubscribe(ctx, connID, topic)
	}
	if h.relay != nil {
		if err := h.relay.OnUnsubscribe(ctx, connID, topic, isEmpty); err != nil {
			h.logger.Warn("relay: unsubscribe failed", "connID", connID, "topic", topic, "error", err)
		}
	}
}

// unsubscribeAll removes a connection from every topic it had joined.
//
// This is the second big win of the refactor: in the old design every caller
// took Hub.mu.Lock and iterated a hub-wide connTopics map, so 10k disconnects
// at once serialised on a single mutex. Now we drain the per-conn subs set
// (one Conn lock) and then touch each topic independently, so disconnect
// scaling becomes per-conn × average-subs-per-conn instead of global.
func (h *Hub) unsubscribeAll(c *Conn) {
	subs := c.drainSubs()
	if len(subs) == 0 {
		return
	}

	type unsubInfo struct {
		topic      string
		lastOnNode bool
	}
	removed := make([]unsubInfo, 0, len(subs))

	for topic := range subs {
		t := h.getTopic(topic)
		if t == nil {
			continue
		}

		t.writeMu.Lock()
		if t.destroyed {
			t.writeMu.Unlock()
			continue
		}
		if _, was := t.members[c.id]; !was {
			t.writeMu.Unlock()
			continue
		}
		delete(t.members, c.id)
		t.rebuildSnapshot()
		isEmpty := len(t.members) == 0
		t.writeMu.Unlock()

		if isEmpty {
			h.removeTopicIfEmpty(topic, t)
		}
		removed = append(removed, unsubInfo{topic: topic, lastOnNode: isEmpty})
	}

	// Fire callbacks outside any lock to avoid blocking other goroutines if
	// the user-supplied handlers are slow.
	ctx := context.Background()
	for _, ti := range removed {
		if h.topicHandler != nil {
			h.topicHandler.OnUnsubscribe(ctx, c.id, ti.topic)
		}
		if h.relay != nil {
			if err := h.relay.OnUnsubscribe(ctx, c.id, ti.topic, ti.lastOnNode); err != nil {
				h.logger.Warn("relay: unsubscribeAll failed", "connID", c.id, "topic", ti.topic, "error", err)
			}
		}
	}
}

// CloseTopic sends a notification to all subscribers, then removes the topic.
// In cluster mode, only the first OnUnsubscribe call carries lastOnNode=true
// to avoid redundant SRem calls on the relay.
func (h *Hub) CloseTopic(ctx context.Context, topic string, data []byte) {
	// Step 1: detach the *topicSubs from h.topics so no new Subscribe lands.
	h.topicsMu.Lock()
	t, ok := h.topics[topic]
	if !ok {
		h.topicsMu.Unlock()
		return
	}
	delete(h.topics, topic)
	h.topicsMu.Unlock()

	// Step 2: drain members under the topic's writeMu so concurrent
	// Subscribe/Unsubscribe paths see a coherent state.
	t.writeMu.Lock()
	members := t.members
	t.members = nil
	t.snapshot.Store(emptyConnSlice)
	t.destroyed = true
	t.writeMu.Unlock()

	if len(members) == 0 {
		return
	}

	// Step 3: remove this topic from each member's per-conn subs index.
	// Done outside the topic lock; each acquisition is a per-conn lock only.
	// We already hold *Conn directly in members so no sync.Map lookup is needed.
	ids := make([]string, 0, len(members))
	for connID, c := range members {
		ids = append(ids, connID)
		c.removeSub(topic)
	}

	// Step 4: deliver the optional final message.
	if data != nil {
		for _, id := range ids {
			if err := h.Send(ctx, id, data); err != nil &&
				!errors.Is(err, ErrMessageDropped) && !errors.Is(err, ErrSendChannelFull) {
				h.logger.Warn("closeTopic: send failed", "topic", topic, "connID", id, "error", err)
			}
		}
	}

	// Step 5: fire callbacks. The relay only needs lastOnNode=true once to
	// remove this node from the topic set; subsequent calls pass false.
	for i, id := range ids {
		if h.topicHandler != nil {
			h.topicHandler.OnUnsubscribe(ctx, id, topic)
		}
		if h.relay != nil {
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
		h.unsubscribeAll(c)
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
	h.topicsMu.RLock()
	defer h.topicsMu.RUnlock()
	return len(h.topics)
}

// TopicSubscribers returns the connIDs subscribed to a topic.
//
// Reads the snapshot lock-free. The returned slice is a fresh allocation;
// callers may mutate it freely without affecting the hub.
func (h *Hub) TopicSubscribers(topic string) []string {
	t := h.getTopic(topic)
	if t == nil {
		return nil
	}
	src := *t.snapshot.Load()
	if len(src) == 0 {
		return nil
	}
	out := make([]string, 0, len(src))
	for _, c := range src {
		out = append(out, c.id)
	}
	return out
}

// ConnTopics returns all topics a specific connection is subscribed to.
// Returns nil if the connection has no subscriptions or is not registered.
func (h *Hub) ConnTopics(connID string) []string {
	val, ok := h.conns.Load(connID)
	if !ok {
		return nil
	}
	c := val.(*Conn)
	c.subsMu.Lock()
	defer c.subsMu.Unlock()
	if len(c.subs) == 0 {
		return nil
	}
	topics := make([]string, 0, len(c.subs))
	for t := range c.subs {
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

	// Snapshot every conn's subs before tearing them down so callbacks can
	// fire with accurate per-conn state.
	type connSubs struct {
		connID string
		topics []string
	}
	var allConnSubs []connSubs
	var connIDs []string

	h.conns.Range(func(key, value interface{}) bool {
		c := value.(*Conn)
		connIDs = append(connIDs, c.id)
		subs := c.drainSubs()
		if len(subs) > 0 {
			topics := make([]string, 0, len(subs))
			for t := range subs {
				topics = append(topics, t)
			}
			allConnSubs = append(allConnSubs, connSubs{connID: c.id, topics: topics})
		}
		if msg != nil {
			c.Send(msg)
		}
		c.Close("server shutdown")
		c.Finish()
		h.conns.Delete(key)
		return true
	})
	h.connCount.Store(0)

	if h.topicHandler != nil {
		for _, cs := range allConnSubs {
			for _, t := range cs.topics {
				h.topicHandler.OnUnsubscribe(ctx, cs.connID, t)
			}
		}
	}

	h.topicsMu.Lock()
	h.topics = make(map[string]*topicSubs)
	h.topicsMu.Unlock()

	if h.tokenProvider != nil {
		for _, id := range connIDs {
			if err := h.tokenProvider.Revoke(ctx, id); err != nil {
				h.logger.Warn("shutdown: token revoke failed", "connID", id, "error", err)
			}
		}
	}

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
		if h.pendingStore != nil {
			err := h.pendingStore.Push(ctx, connID, data)
			if err == nil {
				c.signalOverflow()
			}
			return err
		}
		h.metrics.IncDrops()
		return ErrSendChannelFull
	}
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
