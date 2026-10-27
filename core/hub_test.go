package core

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// ── Mock PendingStore ────────────────────────────────────────────────────────

type memPendingStore struct {
	mu   sync.Mutex
	data map[string][]PendingMessage
}

func newMemPendingStore() *memPendingStore {
	return &memPendingStore{data: make(map[string][]PendingMessage)}
}

func (s *memPendingStore) Push(_ context.Context, connID string, data []byte) error {
	return s.PushEnvelope(context.Background(), connID, PendingMessage{Data: data, MsgType: MsgTypeText})
}

func (s *memPendingStore) PushEnvelope(_ context.Context, connID string, msg PendingMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[connID] = append(s.data[connID], msg)
	return nil
}

func (s *memPendingStore) PushFront(_ context.Context, connID string, msgs []PendingMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[connID] = append(append([]PendingMessage(nil), msgs...), s.data[connID]...)
	return nil
}

func (s *memPendingStore) PopAll(_ context.Context, connID string) ([]PendingMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	msgs := s.data[connID]
	delete(s.data, connID)
	return msgs, nil
}

func (s *memPendingStore) count(connID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.data[connID])
}

// ── Mock ClusterRelay ────────────────────────────────────────────────────────

type mockRelay struct {
	mu           sync.Mutex
	registers    []string
	unregisters  []string
	subscribes   []string
	unsubscribes []string
	sends        []string
	broadcasts   []string
}

func (m *mockRelay) Start(RelayHandler) error { return nil }
func (m *mockRelay) Stop() error              { return nil }

func (m *mockRelay) OnRegister(_ context.Context, connID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.registers = append(m.registers, connID)
	return nil
}

func (m *mockRelay) OnUnregister(_ context.Context, connID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.unregisters = append(m.unregisters, connID)
	return nil
}

func (m *mockRelay) OnSubscribe(_ context.Context, _, topic string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.subscribes = append(m.subscribes, topic)
	return nil
}

func (m *mockRelay) OnUnsubscribe(_ context.Context, _, topic string, _ bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.unsubscribes = append(m.unsubscribes, topic)
	return nil
}

func (m *mockRelay) PublishSend(_ context.Context, connID string, _ []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sends = append(m.sends, connID)
	return nil
}

func (m *mockRelay) PublishBroadcast(_ context.Context, topic string, _ []byte, _ []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.broadcasts = append(m.broadcasts, topic)
	return nil
}

// ── Helper ───────────────────────────────────────────────────────────────────

func testConn(id string) *Conn {
	return &Conn{
		id:          id,
		sendCh:      make(chan MessageEnvelope, 256),
		done:        make(chan struct{}),
		drained:     make(chan struct{}),
		overflowSig: make(chan struct{}, 1),
	}
}

// ── Tests ────────────────────────────────────────────────────────────────────

func TestHub_RegisterUnregister(t *testing.T) {
	h := NewHub()
	c := testConn("u1")
	c.hub = h

	if err := h.register(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	if h.ConnCount() != 1 {
		t.Fatalf("expected 1, got %d", h.ConnCount())
	}

	if !h.unregister(c) {
		t.Fatal("unregister should return true")
	}
	if h.ConnCount() != 0 {
		t.Fatalf("expected 0, got %d", h.ConnCount())
	}

	// Double unregister should be no-op.
	if h.unregister(c) {
		t.Fatal("second unregister should return false")
	}
}

func TestHub_Send_LocalDelivery(t *testing.T) {
	h := NewHub()
	c := testConn("u1")
	c.hub = h
	_ = h.register(context.Background(), c)

	if err := h.Send(context.Background(), "u1", []byte("hello")); err != nil {
		t.Fatal(err)
	}

	select {
	case env := <-c.sendCh:
		if string(env.Data) != "hello" {
			t.Fatalf("expected hello, got %s", env.Data)
		}
	default:
		t.Fatal("expected message in sendCh")
	}
}

func TestHub_Send_OfflinePending(t *testing.T) {
	ps := newMemPendingStore()
	h := NewHub(WithHubPendingStore(ps))

	err := h.Send(context.Background(), "offline-user", []byte("buffered"))
	if err != nil {
		t.Fatal(err)
	}
	if ps.count("offline-user") != 1 {
		t.Fatal("expected 1 pending message")
	}
}

func TestHub_Send_OfflineRelay(t *testing.T) {
	relay := &mockRelay{}
	h := NewHub(WithHubClusterRelay(relay))

	_ = h.Send(context.Background(), "remote-user", []byte("data"))
	if len(relay.sends) != 1 || relay.sends[0] != "remote-user" {
		t.Fatalf("expected relay send, got %v", relay.sends)
	}
}

func TestHub_Broadcast(t *testing.T) {
	h := NewHub()
	c1 := testConn("u1")
	c1.hub = h
	c2 := testConn("u2")
	c2.hub = h
	ctx := context.Background()

	_ = h.register(ctx, c1)
	_ = h.register(ctx, c2)

	h.Subscribe(ctx, c1, "room1")
	h.Subscribe(ctx, c2, "room1")

	h.Broadcast(ctx, "room1", []byte("msg"))

	for _, c := range []*Conn{c1, c2} {
		select {
		case env := <-c.sendCh:
			if string(env.Data) != "msg" {
				t.Fatalf("expected msg, got %s", env.Data)
			}
		default:
			t.Fatalf("no message for %s", c.id)
		}
	}
}

func TestHub_BroadcastExclude(t *testing.T) {
	h := NewHub()
	c1 := testConn("u1")
	c1.hub = h
	c2 := testConn("u2")
	c2.hub = h
	ctx := context.Background()

	_ = h.register(ctx, c1)
	_ = h.register(ctx, c2)
	h.Subscribe(ctx, c1, "room1")
	h.Subscribe(ctx, c2, "room1")

	h.BroadcastExclude(ctx, "room1", []byte("msg"), "u1")

	// u2 should receive, u1 should not.
	select {
	case <-c1.sendCh:
		t.Fatal("u1 should be excluded")
	default:
	}
	select {
	case env := <-c2.sendCh:
		if string(env.Data) != "msg" {
			t.Fatal("wrong data")
		}
	default:
		t.Fatal("u2 should receive")
	}
}

func TestHub_Subscribe_Unsubscribe(t *testing.T) {
	h := NewHub()
	ctx := context.Background()

	// Subscribe now requires the conn to be registered first so the per-conn
	// subs index can stay consistent with the topic side.
	c := testConn("u1")
	c.hub = h
	_ = h.register(ctx, c)

	h.Subscribe(ctx, c, "t1")
	h.Subscribe(ctx, c, "t2")

	topics := h.ConnTopics("u1")
	if len(topics) != 2 {
		t.Fatalf("expected 2 topics, got %d", len(topics))
	}

	h.Unsubscribe(ctx, c, "t1")
	topics = h.ConnTopics("u1")
	if len(topics) != 1 {
		t.Fatalf("expected 1 topic, got %d", len(topics))
	}

	subs := h.TopicSubscribers("t1")
	if len(subs) != 0 {
		t.Fatal("t1 should have no subscribers")
	}
}

// register intentionally does not flush the PendingStore: connIDs are fresh
// per connection, so a buffer keyed by connID is never reclaimed by a later
// connection (see hub.go). This pins that contract.
func TestHub_Register_DoesNotFlushPending(t *testing.T) {
	ps := newMemPendingStore()
	h := NewHub(WithHubPendingStore(ps))
	_ = ps.PushEnvelope(context.Background(), "u1", PendingMessage{Data: []byte("old"), MsgType: MsgTypeText})

	c := testConn("u1")
	c.hub = h
	if err := h.register(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	if len(c.sendCh) != 0 {
		t.Fatalf("register must not deliver pending messages, sendCh has %d", len(c.sendCh))
	}
	if ps.count("u1") != 1 {
		t.Fatalf("register must not pop the pending store, count = %d", ps.count("u1"))
	}
}

// flushPending is not called by register, but it is kept for callers that do
// replay; it must preserve type and order, and put what doesn't fit back at
// the head of the store.
func TestHub_FlushPending_PreservesTypeOrderAndRequeuesFront(t *testing.T) {
	ps := newMemPendingStore()
	h := NewHub(WithHubPendingStore(ps), WithHubLogger(NopLogger{}))
	ctx := context.Background()
	_ = ps.PushEnvelope(ctx, "u1", PendingMessage{Data: []byte{0xDE, 0xAD}, MsgType: MsgTypeBinary})
	_ = ps.PushEnvelope(ctx, "u1", PendingMessage{Data: []byte("t1"), MsgType: MsgTypeText})
	_ = ps.PushEnvelope(ctx, "u1", PendingMessage{Data: []byte("t2"), MsgType: MsgTypeText})
	_ = ps.PushEnvelope(ctx, "u1", PendingMessage{Data: []byte("t3"), MsgType: MsgTypeText})

	c := testConn("u1")
	c.sendCh = make(chan MessageEnvelope, 2) // room for two of the four
	h.flushPending(ctx, c)

	env := <-c.sendCh
	if env.MsgType != websocket.MessageBinary || string(env.Data) != "\xde\xad" {
		t.Fatalf("first = %v %q, want binary dead", env.MsgType, env.Data)
	}
	env = <-c.sendCh
	if env.MsgType != websocket.MessageText || string(env.Data) != "t1" {
		t.Fatalf("second = %v %q, want text t1", env.MsgType, env.Data)
	}
	rest, _ := ps.PopAll(ctx, "u1")
	if len(rest) != 2 || string(rest[0].Data) != "t2" || string(rest[1].Data) != "t3" {
		t.Fatalf("requeued = %+v, want [t2 t3] in order", rest)
	}
}

func TestHub_Relay_RegisterUnregister(t *testing.T) {
	relay := &mockRelay{}
	h := NewHub(WithHubClusterRelay(relay))
	c := testConn("u1")
	c.hub = h
	ctx := context.Background()

	_ = h.register(ctx, c)
	if len(relay.registers) != 1 {
		t.Fatal("expected 1 register call")
	}

	h.unregister(c)
	if len(relay.unregisters) != 1 {
		t.Fatal("expected 1 unregister call")
	}
}

func TestHub_Relay_SubscribeUnsubscribe(t *testing.T) {
	relay := &mockRelay{}
	h := NewHub(WithHubClusterRelay(relay))
	ctx := context.Background()

	c := testConn("u1")
	c.hub = h
	_ = h.register(ctx, c)

	h.Subscribe(ctx, c, "topic1")
	if len(relay.subscribes) != 1 {
		t.Fatal("expected 1 subscribe relay call")
	}

	h.Unsubscribe(ctx, c, "topic1")
	if len(relay.unsubscribes) != 1 {
		t.Fatal("expected 1 unsubscribe relay call")
	}
}

func TestHub_Relay_BroadcastPublishes(t *testing.T) {
	relay := &mockRelay{}
	h := NewHub(WithHubClusterRelay(relay))
	ctx := context.Background()

	c := testConn("u1")
	c.hub = h
	_ = h.register(ctx, c)

	h.Subscribe(ctx, c, "topic1")
	h.Broadcast(ctx, "topic1", []byte("data"))

	if len(relay.broadcasts) != 1 || relay.broadcasts[0] != "topic1" {
		t.Fatalf("expected relay broadcast, got %v", relay.broadcasts)
	}
}

func TestHub_CloseTopic_NotifiesRelay(t *testing.T) {
	relay := &mockRelay{}
	h := NewHub(WithHubClusterRelay(relay))
	ctx := context.Background()

	// Register both conns first — Subscribe requires it.
	c1 := testConn("u1")
	c1.hub = h
	_ = h.register(ctx, c1)
	c2 := testConn("u2")
	c2.hub = h
	_ = h.register(ctx, c2)

	// Subscribe two connections to a topic.
	h.Subscribe(ctx, c1, "events")
	h.Subscribe(ctx, c2, "events")

	// Reset to isolate CloseTopic calls.
	relay.mu.Lock()
	relay.unsubscribes = nil
	relay.mu.Unlock()

	// Close the topic — should fire OnUnsubscribe for each subscriber.
	h.CloseTopic(ctx, "events", nil)

	relay.mu.Lock()
	defer relay.mu.Unlock()

	if len(relay.unsubscribes) != 2 {
		t.Fatalf("expected 2 unsubscribe relay calls, got %d: %v", len(relay.unsubscribes), relay.unsubscribes)
	}
	// Both should be for topic "events".
	for _, topic := range relay.unsubscribes {
		if topic != "events" {
			t.Fatalf("expected topic 'events', got %q", topic)
		}
	}

	// Topic should be gone from Hub.
	if h.TopicCount() != 0 {
		t.Fatalf("expected 0 topics, got %d", h.TopicCount())
	}
}

func TestHub_CloseTopic_NotifiesRelay_NoSubscribers(t *testing.T) {
	relay := &mockRelay{}
	h := NewHub(WithHubClusterRelay(relay))
	ctx := context.Background()

	// CloseTopic on a non-existent topic should be a no-op.
	h.CloseTopic(ctx, "phantom", nil)

	relay.mu.Lock()
	defer relay.mu.Unlock()

	if len(relay.unsubscribes) != 0 {
		t.Fatalf("expected 0 unsubscribe calls for phantom topic, got %d", len(relay.unsubscribes))
	}
}

func TestHub_UnsubscribeAll_NotifiesRelay(t *testing.T) {
	relay := &mockRelay{}
	h := NewHub(WithHubClusterRelay(relay))
	c := testConn("u1")
	c.hub = h
	ctx := context.Background()

	_ = h.register(ctx, c)
	h.Subscribe(ctx, c, "t1")
	h.Subscribe(ctx, c, "t2")

	// Reset to isolate unsubscribeAll calls.
	relay.mu.Lock()
	relay.unsubscribes = nil
	relay.mu.Unlock()

	// Unregister triggers unsubscribeAll.
	h.unregister(c)

	relay.mu.Lock()
	defer relay.mu.Unlock()

	if len(relay.unsubscribes) != 2 {
		t.Fatalf("expected 2 unsubscribe relay calls from unsubscribeAll, got %d", len(relay.unsubscribes))
	}
}

func TestHub_LocalSend(t *testing.T) {
	h := NewHub()
	c := testConn("u1")
	c.hub = h
	_ = h.register(context.Background(), c)

	if err := h.LocalSend(context.Background(), "u1", []byte("cross-node")); err != nil {
		t.Fatal(err)
	}

	select {
	case env := <-c.sendCh:
		if string(env.Data) != "cross-node" {
			t.Fatal("wrong data")
		}
	default:
		t.Fatal("expected message")
	}
}

func TestHub_LocalBroadcast(t *testing.T) {
	h := NewHub()
	c := testConn("u1")
	c.hub = h
	ctx := context.Background()

	_ = h.register(ctx, c)
	h.Subscribe(ctx, c, "room")

	h.LocalBroadcast(ctx, "room", []byte("from-other-node"), nil)

	select {
	case env := <-c.sendCh:
		if string(env.Data) != "from-other-node" {
			t.Fatal("wrong data")
		}
	default:
		t.Fatal("expected message")
	}
}

func TestHub_Send_OverflowToPending(t *testing.T) {
	ps := newMemPendingStore()
	h := NewHub(WithHubPendingStore(ps))
	// Create a connection with sendCh size=1 to easily trigger overflow.
	c := &Conn{
		id:          "u1",
		sendCh:      make(chan MessageEnvelope, 1),
		done:        make(chan struct{}),
		drained:     make(chan struct{}),
		overflowSig: make(chan struct{}, 1),
	}
	c.hub = h
	_ = h.register(context.Background(), c)

	ctx := context.Background()

	// First send — goes into sendCh (capacity 1).
	if err := h.Send(ctx, "u1", []byte("msg1")); err != nil {
		t.Fatalf("first send: %v", err)
	}
	// Second send — sendCh full, should overflow to PendingStore.
	if err := h.Send(ctx, "u1", []byte("msg2")); err != nil {
		t.Fatalf("second send (overflow): %v", err)
	}
	// Third send — also overflow.
	if err := h.Send(ctx, "u1", []byte("msg3")); err != nil {
		t.Fatalf("third send (overflow): %v", err)
	}

	// Verify: 1 message in sendCh, 2 in PendingStore.
	if len(c.sendCh) != 1 {
		t.Fatalf("expected 1 in sendCh, got %d", len(c.sendCh))
	}
	if ps.count("u1") != 2 {
		t.Fatalf("expected 2 in pending store, got %d", ps.count("u1"))
	}

	// Verify overflow flag is set.
	if !c.hasOverflow() {
		t.Fatal("expected overflow flag to be set")
	}
}

func TestHub_Broadcast_OverflowToPending(t *testing.T) {
	ps := newMemPendingStore()
	h := NewHub(WithHubPendingStore(ps))
	// sendCh size=1.
	c := &Conn{
		id:          "u1",
		sendCh:      make(chan MessageEnvelope, 1),
		done:        make(chan struct{}),
		drained:     make(chan struct{}),
		overflowSig: make(chan struct{}, 1),
	}
	c.hub = h
	_ = h.register(context.Background(), c)
	h.Subscribe(context.Background(), c, "room")

	ctx := context.Background()

	// First broadcast fills sendCh.
	h.Broadcast(ctx, "room", []byte("b1"))
	// Second broadcast — sendCh full, should overflow.
	h.Broadcast(ctx, "room", []byte("b2"))

	if len(c.sendCh) != 1 {
		t.Fatalf("expected 1 in sendCh, got %d", len(c.sendCh))
	}
	if ps.count("u1") != 1 {
		t.Fatalf("expected 1 in pending store, got %d", ps.count("u1"))
	}
	if !c.hasOverflow() {
		t.Fatal("expected overflow flag to be set")
	}
}

func TestHub_MaxConns(t *testing.T) {
	h := NewHub(WithHubMaxConns(1))
	c1 := testConn("u1")
	c1.hub = h
	_ = h.register(context.Background(), c1)

	c2 := testConn("u2")
	c2.hub = h
	err := h.register(context.Background(), c2)
	if err != ErrMaxConnsReached {
		t.Fatalf("expected ErrMaxConnsReached, got %v", err)
	}
}

// wsConn returns the server side of a live WebSocket connection. The client
// side keeps reading so close handshakes complete immediately.
func wsConn(t *testing.T) *websocket.Conn {
	t.Helper()
	accepted := make(chan *websocket.Conn, 1)
	hs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept: %v", err)
			return
		}
		accepted <- c
		<-r.Context().Done()
	}))
	t.Cleanup(hs.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(hs.URL, "http"), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = client.CloseNow() })
	go func() {
		for {
			if _, _, err := client.Read(context.Background()); err != nil {
				return
			}
		}
	}()
	select {
	case c := <-accepted:
		t.Cleanup(func() { _ = c.CloseNow() })
		return c
	case <-time.After(3 * time.Second):
		t.Fatal("server side not accepted")
		return nil
	}
}

// Registering a second conn with the same connID replaces the first: the old
// conn is closed as replaced, its subscriptions are dropped, and its own late
// unregister must not remove the new conn.
func TestHub_ConnectionReplacement(t *testing.T) {
	relay := &mockRelay{}
	h := NewHub(WithHubClusterRelay(relay), WithHubLogger(NopLogger{}))
	ctx := context.Background()

	c1 := testConn("u1")
	c1.ws = wsConn(t)
	c1.hub = h
	if err := h.register(ctx, c1); err != nil {
		t.Fatal(err)
	}
	h.Subscribe(ctx, c1, "room")
	c1.MarkDrained() // its writePump has nothing left to drain

	c2 := testConn("u1")
	c2.hub = h
	if err := h.register(ctx, c2); err != nil {
		t.Fatal(err)
	}

	if h.GetConn("u1") != c2 {
		t.Fatal("GetConn should return the new conn")
	}
	if h.ConnCount() != 1 {
		t.Fatalf("ConnCount = %d, want 1", h.ConnCount())
	}
	select {
	case <-c1.Done():
	default:
		t.Fatal("old conn should be finished")
	}
	if got := c1.CloseInfo().Reason; got != CloseReasonReplaced {
		t.Fatalf("old conn close reason = %q, want %q", got, CloseReasonReplaced)
	}
	if subs := h.TopicSubscribers("room"); len(subs) != 0 {
		t.Fatalf("old conn's subscriptions should be dropped, got %v", subs)
	}

	// The old conn's readPump runs unregister late; it must be a no-op.
	if h.unregister(c1) {
		t.Fatal("unregister of the replaced conn must not remove the new one")
	}
	if h.GetConn("u1") != c2 || h.ConnCount() != 1 {
		t.Fatal("new conn must survive the old conn's unregister")
	}
	if len(relay.registers) != 2 || len(relay.unregisters) != 0 {
		t.Fatalf("relay registers=%v unregisters=%v, want 2 and 0", relay.registers, relay.unregisters)
	}
}

// TestHub_TopicSubscribers_ReturnsConnIDs verifies the *Conn snapshot is
// correctly translated back to connIDs at the API boundary.
func TestHub_TopicSubscribers_ReturnsConnIDs(t *testing.T) {
	h := NewHub()
	ctx := context.Background()

	for _, id := range []string{"a", "b", "c"} {
		c := testConn(id)
		c.hub = h
		_ = h.register(ctx, c)
		h.Subscribe(ctx, c, "room")
	}

	subs := h.TopicSubscribers("room")
	if len(subs) != 3 {
		t.Fatalf("expected 3 subscribers, got %d", len(subs))
	}
	got := map[string]bool{}
	for _, id := range subs {
		got[id] = true
	}
	for _, want := range []string{"a", "b", "c"} {
		if !got[want] {
			t.Fatalf("missing %q in subscribers: %v", want, subs)
		}
	}
}

// TestHub_Subscribe_RequiresRegistered confirms the documented contract:
// Subscribe returns false (and is a no-op) if the conn is not registered.
func TestHub_Subscribe_RequiresRegistered(t *testing.T) {
	h := NewHub()
	ghost := testConn("ghost") // built but intentionally NOT registered with h
	if ok := h.Subscribe(context.Background(), ghost, "room"); ok {
		t.Fatal("Subscribe of unregistered conn must return false")
	}
	if subs := h.TopicSubscribers("room"); len(subs) != 0 {
		t.Fatalf("topic must be empty, got %v", subs)
	}
}

// TestHub_Concurrent_BroadcastSubscribeDisconnect stresses the COW + per-conn
// subs path under heavy concurrency. Validated under -race.
func TestHub_Concurrent_BroadcastSubscribeDisconnect(t *testing.T) {
	h := NewHub(WithHubLogger(NopLogger{}))
	ctx := context.Background()
	const numConns = 50
	const ops = 300

	// Pre-populate connections.
	conns := make([]*Conn, numConns)
	for i := 0; i < numConns; i++ {
		c := testConn("u" + string(rune('A'+i%26)) + string(rune('0'+i/26)))
		c.hub = h
		_ = h.register(ctx, c)
		conns[i] = c
		// Drain sendCh in background so writers do not block forever.
		go func(ch chan MessageEnvelope, done <-chan struct{}) {
			for {
				select {
				case <-done:
					return
				case <-ch:
				}
			}
		}(c.sendCh, c.done)
	}

	var wg sync.WaitGroup

	// Subscribers: churn membership of "room".
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(start int) {
			defer wg.Done()
			for n := 0; n < ops; n++ {
				idx := (start + n) % numConns
				h.Subscribe(ctx, conns[idx], "room")
				h.Unsubscribe(ctx, conns[idx], "room")
			}
		}(i * 10)
	}

	// Broadcasters: hammer the lock-free read path.
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < ops; n++ {
				h.Broadcast(ctx, "room", []byte("msg"))
			}
		}()
	}

	// Single-target sends.
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(start int) {
			defer wg.Done()
			for n := 0; n < ops; n++ {
				_ = h.Send(ctx, conns[(start+n)%numConns].id, []byte("hi"))
			}
		}(i * 5)
	}

	// Mass disconnects of a fraction of conns midway.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < numConns/3; i++ {
			h.unregister(conns[i])
		}
	}()

	wg.Wait()

	// Every conn was registered once and a third of them unregistered once.
	if want := numConns - numConns/3; h.ConnCount() != want {
		t.Fatalf("ConnCount = %d, want %d", h.ConnCount(), want)
	}
}

// ── Broadcast Shard Tests ────────────────────────────────────────────────────

func TestBroadcastShardSize_ZeroDisablesSharding(t *testing.T) {
	// broadcastShardSize=0 (default) — sequential path, all conns receive message.
	h := NewHub(WithHubLogger(NopLogger{}))
	ctx := context.Background()
	conns := make([]*Conn, 10)
	for i := range conns {
		c := testConn("u" + string(rune('A'+i)))
		c.hub = h
		_ = h.register(ctx, c)
		h.Subscribe(ctx, c, "room")
		conns[i] = c
	}

	h.Broadcast(ctx, "room", []byte("hello"))

	for _, c := range conns {
		select {
		case env := <-c.sendCh:
			if string(env.Data) != "hello" {
				t.Fatalf("expected hello, got %s", env.Data)
			}
		default:
			t.Fatalf("no message for %s", c.id)
		}
	}
}

func TestBroadcastShardSize_SmallListNoSharding(t *testing.T) {
	// shardSize=1000 but only 50 conns — should NOT shard (list < shardSize).
	h := NewHub(WithHubLogger(NopLogger{}), WithBroadcastShardSize(1000))
	ctx := context.Background()
	conns := make([]*Conn, 50)
	for i := range conns {
		c := testConn("u" + string(rune('A'+i%26)) + string(rune('0'+i/26)))
		c.hub = h
		_ = h.register(ctx, c)
		h.Subscribe(ctx, c, "room")
		conns[i] = c
	}

	h.Broadcast(ctx, "room", []byte("small"))

	for _, c := range conns {
		select {
		case env := <-c.sendCh:
			if string(env.Data) != "small" {
				t.Fatalf("expected small, got %s", env.Data)
			}
		default:
			t.Fatalf("no message for %s", c.id)
		}
	}
}

func TestBroadcastShardSize_LargeListParallel(t *testing.T) {
	// shardSize=100, 3000 conns → 30 shards in parallel.
	const numConns = 3000
	h := NewHub(WithHubLogger(NopLogger{}), WithBroadcastShardSize(100))
	ctx := context.Background()
	conns := make([]*Conn, numConns)
	for i := range conns {
		id := "u" + string(rune('A'+i%26)) + string(rune('0'+(i/26)%10)) + string(rune('a'+(i/260)%26))
		c := testConn(id)
		c.hub = h
		_ = h.register(ctx, c)
		h.Subscribe(ctx, c, "room")
		conns[i] = c
	}

	h.Broadcast(ctx, "room", []byte("parallel"))

	received := 0
	for _, c := range conns {
		select {
		case env := <-c.sendCh:
			if string(env.Data) != "parallel" {
				t.Fatalf("expected parallel, got %s", env.Data)
			}
			received++
		default:
			t.Fatalf("no message for %s", c.id)
		}
	}
	if received != numConns {
		t.Fatalf("expected %d messages, got %d", numConns, received)
	}
}

func TestBroadcastShardSize_WithExcludeSet(t *testing.T) {
	const numConns = 500
	h := NewHub(WithHubLogger(NopLogger{}), WithBroadcastShardSize(100))
	ctx := context.Background()
	conns := make([]*Conn, numConns)
	for i := range conns {
		id := "u" + string(rune('A'+i%26)) + string(rune('0'+(i/26)%10)) + string(rune('a'+(i/260)%26))
		c := testConn(id)
		c.hub = h
		_ = h.register(ctx, c)
		h.Subscribe(ctx, c, "room")
		conns[i] = c
	}

	// Exclude first 100 connections.
	excludeIDs := make([]string, 100)
	for i := 0; i < 100; i++ {
		excludeIDs[i] = conns[i].id
	}
	h.BroadcastExclude(ctx, "room", []byte("filtered"), excludeIDs...)

	// Excluded conns should NOT receive.
	for i := 0; i < 100; i++ {
		select {
		case <-conns[i].sendCh:
			t.Fatalf("excluded conn %s should not receive", conns[i].id)
		default:
			// OK
		}
	}
	// Remaining conns should receive.
	for i := 100; i < numConns; i++ {
		select {
		case env := <-conns[i].sendCh:
			if string(env.Data) != "filtered" {
				t.Fatalf("wrong data for %s", conns[i].id)
			}
		default:
			t.Fatalf("no message for %s", conns[i].id)
		}
	}
}

func TestBroadcastShardSize_WithOverflow(t *testing.T) {
	ps := newMemPendingStore()
	h := NewHub(WithHubLogger(NopLogger{}), WithHubPendingStore(ps), WithBroadcastShardSize(50))
	ctx := context.Background()

	const numConns = 200
	conns := make([]*Conn, numConns)
	for i := range conns {
		id := "u" + string(rune('A'+i%26)) + string(rune('0'+(i/26)%10))
		// sendCh buffer=1 to force overflow on second broadcast.
		c := &Conn{
			id:          id,
			sendCh:      make(chan MessageEnvelope, 1),
			done:        make(chan struct{}),
			drained:     make(chan struct{}),
			overflowSig: make(chan struct{}, 1),
		}
		c.hub = h
		_ = h.register(ctx, c)
		h.Subscribe(ctx, c, "room")
		conns[i] = c
	}

	// First broadcast fills sendCh (buffer=1).
	h.Broadcast(ctx, "room", []byte("first"))
	// Second broadcast — all sendCh full, should overflow to PendingStore.
	h.Broadcast(ctx, "room", []byte("second"))

	overflowed := 0
	for _, c := range conns {
		if ps.count(c.id) > 0 {
			overflowed++
		}
	}
	if overflowed != numConns {
		t.Fatalf("expected %d overflowed, got %d", numConns, overflowed)
	}
}

func TestBroadcastShardSize_ClosedConn(t *testing.T) {
	h := NewHub(WithHubLogger(NopLogger{}), WithBroadcastShardSize(50))
	ctx := context.Background()

	const numConns = 200
	conns := make([]*Conn, numConns)
	for i := range conns {
		id := "u" + string(rune('A'+i%26)) + string(rune('0'+(i/26)%10))
		c := testConn(id)
		c.hub = h
		_ = h.register(ctx, c)
		h.Subscribe(ctx, c, "room")
		conns[i] = c
	}

	// Close half the connections.
	for i := 0; i < numConns/2; i++ {
		conns[i].Finish()
	}

	// Should not panic.
	h.Broadcast(ctx, "room", []byte("mixed"))

	// Open conns should receive.
	received := 0
	for i := numConns / 2; i < numConns; i++ {
		select {
		case env := <-conns[i].sendCh:
			if string(env.Data) != "mixed" {
				t.Fatalf("wrong data")
			}
			received++
		default:
			t.Fatalf("no message for open conn %s", conns[i].id)
		}
	}
	if received != numConns/2 {
		t.Fatalf("expected %d received, got %d", numConns/2, received)
	}
}

// ── Benchmarks ───────────────────────────────────────────────────────────────

func BenchmarkHub_Send(b *testing.B) {
	h := NewHub(WithHubLogger(NopLogger{}))
	c := testConn("u1")
	c.hub = h
	_ = h.register(context.Background(), c)

	data := []byte(`{"type":"session_update","sessionId":"sess-001"}`)
	ctx := context.Background()

	// Drain sendCh in background.
	go func() {
		for range c.sendCh {
		}
	}()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = h.Send(ctx, "u1", data)
	}
}

func BenchmarkHub_Broadcast_10(b *testing.B) {
	benchmarkBroadcast(b, 10, 0)
}

func BenchmarkHub_Broadcast_100(b *testing.B) {
	benchmarkBroadcast(b, 100, 0)
}

func BenchmarkHub_Broadcast_1000(b *testing.B) {
	benchmarkBroadcast(b, 1000, 0)
}

func BenchmarkHub_Broadcast_1000_Sharded(b *testing.B) {
	benchmarkBroadcast(b, 1000, 200)
}

func BenchmarkHub_Broadcast_5000(b *testing.B) {
	benchmarkBroadcast(b, 5000, 0)
}

func BenchmarkHub_Broadcast_5000_Sharded(b *testing.B) {
	benchmarkBroadcast(b, 5000, 1000)
}

func BenchmarkHub_Broadcast_10000(b *testing.B) {
	benchmarkBroadcast(b, 10000, 0)
}

func BenchmarkHub_Broadcast_10000_Sharded(b *testing.B) {
	benchmarkBroadcast(b, 10000, 1000)
}

func benchmarkBroadcast(b *testing.B, n int, shardSize int) {
	opts := []HubOption{WithHubLogger(NopLogger{})}
	if shardSize > 0 {
		opts = append(opts, WithBroadcastShardSize(shardSize))
	}
	h := NewHub(opts...)
	ctx := context.Background()

	for i := 0; i < n; i++ {
		// Mixed-radix ID: unique for i < 26*10*26*26.
		id := "b" + string(rune('A'+i%26)) + string(rune('0'+(i/26)%10)) + string(rune('a'+(i/260)%26)) + string(rune('A'+(i/6760)%26))
		c := testConn(id)
		c.hub = h
		_ = h.register(ctx, c)
		h.Subscribe(ctx, c, "room")
		// Drain in background.
		go func(ch chan MessageEnvelope) {
			for range ch {
			}
		}(c.sendCh)
	}

	data := []byte(`{"type":"session_update"}`)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		h.Broadcast(ctx, "room", data)
	}
}

func BenchmarkHub_Subscribe(b *testing.B) {
	h := NewHub(WithHubLogger(NopLogger{}))
	ctx := context.Background()
	c := testConn("u1")
	c.hub = h
	_ = h.register(ctx, c)
	go func() {
		for range c.sendCh {
		}
	}()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		h.Subscribe(ctx, c, "topic")
		h.Unsubscribe(ctx, c, "topic")
	}
}
