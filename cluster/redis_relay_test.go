package cluster

import (
	"context"
	"encoding/base64"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/taixiaomin/ws-synapse/core"
)

// ── Mock RelayHandler ──────────────────────────────────────────────────────

type mockRelayHandler struct {
	mu         sync.Mutex
	sends      []sendCall
	broadcasts []broadcastCall
}

type sendCall struct {
	ConnID string
	Data   []byte
}

type broadcastCall struct {
	Topic      string
	Data       []byte
	ExcludeIDs []string
}

func (m *mockRelayHandler) LocalSend(_ context.Context, connID string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sends = append(m.sends, sendCall{ConnID: connID, Data: data})
	return nil
}

func (m *mockRelayHandler) LocalBroadcast(_ context.Context, topic string, data []byte, excludeIDs []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.broadcasts = append(m.broadcasts, broadcastCall{Topic: topic, Data: data, ExcludeIDs: excludeIDs})
}

func (m *mockRelayHandler) getSends() []sendCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]sendCall, len(m.sends))
	copy(cp, m.sends)
	return cp
}

func (m *mockRelayHandler) getBroadcasts() []broadcastCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]broadcastCall, len(m.broadcasts))
	copy(cp, m.broadcasts)
	return cp
}

var _ core.RelayHandler = (*mockRelayHandler)(nil)

// ── Mock PendingStore ──────────────────────────────────────────────────────

type mockPendingStore struct {
	mu    sync.Mutex
	items map[string][][]byte
}

func newMockPendingStore() *mockPendingStore {
	return &mockPendingStore{items: make(map[string][][]byte)}
}

func (s *mockPendingStore) Push(_ context.Context, connID string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items[connID] = append(s.items[connID], data)
	return nil
}

func (s *mockPendingStore) PushEnvelope(_ context.Context, connID string, msg core.PendingMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items[connID] = append(s.items[connID], msg.Data)
	return nil
}

func (s *mockPendingStore) PopAll(_ context.Context, connID string) ([]core.PendingMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	items := s.items[connID]
	delete(s.items, connID)
	var msgs []core.PendingMessage
	for _, d := range items {
		msgs = append(msgs, core.PendingMessage{Data: d})
	}
	return msgs, nil
}

func (s *mockPendingStore) getItems(connID string) [][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([][]byte, len(s.items[connID]))
	copy(cp, s.items[connID])
	return cp
}

var _ core.PendingStore = (*mockPendingStore)(nil)

// ── Helpers ────────────────────────────────────────────────────────────────

func setupMiniredis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return mr, client
}

func fastOpts() []Option {
	return []Option{
		WithHeartbeatInterval(100 * time.Millisecond),
		WithConnTTL(5 * time.Second),
		WithNodeTTL(3 * time.Second),
		WithBlockDuration(200 * time.Millisecond),
	}
}

// pollUntil retries fn every 20ms until it returns true or timeout elapses.
func pollUntil(t *testing.T, timeout time.Duration, fn func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

// ── Tests ──────────────────────────────────────────────────────────────────

func TestPublishSend_CrossNode(t *testing.T) {
	// Two relay instances sharing the same Redis. Node A registers a connID,
	// Node B calls PublishSend targeting that connID. We verify that Node B
	// writes the correct stream message, and Node A's consume loop dispatches
	// the LocalSend callback.
	_, client := setupMiniredis(t)

	pending := newMockPendingStore()
	handlerA := &mockRelayHandler{}
	handlerB := &mockRelayHandler{}

	relayA := NewRedisClusterRelay(client, pending, fastOpts()...)
	relayB := NewRedisClusterRelay(client, pending, fastOpts()...)

	if err := relayA.Start(handlerA); err != nil {
		t.Fatalf("relayA.Start: %v", err)
	}
	if err := relayB.Start(handlerB); err != nil {
		t.Fatalf("relayB.Start: %v", err)
	}

	ctx := context.Background()

	// Register connID on node A.
	if err := relayA.OnRegister(ctx, "conn-1"); err != nil {
		t.Fatalf("OnRegister: %v", err)
	}

	// Node B publishes a send to conn-1. This should XADD to node A's stream.
	payload := []byte("hello from B")
	if err := relayB.PublishSend(ctx, "conn-1", payload); err != nil {
		t.Fatalf("PublishSend: %v", err)
	}

	// Poll until handler A receives the LocalSend callback.
	ok := pollUntil(t, 6*time.Second, func() bool {
		return len(handlerA.getSends()) > 0
	})

	// Stop relays (must be done before test ends so goroutines finish).
	relayA.Stop()
	relayB.Stop()

	if !ok {
		t.Fatal("timed out waiting for LocalSend on node A")
	}

	sends := handlerA.getSends()
	if sends[0].ConnID != "conn-1" {
		t.Errorf("expected connID conn-1, got %s", sends[0].ConnID)
	}
	if string(sends[0].Data) != "hello from B" {
		t.Errorf("expected data 'hello from B', got %q", string(sends[0].Data))
	}
}

func TestPublishBroadcast_CrossNode(t *testing.T) {
	// Two relays, both subscribe to same topic. Node A calls PublishBroadcast,
	// Node B should receive LocalBroadcast via its consume loop.
	_, client := setupMiniredis(t)

	pending := newMockPendingStore()
	handlerA := &mockRelayHandler{}
	handlerB := &mockRelayHandler{}

	relayA := NewRedisClusterRelay(client, pending, fastOpts()...)
	relayB := NewRedisClusterRelay(client, pending, fastOpts()...)

	if err := relayA.Start(handlerA); err != nil {
		t.Fatalf("relayA.Start: %v", err)
	}
	if err := relayB.Start(handlerB); err != nil {
		t.Fatalf("relayB.Start: %v", err)
	}

	ctx := context.Background()

	// Both nodes subscribe to "chat:room1".
	if err := relayA.OnSubscribe(ctx, "conn-a", "chat:room1"); err != nil {
		t.Fatalf("relayA.OnSubscribe: %v", err)
	}
	if err := relayB.OnSubscribe(ctx, "conn-b", "chat:room1"); err != nil {
		t.Fatalf("relayB.OnSubscribe: %v", err)
	}

	// Node A broadcasts to chat:room1.
	payload := []byte("broadcast msg")
	if err := relayA.PublishBroadcast(ctx, "chat:room1", payload, nil); err != nil {
		t.Fatalf("PublishBroadcast: %v", err)
	}

	// Node B should receive LocalBroadcast (node A skips itself).
	ok := pollUntil(t, 6*time.Second, func() bool {
		return len(handlerB.getBroadcasts()) > 0
	})

	relayA.Stop()
	relayB.Stop()

	if !ok {
		t.Fatal("timed out waiting for LocalBroadcast on node B")
	}

	bcasts := handlerB.getBroadcasts()
	if bcasts[0].Topic != "chat:room1" {
		t.Errorf("expected topic chat:room1, got %s", bcasts[0].Topic)
	}
	if string(bcasts[0].Data) != "broadcast msg" {
		t.Errorf("expected data 'broadcast msg', got %q", string(bcasts[0].Data))
	}

	// Node A should NOT get a LocalBroadcast from its own publish.
	if len(handlerA.getBroadcasts()) != 0 {
		t.Errorf("node A should not receive its own broadcast, got %d", len(handlerA.getBroadcasts()))
	}
}

func TestPublishSend_UnknownConnID_FallsToPending(t *testing.T) {
	_, client := setupMiniredis(t)
	pending := newMockPendingStore()
	handler := &mockRelayHandler{}

	relay := NewRedisClusterRelay(client, pending, fastOpts()...)
	if err := relay.Start(handler); err != nil {
		t.Fatalf("Start: %v", err)
	}

	ctx := context.Background()
	payload := []byte("offline message")

	// PublishSend to a connID that was never registered.
	if err := relay.PublishSend(ctx, "unknown-conn", payload); err != nil {
		t.Fatalf("PublishSend: %v", err)
	}

	relay.Stop()

	items := pending.getItems("unknown-conn")
	if len(items) != 1 {
		t.Fatalf("expected 1 pending item, got %d", len(items))
	}
	if string(items[0]) != "offline message" {
		t.Errorf("expected 'offline message', got %q", string(items[0]))
	}

	// Handler should not have received anything.
	if len(handler.getSends()) != 0 {
		t.Errorf("handler should not have received sends, got %d", len(handler.getSends()))
	}
}

func TestPublishSend_DeadNode_FallsToPendingAndCleansRegistry(t *testing.T) {
	_, client := setupMiniredis(t)

	pending := newMockPendingStore()
	handler := &mockRelayHandler{}

	relay := NewRedisClusterRelay(client, pending, fastOpts()...)
	if err := relay.Start(handler); err != nil {
		t.Fatalf("Start: %v", err)
	}

	ctx := context.Background()

	// Simulate a dead node: manually write a conn registry pointing to a fake node.
	fakeNodeID := "dead-node-xyz"
	connKey := "ws:conn:conn-dead"
	if err := client.Set(ctx, connKey, fakeNodeID, 5*time.Second).Err(); err != nil {
		t.Fatalf("Set conn key: %v", err)
	}

	// Ensure the fake node's alive key does NOT exist (node is dead).
	aliveKey := "ws:node:alive:" + fakeNodeID
	exists, _ := client.Exists(ctx, aliveKey).Result()
	if exists != 0 {
		t.Fatal("expected dead node alive key to not exist")
	}

	payload := []byte("to dead node")
	if err := relay.PublishSend(ctx, "conn-dead", payload); err != nil {
		t.Fatalf("PublishSend: %v", err)
	}

	relay.Stop()

	// Should fall back to pending store.
	items := pending.getItems("conn-dead")
	if len(items) != 1 {
		t.Fatalf("expected 1 pending item, got %d", len(items))
	}
	if string(items[0]) != "to dead node" {
		t.Errorf("expected 'to dead node', got %q", string(items[0]))
	}

	// The stale conn registry key should have been cleaned up.
	val, err := client.Get(ctx, connKey).Result()
	if err != redis.Nil {
		t.Errorf("expected conn key to be deleted, got val=%q err=%v", val, err)
	}
}

func TestOnSubscribe_OnUnsubscribe_TopicSets(t *testing.T) {
	_, client := setupMiniredis(t)
	pending := newMockPendingStore()
	handler := &mockRelayHandler{}

	relay := NewRedisClusterRelay(client, pending, fastOpts()...)
	if err := relay.Start(handler); err != nil {
		t.Fatalf("Start: %v", err)
	}

	ctx := context.Background()
	topicKey := "ws:topic:events"

	// First subscriber on this node adds node to topic set.
	if err := relay.OnSubscribe(ctx, "conn-1", "events"); err != nil {
		t.Fatalf("OnSubscribe conn-1: %v", err)
	}
	members, err := client.SMembers(ctx, topicKey).Result()
	if err != nil {
		t.Fatalf("SMembers: %v", err)
	}
	if len(members) != 1 || members[0] != relay.nodeID {
		t.Errorf("expected topic set to contain nodeID, got %v", members)
	}

	// Second subscriber on the same node should NOT add a duplicate.
	if err := relay.OnSubscribe(ctx, "conn-2", "events"); err != nil {
		t.Fatalf("OnSubscribe conn-2: %v", err)
	}
	members, _ = client.SMembers(ctx, topicKey).Result()
	if len(members) != 1 {
		t.Errorf("expected 1 member in topic set, got %d", len(members))
	}

	// Unsubscribe first conn (not the last one) — node should remain in set.
	if err := relay.OnUnsubscribe(ctx, "conn-1", "events", false); err != nil {
		t.Fatalf("OnUnsubscribe conn-1: %v", err)
	}
	members, _ = client.SMembers(ctx, topicKey).Result()
	if len(members) != 1 {
		t.Errorf("expected 1 member after non-last unsub, got %d", len(members))
	}

	// Unsubscribe last conn — node should be removed from set.
	if err := relay.OnUnsubscribe(ctx, "conn-2", "events", true); err != nil {
		t.Fatalf("OnUnsubscribe conn-2: %v", err)
	}
	members, _ = client.SMembers(ctx, topicKey).Result()
	if len(members) != 0 {
		t.Errorf("expected empty topic set after last unsub, got %v", members)
	}

	relay.Stop()
}

func TestStop_CleansUpRedisKeys(t *testing.T) {
	_, client := setupMiniredis(t)
	pending := newMockPendingStore()
	handler := &mockRelayHandler{}

	relay := NewRedisClusterRelay(client, pending, fastOpts()...)
	if err := relay.Start(handler); err != nil {
		t.Fatalf("Start: %v", err)
	}

	ctx := context.Background()
	nodeID := relay.NodeID()

	// Register connections and subscribe to topics.
	if err := relay.OnRegister(ctx, "conn-x"); err != nil {
		t.Fatalf("OnRegister conn-x: %v", err)
	}
	if err := relay.OnRegister(ctx, "conn-y"); err != nil {
		t.Fatalf("OnRegister conn-y: %v", err)
	}
	if err := relay.OnSubscribe(ctx, "conn-x", "topic-1"); err != nil {
		t.Fatalf("OnSubscribe: %v", err)
	}

	// Verify keys exist before Stop.
	aliveKey := "ws:node:alive:" + nodeID
	exists, _ := client.Exists(ctx, aliveKey).Result()
	if exists == 0 {
		t.Fatal("expected alive key to exist before Stop")
	}

	connXKey := "ws:conn:conn-x"
	exists, _ = client.Exists(ctx, connXKey).Result()
	if exists == 0 {
		t.Fatal("expected conn-x key to exist before Stop")
	}

	// Stop the relay.
	if err := relay.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	// All keys should be cleaned up.
	exists, _ = client.Exists(ctx, aliveKey).Result()
	if exists != 0 {
		t.Error("alive key should be deleted after Stop")
	}

	streamKey := "ws:stream:" + nodeID
	exists, _ = client.Exists(ctx, streamKey).Result()
	if exists != 0 {
		t.Error("stream key should be deleted after Stop")
	}

	exists, _ = client.Exists(ctx, connXKey).Result()
	if exists != 0 {
		t.Error("conn-x key should be deleted after Stop")
	}

	connYKey := "ws:conn:conn-y"
	exists, _ = client.Exists(ctx, connYKey).Result()
	if exists != 0 {
		t.Error("conn-y key should be deleted after Stop")
	}

	topicKey := "ws:topic:topic-1"
	members, _ := client.SMembers(ctx, topicKey).Result()
	for _, m := range members {
		if m == nodeID {
			t.Error("nodeID should be removed from topic set after Stop")
		}
	}
}

func TestDispatch_Send_ViaDirectXADD(t *testing.T) {
	// Verifies that the consume loop picks up XADD messages and dispatches
	// them to the local handler. This tests the internal dispatch path directly
	// by writing to the stream, bypassing PublishSend.
	_, client := setupMiniredis(t)
	pending := newMockPendingStore()
	handler := &mockRelayHandler{}

	relay := NewRedisClusterRelay(client, pending, fastOpts()...)
	if err := relay.Start(handler); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Write a "send" message directly to relay's stream from a fake source node.
	payload := []byte("direct xadd msg")
	streamKey := relay.streamKey(relay.nodeID)
	err := client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: streamKey,
		Values: map[string]interface{}{
			"action":     "send",
			"connID":     "conn-direct",
			"data":       base64.StdEncoding.EncodeToString(payload),
			"sourceNode": "other-node",
		},
	}).Err()
	if err != nil {
		t.Fatalf("XADD: %v", err)
	}

	ok := pollUntil(t, 6*time.Second, func() bool {
		return len(handler.getSends()) > 0
	})

	relay.Stop()

	if !ok {
		t.Fatal("timed out waiting for dispatch of directly written stream message")
	}

	sends := handler.getSends()
	if sends[0].ConnID != "conn-direct" {
		t.Errorf("expected connID conn-direct, got %s", sends[0].ConnID)
	}
	if string(sends[0].Data) != "direct xadd msg" {
		t.Errorf("expected 'direct xadd msg', got %q", string(sends[0].Data))
	}
}

func TestDispatch_Broadcast_ViaDirectXADD(t *testing.T) {
	_, client := setupMiniredis(t)
	pending := newMockPendingStore()
	handler := &mockRelayHandler{}

	relay := NewRedisClusterRelay(client, pending, fastOpts()...)
	if err := relay.Start(handler); err != nil {
		t.Fatalf("Start: %v", err)
	}

	payload := []byte("broadcast via xadd")
	streamKey := relay.streamKey(relay.nodeID)
	err := client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: streamKey,
		Values: map[string]interface{}{
			"action":     "broadcast",
			"topic":      "room:42",
			"data":       base64.StdEncoding.EncodeToString(payload),
			"excludeIDs": `["conn-skip"]`,
			"sourceNode": "other-node",
		},
	}).Err()
	if err != nil {
		t.Fatalf("XADD: %v", err)
	}

	ok := pollUntil(t, 6*time.Second, func() bool {
		return len(handler.getBroadcasts()) > 0
	})

	relay.Stop()

	if !ok {
		t.Fatal("timed out waiting for dispatch of broadcast stream message")
	}

	bcasts := handler.getBroadcasts()
	if bcasts[0].Topic != "room:42" {
		t.Errorf("expected topic room:42, got %s", bcasts[0].Topic)
	}
	if string(bcasts[0].Data) != "broadcast via xadd" {
		t.Errorf("expected 'broadcast via xadd', got %q", string(bcasts[0].Data))
	}
	if len(bcasts[0].ExcludeIDs) != 1 || bcasts[0].ExcludeIDs[0] != "conn-skip" {
		t.Errorf("expected excludeIDs [conn-skip], got %v", bcasts[0].ExcludeIDs)
	}
}

func TestPublishSend_LocalNode_DeliversDirectly(t *testing.T) {
	// When conn is registered on the same node, PublishSend should call
	// LocalSend directly without going through the stream.
	_, client := setupMiniredis(t)
	pending := newMockPendingStore()
	handler := &mockRelayHandler{}

	relay := NewRedisClusterRelay(client, pending, fastOpts()...)
	if err := relay.Start(handler); err != nil {
		t.Fatalf("Start: %v", err)
	}

	ctx := context.Background()

	if err := relay.OnRegister(ctx, "local-conn"); err != nil {
		t.Fatalf("OnRegister: %v", err)
	}

	payload := []byte("local delivery")
	if err := relay.PublishSend(ctx, "local-conn", payload); err != nil {
		t.Fatalf("PublishSend: %v", err)
	}

	relay.Stop()

	// Should be delivered synchronously, no need to poll.
	sends := handler.getSends()
	if len(sends) != 1 {
		t.Fatalf("expected 1 send, got %d", len(sends))
	}
	if sends[0].ConnID != "local-conn" {
		t.Errorf("expected connID local-conn, got %s", sends[0].ConnID)
	}
	if string(sends[0].Data) != "local delivery" {
		t.Errorf("expected 'local delivery', got %q", string(sends[0].Data))
	}
}
