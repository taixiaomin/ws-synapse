package cluster

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Regression tests for the Redis consistency fixes: topic membership scored by
// the Redis server clock (no SCAN sweep), order-insensitive subscribe counts,
// compare-and-set connection routes, and NOMKSTREAM on stream writes.

func topicMembers(t *testing.T, client *redis.Client, topic string) []string {
	t.Helper()
	m, err := client.ZRange(context.Background(), "ws:topicz:"+topic, 0, -1).Result()
	if err != nil {
		t.Fatalf("ZRange: %v", err)
	}
	return m
}

// An unsubscribe that reaches the relay before its subscribe must cancel it
// out instead of being dropped (which used to leave the count at 1 and this
// node in the topic forever).
func TestAdjustTopic_UnsubscribeBeforeSubscribe(t *testing.T) {
	_, client := setupMiniredis(t)
	relay := NewRedisClusterRelay(client, nil, fastOpts()...)
	ctx := context.Background()

	if err := relay.OnUnsubscribe(ctx, "c1", "t", true); err != nil {
		t.Fatalf("OnUnsubscribe: %v", err)
	}
	if got := topicMembers(t, client, "t"); len(got) != 0 {
		t.Fatalf("early unsubscribe must not write; members = %v", got)
	}
	if err := relay.OnSubscribe(ctx, "c1", "t"); err != nil {
		t.Fatalf("OnSubscribe: %v", err)
	}
	if got := topicMembers(t, client, "t"); len(got) != 0 {
		t.Fatalf("-1 then +1 nets to zero subscribers; members = %v", got)
	}
	if _, ok := relay.localTopics.Load("t"); ok {
		t.Fatal("count 0 must not be kept in localTopics")
	}

	// A real subscribe afterwards joins normally.
	if err := relay.OnSubscribe(ctx, "c2", "t"); err != nil {
		t.Fatalf("OnSubscribe c2: %v", err)
	}
	if got := topicMembers(t, client, "t"); len(got) != 1 || got[0] != relay.NodeID() {
		t.Fatalf("members = %v, want [%s]", got, relay.NodeID())
	}
}

// A member whose score is older than nodeTTL is invisible to broadcasts (no
// stream write, and with NOMKSTREAM no stream is created for it) and is pruned
// by any live member's heartbeat — no keyspace SCAN involved.
func TestDeadTopicMember_FilteredAndPruned(t *testing.T) {
	_, client := setupMiniredis(t)
	ctx := context.Background()

	handlerA, handlerB := &mockRelayHandler{}, &mockRelayHandler{}
	relayA := NewRedisClusterRelay(client, nil, fastOpts()...)
	relayB := NewRedisClusterRelay(client, nil, fastOpts()...)
	if err := relayA.Start(handlerA); err != nil {
		t.Fatal(err)
	}
	defer relayA.Stop()
	if err := relayB.Start(handlerB); err != nil {
		t.Fatal(err)
	}
	defer relayB.Stop()

	// A crashed node that never left the topic.
	if err := client.ZAdd(ctx, "ws:topicz:room", redis.Z{Score: 1, Member: "dead-node"}).Err(); err != nil {
		t.Fatal(err)
	}
	if err := relayB.OnSubscribe(ctx, "cb", "room"); err != nil {
		t.Fatal(err)
	}

	if err := relayA.PublishBroadcast(ctx, "room", []byte("hi"), nil); err != nil {
		t.Fatalf("PublishBroadcast: %v", err)
	}
	if !pollUntil(t, 3*time.Second, func() bool { return len(handlerB.getBroadcasts()) == 1 }) {
		t.Fatal("live node B did not receive the broadcast")
	}
	if n, _ := client.Exists(ctx, "ws:stream:dead-node").Result(); n != 0 {
		t.Fatal("broadcast must not create a stream for a dead node")
	}

	// B's heartbeat prunes the dead member (topicTouchScript): already done by
	// OnSubscribe's touch, and kept that way by every heartbeat.
	if !pollUntil(t, 2*time.Second, func() bool {
		got := topicMembers(t, client, "room")
		return len(got) == 1 && got[0] == relayB.NodeID()
	}) {
		t.Fatalf("dead member not pruned; members = %v", topicMembers(t, client, "room"))
	}
}

// Scores come from the Redis server clock, so the result does not depend on
// the application nodes' clocks. With a server clock far from this process's
// wall clock, scoring by local time would mark every member dead (or keep dead
// ones alive); with TIME in Lua, membership still works.
func TestTopicMembership_UsesRedisServerClock(t *testing.T) {
	mr, client := setupMiniredis(t)
	mr.SetTime(time.Date(2001, 1, 1, 0, 0, 0, 0, time.UTC))
	ctx := context.Background()

	handlerA, handlerB := &mockRelayHandler{}, &mockRelayHandler{}
	relayA := NewRedisClusterRelay(client, nil, fastOpts()...)
	relayB := NewRedisClusterRelay(client, nil, fastOpts()...)
	if err := relayA.Start(handlerA); err != nil {
		t.Fatal(err)
	}
	defer relayA.Stop()
	if err := relayB.Start(handlerB); err != nil {
		t.Fatal(err)
	}
	defer relayB.Stop()

	if err := relayB.OnSubscribe(ctx, "cb", "room"); err != nil {
		t.Fatal(err)
	}
	if err := relayA.PublishBroadcast(ctx, "room", []byte("hi"), nil); err != nil {
		t.Fatal(err)
	}
	if !pollUntil(t, 3*time.Second, func() bool { return len(handlerB.getBroadcasts()) == 1 }) {
		t.Fatal("broadcast lost: membership must be judged on the Redis clock")
	}

	// Staleness is judged on the same clock: a member 10s older than the
	// server's "now" (nodeTTL is 3s) is dead, B is alive.
	stale := time.Date(2000, 12, 31, 23, 59, 50, 0, time.UTC).UnixMilli()
	client.ZAdd(ctx, "ws:topicz:room", redis.Z{Score: float64(stale), Member: "ghost"})
	live, err := topicLiveScript.Run(ctx, client, []string{"ws:topicz:room"}, relayA.nodeTTL.Milliseconds()).StringSlice()
	if err != nil {
		t.Fatal(err)
	}
	if len(live) != 1 || live[0] != relayB.NodeID() {
		t.Fatalf("live members = %v, want only [%s]", live, relayB.NodeID())
	}
}

// OnUnregister and Stop must not delete a route another node has taken over.
func TestConnRoute_CompareAndDelete(t *testing.T) {
	_, client := setupMiniredis(t)
	ctx := context.Background()
	relay := NewRedisClusterRelay(client, nil, fastOpts()...)
	if err := relay.Start(&mockRelayHandler{}); err != nil {
		t.Fatal(err)
	}

	if err := relay.OnRegister(ctx, "c1"); err != nil {
		t.Fatal(err)
	}
	if err := relay.OnRegister(ctx, "c2"); err != nil {
		t.Fatal(err)
	}
	// Both connIDs re-register on another node.
	client.Set(ctx, "ws:conn:c1", "other-node", time.Minute)
	client.Set(ctx, "ws:conn:c2", "other-node", time.Minute)

	if err := relay.OnUnregister(ctx, "c1"); err != nil {
		t.Fatal(err)
	}
	if v, _ := client.Get(ctx, "ws:conn:c1").Result(); v != "other-node" {
		t.Fatalf("OnUnregister deleted another node's route; got %q", v)
	}

	_ = relay.Stop()
	if v, _ := client.Get(ctx, "ws:conn:c2").Result(); v != "other-node" {
		t.Fatalf("Stop deleted another node's route; got %q", v)
	}
}

// The heartbeat renews routes it owns and recreates vanished ones, but never
// overwrites a route that now names another node.
func TestHeartbeat_DoesNotOverwriteForeignRoute(t *testing.T) {
	_, client := setupMiniredis(t)
	ctx := context.Background()
	relay := NewRedisClusterRelay(client, nil, fastOpts()...)

	relay.localConns.Store("mine", struct{}{})
	relay.localConns.Store("gone", struct{}{})
	relay.localConns.Store("moved", struct{}{})
	client.Set(ctx, "ws:conn:mine", relay.NodeID(), time.Second)
	client.Set(ctx, "ws:conn:moved", "other-node", time.Minute)

	relay.refreshRegistrations(ctx)

	if v, _ := client.Get(ctx, "ws:conn:mine").Result(); v != relay.NodeID() {
		t.Fatalf("own route not renewed: %q", v)
	}
	if ttl, _ := client.PTTL(ctx, "ws:conn:mine").Result(); ttl < 4*time.Second {
		t.Fatalf("own route TTL not extended to connTTL: %v", ttl)
	}
	if v, _ := client.Get(ctx, "ws:conn:gone").Result(); v != relay.NodeID() {
		t.Fatalf("vanished route not recreated: %q", v)
	}
	if v, _ := client.Get(ctx, "ws:conn:moved").Result(); v != "other-node" {
		t.Fatalf("heartbeat overwrote another node's route: %q", v)
	}
}

// Sending to a connID whose node is dead buffers the message, removes the
// stale route only if it still names the dead node, and creates no stream.
func TestPublishSend_DeadNode_NoStreamCreated(t *testing.T) {
	_, client := setupMiniredis(t)
	ctx := context.Background()
	pending := newMockPendingStore()
	relay := NewRedisClusterRelay(client, pending, fastOpts()...)
	if err := relay.Start(&mockRelayHandler{}); err != nil {
		t.Fatal(err)
	}
	defer relay.Stop()

	client.Set(ctx, "ws:conn:c1", "dead-node", time.Minute)
	if err := relay.PublishSend(ctx, "c1", []byte("x")); err != nil {
		t.Fatalf("PublishSend: %v", err)
	}
	if got := pending.getItems("c1"); len(got) != 1 {
		t.Fatalf("message not buffered: %v", got)
	}
	if n, _ := client.Exists(ctx, "ws:conn:c1").Result(); n != 0 {
		t.Fatal("stale route to dead node should be removed")
	}
	if n, _ := client.Exists(ctx, "ws:stream:dead-node").Result(); n != 0 {
		t.Fatal("PublishSend must not create a stream for a dead node")
	}
}

// A node that is alive but whose stream is gone (e.g. it is mid-shutdown) must
// not get its stream recreated without a TTL; the message falls back instead.
func TestPublishSend_MissingStream_NotRecreated(t *testing.T) {
	_, client := setupMiniredis(t)
	ctx := context.Background()
	pending := newMockPendingStore()
	relay := NewRedisClusterRelay(client, pending, fastOpts()...)
	if err := relay.Start(&mockRelayHandler{}); err != nil {
		t.Fatal(err)
	}
	defer relay.Stop()

	client.Set(ctx, "ws:node:alive:peer", "1", time.Minute)
	client.Set(ctx, "ws:conn:c1", "peer", time.Minute)
	if err := relay.PublishSend(ctx, "c1", []byte("x")); err != nil {
		t.Fatalf("PublishSend: %v", err)
	}
	if n, _ := client.Exists(ctx, "ws:stream:peer").Result(); n != 0 {
		t.Fatal("XADD must use NOMKSTREAM")
	}
	if got := pending.getItems("c1"); len(got) != 1 {
		t.Fatalf("message not buffered after stream write failed: %v", got)
	}
}

// gatedClient blocks the first EvalSha/Eval/ZRem on gateKey until release is
// closed, signalling entered when it starts waiting.
type gatedClient struct {
	redis.Cmdable
	gateKey string
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func newGatedClient(c redis.Cmdable, key string) *gatedClient {
	return &gatedClient{Cmdable: c, gateKey: key, entered: make(chan struct{}), release: make(chan struct{})}
}

func (g *gatedClient) wait(keys ...string) {
	if len(keys) == 0 || keys[0] != g.gateKey {
		return
	}
	g.once.Do(func() {
		close(g.entered)
		<-g.release
	})
}

func (g *gatedClient) EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd {
	g.wait(keys...)
	return g.Cmdable.EvalSha(ctx, sha1, keys, args...)
}

func (g *gatedClient) Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd {
	g.wait(keys...)
	return g.Cmdable.Eval(ctx, script, keys, args...)
}

func (g *gatedClient) ZRem(ctx context.Context, key string, members ...interface{}) *redis.IntCmd {
	g.wait(key)
	return g.Cmdable.ZRem(ctx, key, members...)
}

func waitDone(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("%s did not return", what)
	}
}

// A membership write that is stuck on Redis must not block subscribes to
// other topics that share its lock stripe.
func TestAdjustTopic_NoRedisIOUnderStripeLock(t *testing.T) {
	_, client := setupMiniredis(t)
	probe := NewRedisClusterRelay(client, nil)
	slow, other := "slow", ""
	for i := 0; other == ""; i++ {
		if tp := fmt.Sprintf("other-%d", i); probe.topicStripe(tp) == probe.topicStripe(slow) {
			other = tp
		}
	}
	gc := newGatedClient(client, "ws:topicz:"+slow)
	relay := NewRedisClusterRelay(gc, nil, fastOpts()...)
	ctx := context.Background()

	slowDone := make(chan struct{})
	go func() { _ = relay.OnSubscribe(ctx, "c1", slow); close(slowDone) }()
	<-gc.entered

	otherDone := make(chan struct{})
	go func() { _ = relay.OnSubscribe(ctx, "c2", other); close(otherDone) }()
	waitDone(t, otherDone, "subscribe on a sibling topic")

	close(gc.release)
	waitDone(t, slowDone, "gated subscribe")
	if got := topicMembers(t, client, slow); len(got) != 1 {
		t.Fatalf("%s members = %v", slow, got)
	}
	if got := topicMembers(t, client, other); len(got) != 1 {
		t.Fatalf("%s members = %v", other, got)
	}
}

// If the count drops back to zero while the join is being written, the
// in-flight writer must follow up with a leave.
func TestAdjustTopic_ConvergesAfterLeaveDuringJoin(t *testing.T) {
	_, client := setupMiniredis(t)
	gc := newGatedClient(client, "ws:topicz:t")
	relay := NewRedisClusterRelay(gc, nil, fastOpts()...)
	ctx := context.Background()

	joinDone := make(chan struct{})
	go func() { _ = relay.OnSubscribe(ctx, "c1", "t"); close(joinDone) }()
	<-gc.entered

	leaveDone := make(chan struct{})
	go func() { _ = relay.OnUnsubscribe(ctx, "c1", "t", true); close(leaveDone) }()
	waitDone(t, leaveDone, "unsubscribe while join in flight")

	close(gc.release)
	waitDone(t, joinDone, "gated subscribe")
	if got := topicMembers(t, client, "t"); len(got) != 0 {
		t.Fatalf("count is 0 but members = %v", got)
	}
	if _, busy := relay.topicStripe("t").syncing["t"]; busy {
		t.Fatal("syncing flag not cleared")
	}
}

// If a subscriber comes back while the leave is being written, the in-flight
// writer must re-join.
func TestAdjustTopic_ConvergesAfterJoinDuringLeave(t *testing.T) {
	_, client := setupMiniredis(t)
	relay := NewRedisClusterRelay(client, nil, fastOpts()...)
	ctx := context.Background()
	if err := relay.OnSubscribe(ctx, "c1", "t"); err != nil {
		t.Fatal(err)
	}
	// Gate only the leave from here on.
	gc := newGatedClient(client, "ws:topicz:t")
	relay.client = gc

	leaveDone := make(chan struct{})
	go func() { _ = relay.OnUnsubscribe(ctx, "c1", "t", true); close(leaveDone) }()
	<-gc.entered

	joinDone := make(chan struct{})
	go func() { _ = relay.OnSubscribe(ctx, "c2", "t"); close(joinDone) }()
	waitDone(t, joinDone, "subscribe while leave in flight")

	close(gc.release)
	waitDone(t, leaveDone, "gated unsubscribe")
	if got := topicMembers(t, client, "t"); len(got) != 1 || got[0] != relay.NodeID() {
		t.Fatalf("count is 1 but members = %v", got)
	}
}
