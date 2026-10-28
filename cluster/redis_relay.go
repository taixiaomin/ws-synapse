// Package cluster provides a Redis-backed ClusterRelay for multi-node ws-synapse deployments.
//
// It uses Redis Streams for reliable cross-node message delivery (not Pub/Sub),
// independent String keys with TTL for connection registry, and targeted broadcast
// that only fans out to nodes with active subscribers.
//
// Topic membership is a ZSET per topic (member = nodeID, score = the time of the
// node's last refresh). Liveness is decided per member by score, so dead nodes
// drop out without any keyspace SCAN. All scores are taken from the Redis
// server's clock inside Lua (redis.call('TIME')): writes, pruning and reads of a
// topic key happen on the shard that owns it, so application-node clock skew
// can never make one node evict another.
package cluster

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"math/rand"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/taixiaomin/ws-synapse/core"
)

// RedisClusterRelay implements core.ClusterRelay using Redis Streams.
type RedisClusterRelay struct {
	client redis.Cmdable
	nodeID string

	handler           core.RelayHandler
	pendingStore      core.PendingStore
	logger            core.Logger
	keyPrefix         string
	streamMaxLen      int64
	connTTL           time.Duration
	nodeTTL           time.Duration
	heartbeatInterval time.Duration
	blockDuration     time.Duration
	topicSharedTTL    time.Duration
	topicTTL          time.Duration
	streamTTL         time.Duration

	// localConns tracks connIDs registered on this node for TTL renewal.
	localConns sync.Map // connID → struct{}

	// localTopics tracks per-topic subscriber count on this node. Values are
	// int64 (immutable, so heartbeat can read them without a lock) and may be
	// temporarily negative; see adjustTopic. Only counts > 0 are members.
	localTopics sync.Map // topic → int64
	// topicStripes guard localTopics count changes and track which topics have
	// a membership write in flight; see adjustTopic. No Redis I/O happens
	// under a stripe lock.
	topicStripes [64]topicStripe

	// aliveCache remembers recent positive node-liveness checks for
	// PublishSend, saving an EXISTS round-trip per message.
	aliveCache sync.Map // nodeID → time.Time (expiry)

	// topicShared caches whether a topic has subscribers on any node other
	// than this one. PublishBroadcast uses it as a fast path to skip the
	// membership read + fan-out when the topic is known to be local-only.
	// Populated lazily by PublishBroadcast (slow path) and invalidated by
	// pub/sub events from OnSubscribe/OnUnsubscribe on other nodes.
	//
	//   value.shared=true  → topic has subscribers on ≥1 remote node
	//   value.shared=false → topic has subscribers ONLY on this node
	//   key absent  → unknown, slow-path will populate
	topicShared sync.Map // topic → topicSharedCacheEntry

	// membershipPubSub listens for topic-membership change broadcasts so
	// topicShared can be invalidated when remote nodes join/leave a topic.
	// nil when the underlying redis client does not support Subscribe — in
	// that case the fast path stays disabled (cache never populates).
	membershipPubSub *redis.PubSub

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// subscriber is the minimal interface we need to listen for membership
// changes. We pull it out of redis.Cmdable via a type assertion in Start
// so callers that pass a custom Cmdable (e.g. a pipelining wrapper) still
// work — they just lose the fast-path optimization.
type subscriber interface {
	Subscribe(ctx context.Context, channels ...string) *redis.PubSub
}

const (
	// heartbeatBatch caps how many script calls one heartbeat round-trip
	// carries, so a node with many connections never builds one huge pipeline.
	heartbeatBatch = 512
	// aliveCacheTTL is how long PublishSend trusts a positive liveness check.
	aliveCacheTTL = 3 * time.Second
)

type topicSharedCacheEntry struct {
	shared    bool
	expiresAt time.Time
}

// topicStripe is one shard of the per-topic bookkeeping lock. syncing holds
// the topics whose Redis membership write is in flight; at most one goroutine
// writes a given topic's membership at a time.
type topicStripe struct {
	mu      sync.Mutex
	syncing map[string]struct{}
}

// NewRedisClusterRelay creates a new relay. pendingStore is used as a fallback
// when the target node for a Send is unreachable.
func NewRedisClusterRelay(client redis.Cmdable, pendingStore core.PendingStore, opts ...Option) *RedisClusterRelay {
	r := &RedisClusterRelay{
		client:            client,
		nodeID:            uuid.New().String(),
		pendingStore:      pendingStore,
		keyPrefix:         defaultKeyPrefix,
		streamMaxLen:      defaultStreamMaxLen,
		connTTL:           defaultConnTTL,
		nodeTTL:           defaultNodeTTL,
		heartbeatInterval: defaultHeartbeatInterval,
		blockDuration:     defaultBlockDuration,
		topicSharedTTL:    defaultTopicSharedTTL,
		topicTTL:          defaultTopicTTL,
		streamTTL:         defaultStreamTTL,
		stopCh:            make(chan struct{}),
	}
	for _, fn := range opts {
		fn(r)
	}
	if r.logger == nil {
		r.logger = nopLogger{}
	}
	return r
}

// NodeID returns the unique identifier of this relay node.
func (r *RedisClusterRelay) NodeID() string { return r.nodeID }

// ── core.ClusterRelay implementation ────────────────────────────────────────

func (r *RedisClusterRelay) Start(handler core.RelayHandler) error {
	r.handler = handler
	ctx := context.Background()

	// Register node alive.
	if err := r.client.Set(ctx, r.nodeAliveKey(), "1", r.nodeTTL).Err(); err != nil {
		return fmt.Errorf("cluster relay start: %w", err)
	}

	// Create consumer group for our stream (ignore error if already exists).
	streamKey := r.streamKey(r.nodeID)
	err := r.client.XGroupCreateMkStream(ctx, streamKey, defaultGroupName, "0").Err()
	if err != nil && !isGroupExistsErr(err) {
		return fmt.Errorf("cluster relay create group: %w", err)
	}
	// Set initial TTL so the stream auto-expires if this node crashes
	// before the first heartbeat tick.
	if r.streamTTL > 0 {
		_ = r.client.Expire(ctx, streamKey, r.streamTTL).Err()
	}

	r.wg.Add(2)
	go r.heartbeatLoop()
	go r.consumeLoop()

	// Topic-share fast path: subscribe to membership-change events so the
	// PublishBroadcast cache can be invalidated when remote nodes touch a
	// topic. Degrade gracefully if the client doesn't expose Subscribe.
	if s, ok := r.client.(subscriber); ok {
		r.membershipPubSub = s.Subscribe(ctx, r.membershipChannel())
		// Block until the subscription is confirmed by Redis. This avoids a
		// race where the very first published event after Start could be
		// missed if we returned before Redis registered our subscription.
		if _, err := r.membershipPubSub.Receive(ctx); err != nil {
			r.logger.Warn("cluster relay: membership subscribe failed; fast-path disabled", "error", err)
			_ = r.membershipPubSub.Close()
			r.membershipPubSub = nil
		} else {
			r.wg.Add(1)
			go r.membershipChangeLoop()
		}
	} else {
		r.logger.Warn("cluster relay: client does not support Subscribe; topic-share fast-path disabled")
	}

	r.logger.Info("cluster relay started", "nodeID", r.nodeID)
	return nil
}

func (r *RedisClusterRelay) Stop() error {
	close(r.stopCh)
	if r.membershipPubSub != nil {
		// Closing the pub/sub causes Receive in the loop to return error,
		// which is the loop's exit signal — must close BEFORE wg.Wait().
		_ = r.membershipPubSub.Close()
	}
	r.wg.Wait()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Release connection routes we still own. Compare-and-delete: a route
	// another node has taken over in the meantime is left alone.
	var calls []*scriptCall
	r.localConns.Range(func(key, _ any) bool {
		calls = append(calls, &scriptCall{script: casDelScript,
			keys: []string{r.connKey(key.(string))}, args: []interface{}{r.nodeID}})
		return true
	})
	r.runScripts(ctx, calls)

	// Clean up node registrations and leave every topic we are a member of.
	pipe := r.client.Pipeline()
	pipe.Del(ctx, r.nodeAliveKey())
	pipe.Del(ctx, r.streamKey(r.nodeID))
	r.localTopics.Range(func(key, val any) bool {
		if val.(int64) > 0 {
			pipe.ZRem(ctx, r.topicKey(key.(string)), r.nodeID)
		}
		return true
	})

	_, err := pipe.Exec(ctx)
	if err != nil {
		r.logger.Warn("cluster relay stop: cleanup errors", "error", err)
	}
	r.logger.Info("cluster relay stopped", "nodeID", r.nodeID)
	return nil
}

func (r *RedisClusterRelay) OnRegister(ctx context.Context, connID string) error {
	r.localConns.Store(connID, struct{}{})
	return r.client.Set(ctx, r.connKey(connID), r.nodeID, r.connTTL).Err()
}

// OnUnregister removes the route only while it still points at this node, so
// a connID that has meanwhile registered on another node keeps its route.
func (r *RedisClusterRelay) OnUnregister(ctx context.Context, connID string) error {
	r.localConns.Delete(connID)
	return casDelScript.Run(ctx, r.client, []string{r.connKey(connID)}, r.nodeID).Err()
}

func (r *RedisClusterRelay) OnSubscribe(ctx context.Context, _, topic string) error {
	return r.adjustTopic(ctx, topic, +1)
}

// OnUnsubscribe ignores lastOnNode: the relay keeps its own count, which is
// the only source of truth consistent with the joins it has written.
func (r *RedisClusterRelay) OnUnsubscribe(ctx context.Context, _, topic string, _ bool) error {
	return r.adjustTopic(ctx, topic, -1)
}

// adjustTopic applies one subscribe (+1) or unsubscribe (-1) and writes to
// Redis only when the local count crosses zero.
//
// The Hub fires relay callbacks after releasing its own locks, so for one
// topic an unsubscribe can reach us before the subscribe it follows (and two
// connections' join/leave can interleave). Two rules make the result
// independent of that order:
//
//   - The count may go negative. An early -1 is recorded instead of dropped,
//     so the final count always equals the real number of local subscribers.
//     (The old code returned early on a missing entry, leaking a +1 and
//     keeping this node in the topic forever.)
//   - At most one goroutine writes a topic's membership at a time (the
//     stripe's syncing set), and it re-reads the count after each write until
//     Redis matches it. A leave's ZREM therefore can't land after a later
//     join's ZADD and drop this node from a topic it still serves.
//
// The stripe lock only covers the count change and the syncing check, never a
// Redis round-trip, so a slow Redis can't make subscribes on other topics in
// the same stripe queue behind this one. Only the caller that starts a write
// waits for it.
func (r *RedisClusterRelay) adjustTopic(ctx context.Context, topic string, delta int64) error {
	s := r.topicStripe(topic)
	s.mu.Lock()
	old := r.localCount(topic)
	cur := old + delta
	if cur == 0 {
		r.localTopics.Delete(topic)
	} else {
		r.localTopics.Store(topic, cur)
	}
	if (old > 0) == (cur > 0) {
		s.mu.Unlock()
		return nil // membership unchanged
	}
	if _, busy := s.syncing[topic]; busy {
		s.mu.Unlock()
		return nil // the in-flight writer converges to the new count
	}
	if s.syncing == nil {
		s.syncing = make(map[string]struct{})
	}
	s.syncing[topic] = struct{}{}
	s.mu.Unlock()

	return r.syncTopic(ctx, topic, cur > 0)
}

// syncTopic writes this node's membership of topic (join) or its removal
// (leave) without holding the stripe lock, then repeats while the local count
// has flipped in the meantime. The caller must have marked topic as syncing.
//
// On a failed write it stops instead of retrying against a failing Redis; the
// state self-heals: a missing join is re-added by the next heartbeat while the
// count is > 0, and a leftover member is no longer refreshed, so it drops out
// of reads after nodeTTL and is pruned by live members' heartbeats.
func (r *RedisClusterRelay) syncTopic(ctx context.Context, topic string, join bool) error {
	s := r.topicStripe(topic)
	key := r.topicKey(topic)
	joined := false
	var err error
	for {
		if join {
			err = topicTouchScript.Run(ctx, r.client, []string{key},
				r.nodeID, r.nodeTTL.Milliseconds(), r.topicTTLMillis()).Err()
			joined = joined || err == nil
		} else {
			// No "-" event: a peer caching shared=true for this topic takes
			// the slow path on every broadcast anyway, so invalidating it buys
			// nothing, while in Redis Cluster every PUBLISH is fanned out to
			// all shards.
			r.topicShared.Delete(topic)
			err = r.client.ZRem(ctx, key, r.nodeID).Err()
		}

		s.mu.Lock()
		want := r.localCount(topic) > 0
		if err != nil || want == join {
			delete(s.syncing, topic)
			s.mu.Unlock()
			break
		}
		s.mu.Unlock()
		join = want
	}

	if joined {
		// Tell other nodes their topicShared cache for this topic is stale —
		// we just joined as a (possibly new) remote node from their POV.
		// Best-effort: a dropped PUBLISH only delays correctness by the next
		// natural cache refresh, it doesn't corrupt anything.
		if pubErr := r.client.Publish(ctx, r.membershipChannel(), membershipMsgAdd+topic).Err(); pubErr != nil {
			r.logger.Warn("relay: membership publish (add) failed", "topic", topic, "error", pubErr)
		}
	}
	return err
}

func (r *RedisClusterRelay) topicStripe(topic string) *topicStripe {
	h := fnv.New32a()
	_, _ = h.Write([]byte(topic))
	return &r.topicStripes[h.Sum32()%uint32(len(r.topicStripes))]
}

// localCount returns this node's subscriber count for topic (may be negative).
func (r *RedisClusterRelay) localCount(topic string) int64 {
	if v, ok := r.localTopics.Load(topic); ok {
		return v.(int64)
	}
	return 0
}

func (r *RedisClusterRelay) topicTTLMillis() int64 {
	if r.topicTTL <= 0 {
		return 0
	}
	return jitterTTL(r.topicTTL).Milliseconds()
}

func (r *RedisClusterRelay) PublishSend(ctx context.Context, connID string, data []byte) error {
	// 1. Look up target node.
	targetNode, err := r.client.Get(ctx, r.connKey(connID)).Result()
	if err != nil || targetNode == "" {
		// Not registered (redis.Nil) or Redis error — buffer for later.
		return r.fallbackToPending(ctx, connID, data)
	}

	// 2. If target is ourselves, deliver locally.
	if targetNode == r.nodeID {
		return r.handler.LocalSend(ctx, connID, data)
	}

	// 3. Check target node alive.
	if !r.isNodeAlive(ctx, targetNode) {
		// Target node dead — drop the stale route, but only if it still names
		// the dead node: the conn may have re-registered elsewhere meanwhile.
		_ = casDelScript.Run(ctx, r.client, []string{r.connKey(connID)}, targetNode).Err()
		return r.fallbackToPending(ctx, connID, data)
	}

	// 4. Publish to target node's stream. NOMKSTREAM: if the node shut down
	// between the liveness check and here, its stream is gone; recreating it
	// would leave a key with no TTL that nothing ever consumes or deletes.
	envelope := map[string]interface{}{
		"action":     "send",
		"connID":     connID,
		"data":       base64.StdEncoding.EncodeToString(data),
		"sourceNode": r.nodeID,
	}
	err = r.client.XAdd(ctx, &redis.XAddArgs{
		Stream:     r.streamKey(targetNode),
		NoMkStream: true,
		MaxLen:     r.streamMaxLen,
		Approx:     true,
		Values:     envelope,
	}).Err()
	if err != nil {
		return r.fallbackToPending(ctx, connID, data)
	}
	return nil
}

// isNodeAlive checks the node's alive key, caching positive answers briefly.
// Negative answers are never cached, so a node that comes back is seen at once.
func (r *RedisClusterRelay) isNodeAlive(ctx context.Context, nodeID string) bool {
	if v, ok := r.aliveCache.Load(nodeID); ok && time.Now().Before(v.(time.Time)) {
		return true
	}
	n, err := r.client.Exists(ctx, r.nodeAliveKeyFor(nodeID)).Result()
	if err != nil || n == 0 {
		r.aliveCache.Delete(nodeID)
		return false
	}
	r.aliveCache.Store(nodeID, time.Now().Add(aliveCacheTTL))
	return true
}

func (r *RedisClusterRelay) PublishBroadcast(ctx context.Context, topic string, data []byte, excludeIDs []string) error {
	// Fast path: if a previous lookup told us this topic is local-only
	// AND no membership-change event has arrived since to invalidate the
	// cache, skip the Redis round-trip and remote fan-out entirely. The
	// local broadcast was already done by Hub.broadcastDirect before we
	// got here, so "skip" here means: do nothing.
	if v, ok := r.topicShared.Load(topic); ok {
		if entry, ok := v.(topicSharedCacheEntry); ok {
			if time.Now().Before(entry.expiresAt) {
				if !entry.shared {
					return nil
				}
			} else {
				r.topicShared.Delete(topic)
			}
		} else {
			r.topicShared.Delete(topic)
		}
	}

	// Slow path: ask Redis which nodes refreshed this topic within nodeTTL.
	// Dead nodes are filtered out by score here, so no per-node EXISTS check.
	nodeIDs, err := topicLiveScript.Run(ctx, r.client, []string{r.topicKey(topic)},
		r.nodeTTL.Milliseconds()).StringSlice()
	if err != nil {
		return err
	}
	if len(nodeIDs) == 0 {
		return nil
	}

	// Cache the result with TTL so the fast path self-heals even if an
	// invalidation event is missed.
	isShared := false
	for _, nid := range nodeIDs {
		if nid != r.nodeID {
			isShared = true
			break
		}
	}
	if r.topicSharedTTL > 0 {
		r.topicShared.Store(topic, topicSharedCacheEntry{
			shared:    isShared,
			expiresAt: time.Now().Add(r.topicSharedTTL),
		})
	}
	if !isShared {
		return nil
	}

	encoded := base64.StdEncoding.EncodeToString(data)
	var excludeJSON string
	if len(excludeIDs) > 0 {
		b, _ := json.Marshal(excludeIDs)
		excludeJSON = string(b)
	}
	envelope := map[string]interface{}{
		"action":     "broadcast",
		"topic":      topic,
		"data":       encoded,
		"excludeIDs": excludeJSON,
		"sourceNode": r.nodeID,
	}

	// Fan out to every remote node's stream in one round-trip.
	pipe := r.client.Pipeline()
	targets := make([]string, 0, len(nodeIDs))
	for _, nid := range nodeIDs {
		if nid == r.nodeID {
			continue // Already handled locally by Hub.
		}
		targets = append(targets, nid)
		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream:     r.streamKey(nid),
			NoMkStream: true, // see PublishSend
			MaxLen:     r.streamMaxLen,
			Approx:     true,
			Values:     envelope,
		})
	}
	cmds, _ := pipe.Exec(ctx)
	for i, cmd := range cmds {
		if err := cmd.Err(); err != nil {
			r.logger.Warn("broadcast: stream write failed", "targetNode", targets[i], "topic", topic, "error", err)
		}
	}
	return nil
}

// ── Internal loops ──────────────────────────────────────────────────────────

func (r *RedisClusterRelay) heartbeatLoop() {
	defer r.wg.Done()
	ticker := time.NewTicker(r.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.stopCh:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

			// Refresh node alive — use Set (not Expire) to recreate the key
			// if it was deleted by another node that mistakenly thought we were dead.
			_ = r.client.Set(ctx, r.nodeAliveKey(), "1", r.nodeTTL).Err()

			// Refresh stream TTL so orphan streams auto-expire after node crashes.
			if r.streamTTL > 0 {
				_ = r.client.Expire(ctx, r.streamKey(r.nodeID), r.streamTTL).Err()
			}

			r.refreshRegistrations(ctx)

			// Drop expired topicShared cache entries so the map doesn't grow
			// unboundedly for topics this node no longer broadcasts to (those
			// never trip the PublishBroadcast fast-path delete and never
			// receive an OnUnsubscribe).
			//
			// CompareAndDelete (not plain Delete) guards against a race where
			// PublishBroadcast.Store() lands between our Load and Delete:
			// without it we would evict the fresh entry that just landed.
			now := time.Now()
			r.topicShared.Range(func(k, v any) bool {
				entry, ok := v.(topicSharedCacheEntry)
				if !ok || now.After(entry.expiresAt) {
					r.topicShared.CompareAndDelete(k, v)
				}
				return true
			})
			r.aliveCache.Range(func(k, v any) bool {
				if now.After(v.(time.Time)) {
					r.aliveCache.CompareAndDelete(k, v)
				}
				return true
			})

			cancel()
		}
	}
}

func (r *RedisClusterRelay) consumeLoop() {
	defer r.wg.Done()

	streamKey := r.streamKey(r.nodeID)
	consumerName := r.nodeID

	for {
		select {
		case <-r.stopCh:
			return
		default:
		}

		ctx, cancel := context.WithTimeout(context.Background(), r.blockDuration+time.Second)
		streams, err := r.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    defaultGroupName,
			Consumer: consumerName,
			Streams:  []string{streamKey, ">"},
			Count:    100,
			Block:    r.blockDuration,
		}).Result()
		cancel()

		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue // No messages, normal.
			}
			// Check if we're shutting down.
			select {
			case <-r.stopCh:
				return
			default:
			}

			// Stream key expired (TTL) or was deleted — recreate it with a
			// fresh consumer group so consumption can resume.
			if isNoGroupErr(err) {
				r.logger.Info("consume: stream expired, recreating consumer group", "stream", streamKey)
				recreateCtx := context.Background()
				recreateErr := r.client.XGroupCreateMkStream(
					recreateCtx, streamKey, defaultGroupName, "0",
				).Err()
				if recreateErr != nil && !isGroupExistsErr(recreateErr) {
					r.logger.Warn("consume: recreate group failed", "error", recreateErr)
				}
				// Set TTL immediately so the stream auto-expires if this node
				// crashes before the next heartbeat tick.
				if r.streamTTL > 0 {
					_ = r.client.Expire(recreateCtx, streamKey, r.streamTTL).Err()
				}
				continue
			}

			r.logger.Warn("consume: xreadgroup error", "error", err)
			time.Sleep(time.Second) // Backoff on error.
			continue
		}

		for _, stream := range streams {
			ids := make([]string, 0, len(stream.Messages))
			for _, msg := range stream.Messages {
				ids = append(ids, msg.ID)
				r.dispatch(msg.Values)
			}
			// ACK all processed messages.
			if len(ids) > 0 {
				ackCtx, ackCancel := context.WithTimeout(context.Background(), 3*time.Second)
				_ = r.client.XAck(ackCtx, streamKey, defaultGroupName, ids...).Err()
				ackCancel()
			}
		}
	}
}

// refreshRegistrations renews this node's connection routes and topic
// memberships, in batches of heartbeatBatch script calls per round-trip.
//
// Connection routes: connRefreshScript renews a route only while it still
// names this node (or has vanished, e.g. after a Redis outage longer than
// connTTL). A route another node has taken over is left alone instead of being
// overwritten every heartbeat.
//
// Topic memberships: topicTouchScript re-scores this node and, in the same
// call, prunes members whose score is older than nodeTTL. Every topic with at
// least one live member is therefore cleaned by that member's heartbeat, and a
// topic whose members are all dead stops being refreshed and expires via
// topicTTL. This replaces the old SCAN-based stale-node sweep. Pruning and
// scoring both use the Redis server clock (see topicTouchScript), so a node
// with a skewed clock cannot prune live peers.
//
// A leave racing with this refresh can re-add this node after OnUnsubscribe
// removed it; the stray member is no longer refreshed, ages out of reads after
// nodeTTL and is pruned by the next live member's heartbeat.
//
// Topics are refreshed before connections: a membership is only valid for
// nodeTTL while a route lasts connTTL (longer), so if a slow Redis makes the
// heartbeat run out of time, the batches that are cut are the cheaper ones.
func (r *RedisClusterRelay) refreshRegistrations(ctx context.Context) {
	connTTL := r.connTTL.Milliseconds()
	nodeTTL := r.nodeTTL.Milliseconds()

	calls := make([]*scriptCall, 0, heartbeatBatch)
	lost := 0
	flush := func() {
		r.runScripts(ctx, calls)
		for _, c := range calls {
			if c.script == connRefreshScript {
				if n, err := c.cmd.Int(); err == nil && n == 0 {
					lost++
				}
			}
		}
		calls = calls[:0]
	}

	r.localTopics.Range(func(key, val any) bool {
		if val.(int64) <= 0 {
			return true
		}
		calls = append(calls, &scriptCall{script: topicTouchScript,
			keys: []string{r.topicKey(key.(string))}, args: []interface{}{r.nodeID, nodeTTL, r.topicTTLMillis()}})
		if len(calls) >= heartbeatBatch {
			flush()
		}
		return true
	})
	r.localConns.Range(func(key, _ any) bool {
		calls = append(calls, &scriptCall{script: connRefreshScript,
			keys: []string{r.connKey(key.(string))}, args: []interface{}{r.nodeID, connTTL}})
		if len(calls) >= heartbeatBatch {
			flush()
		}
		return true
	})
	flush()

	if lost > 0 {
		r.logger.Warn("heartbeat: connection routes owned by another node, not refreshed", "count", lost)
	}
}

// ── Lua scripts ─────────────────────────────────────────────────────────────
//
// Every script touches exactly one key, so all of them are Redis Cluster safe.

// casDelScript deletes KEYS[1] only if its value equals ARGV[1].
var casDelScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
	return redis.call('DEL', KEYS[1])
end
return 0
`)

// connRefreshScript renews a connection route if it still names this node
// (ARGV[1]) or has disappeared; returns 0, without writing, if another node
// owns it. ARGV[2] = TTL in ms.
var connRefreshScript = redis.NewScript(`
local cur = redis.call('GET', KEYS[1])
if cur == false or cur == ARGV[1] then
	redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
	return 1
end
return 0
`)

// topicTouchScript scores member ARGV[1] with the server's current time, drops
// members older than ARGV[2] ms and, when ARGV[3] > 0, sets a TTL of ARGV[3] ms.
var topicTouchScript = redis.NewScript(`
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
redis.call('ZADD', KEYS[1], now, ARGV[1])
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', '(' .. (now - tonumber(ARGV[2])))
if tonumber(ARGV[3]) > 0 then
	redis.call('PEXPIRE', KEYS[1], ARGV[3])
end
return 1
`)

// topicLiveScript returns the members refreshed within the last ARGV[1] ms,
// measured on the same server clock topicTouchScript writes with.
var topicLiveScript = redis.NewScript(`
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
return redis.call('ZRANGEBYSCORE', KEYS[1], now - tonumber(ARGV[1]), '+inf')
`)

// scriptCall is one queued script invocation; cmd holds its result after
// runScripts returns.
type scriptCall struct {
	script *redis.Script
	keys   []string
	args   []interface{}
	cmd    *redis.Cmd
}

// runScripts executes calls in a single pipeline using EVALSHA. If Redis has
// lost its script cache (restart, failover, SCRIPT FLUSH) the batch is re-run
// once with EVAL, which also reloads the scripts. All scripts used here are
// idempotent, so re-running calls that already succeeded is harmless.
func (r *RedisClusterRelay) runScripts(ctx context.Context, calls []*scriptCall) {
	if len(calls) == 0 {
		return
	}
	exec := func(sha bool) (noScript bool) {
		pipe := r.client.Pipeline()
		for _, c := range calls {
			if sha {
				c.cmd = c.script.EvalSha(ctx, pipe, c.keys, c.args...)
			} else {
				c.cmd = c.script.Eval(ctx, pipe, c.keys, c.args...)
			}
		}
		_, _ = pipe.Exec(ctx)
		for _, c := range calls {
			if err := c.cmd.Err(); err != nil && redis.HasErrorPrefix(err, "NOSCRIPT") {
				noScript = true
			}
		}
		return noScript
	}
	if exec(true) {
		exec(false)
	}
	for _, c := range calls {
		if err := c.cmd.Err(); err != nil && !errors.Is(err, redis.Nil) {
			r.logger.Warn("relay: script call failed", "key", c.keys[0], "error", err)
			break // one line per batch is enough; the rest usually share the cause
		}
	}
}

func (r *RedisClusterRelay) dispatch(fields map[string]interface{}) {
	sourceNode, _ := fields["sourceNode"].(string)
	if sourceNode == r.nodeID {
		return // Anti-loop.
	}

	action, _ := fields["action"].(string)
	dataStr, _ := fields["data"].(string)
	data, err := base64.StdEncoding.DecodeString(dataStr)
	if err != nil {
		r.logger.Warn("dispatch: invalid base64 data", "error", err)
		return
	}

	ctx := context.Background()

	switch action {
	case "send":
		connID, _ := fields["connID"].(string)
		if connID == "" {
			return
		}
		if err := r.handler.LocalSend(ctx, connID, data); err != nil {
			r.logger.Warn("dispatch: local send failed", "connID", connID, "error", err)
		}

	case "broadcast":
		topic, _ := fields["topic"].(string)
		if topic == "" {
			return
		}
		var excludeIDs []string
		if excludeStr, ok := fields["excludeIDs"].(string); ok && excludeStr != "" {
			_ = json.Unmarshal([]byte(excludeStr), &excludeIDs)
		}
		r.handler.LocalBroadcast(ctx, topic, data, excludeIDs)
	}
}

// ── Helpers ─────────────────────────────────────────────────────────────────

func (r *RedisClusterRelay) fallbackToPending(ctx context.Context, connID string, data []byte) error {
	if r.pendingStore != nil {
		return r.pendingStore.Push(ctx, connID, data)
	}
	r.logger.Warn("relay: message dropped (no pending store)", "connID", connID)
	return core.ErrMessageDropped
}

func (r *RedisClusterRelay) nodeAliveKey() string {
	return r.keyPrefix + "node:alive:" + r.nodeID
}

func (r *RedisClusterRelay) nodeAliveKeyFor(nodeID string) string {
	return r.keyPrefix + "node:alive:" + nodeID
}

func (r *RedisClusterRelay) streamKey(nodeID string) string {
	return r.keyPrefix + "stream:" + nodeID
}

func (r *RedisClusterRelay) connKey(connID string) string {
	return r.keyPrefix + "conn:" + connID
}

// topicKey names the topic's membership ZSET. The "topicz:" segment differs
// from the Set-based "topic:" keys of earlier versions so a leftover Set can
// never make ZADD fail with WRONGTYPE.
func (r *RedisClusterRelay) topicKey(topic string) string {
	return r.keyPrefix + "topicz:" + topic
}

// membershipChannel is the single global pub/sub channel used to propagate
// topic-membership changes between nodes. Payload format:
//
//	"+<topic>"  — sender just became the first local subscriber for <topic>
//	"-<topic>"  — sender just dropped the last local subscriber for <topic>
//	              (sent only by older versions; still accepted)
//
// Receivers invalidate their topicShared cache for <topic>. Using one
// channel for everything keeps the Redis subscriber count bounded at 1
// per node regardless of topic cardinality.
func (r *RedisClusterRelay) membershipChannel() string {
	return r.keyPrefix + "topic-membership-changes"
}

const (
	membershipMsgAdd    = "+"
	membershipMsgRemove = "-"
)

// membershipChangeLoop reads invalidation events from membershipPubSub and
// drops the corresponding topicShared cache entries. Both +/− payloads
// invalidate (we don't try to maintain the exact membership locally — that
// would require gossip; cheaper to re-fetch on next broadcast).
//
// Exits when membershipPubSub.Close() is called from Stop().
func (r *RedisClusterRelay) membershipChangeLoop() {
	defer r.wg.Done()
	ch := r.membershipPubSub.Channel()
	for {
		select {
		case <-r.stopCh:
			return
		case msg, ok := <-ch:
			if !ok {
				return
			}
			if len(msg.Payload) < 2 {
				continue
			}
			// Skip our own publishes — we already know the cache is fresh
			// (we just touched the set), so invalidating would just cause
			// the next broadcast to do a wasted membership read.
			//
			// However the protocol doesn't carry the sender, so we use a
			// conservative heuristic: only invalidate if the cached state
			// could plausibly be stale. Cheapest is just to invalidate
			// unconditionally and let the next slow path re-cache.
			r.topicShared.Delete(msg.Payload[1:])
		}
	}
}

// jitterTTL returns d with ±10% random jitter to prevent cache stampede.
func jitterTTL(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	// rand.Int63n produces [0, 20% of d), then shift to [-10%, +10%).
	jitter := time.Duration(rand.Int63n(int64(d) / 5))
	return d - d/10 + jitter
}

func isGroupExistsErr(err error) bool {
	return redis.HasErrorPrefix(err, "BUSYGROUP")
}

// isNoGroupErr detects "NOGROUP" Redis errors — the stream key or its
// consumer group no longer exists (typically because the key expired via TTL).
func isNoGroupErr(err error) bool {
	return redis.HasErrorPrefix(err, "NOGROUP")
}

// nopLogger is a no-op logger used when none is configured.
type nopLogger struct{}

func (nopLogger) Debug(string, ...any) {}
func (nopLogger) Info(string, ...any)  {}
func (nopLogger) Warn(string, ...any)  {}
func (nopLogger) Error(string, ...any) {}

var _ core.Logger = nopLogger{}

// Compile-time check.
var _ core.ClusterRelay = (*RedisClusterRelay)(nil)
