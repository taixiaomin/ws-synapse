// Package cluster provides a Redis-backed ClusterRelay for multi-node ws-synapse deployments.
//
// It uses Redis Streams for reliable cross-node message delivery (not Pub/Sub),
// independent String keys with TTL for connection registry, and targeted broadcast
// that only fans out to nodes with active subscribers.
package cluster

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
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
	// localTopics tracks per-topic subscriber count on this node.
	localTopics sync.Map // topic → *int64 (atomic count via mutex)
	topicMu     sync.Mutex

	// topicShared caches whether a topic has subscribers on any node other
	// than this one. PublishBroadcast uses it as a fast path to skip the
	// SMembers round-trip + fan-out when the topic is known to be local-only.
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

type topicSharedCacheEntry struct {
	shared    bool
	expiresAt time.Time
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

	// Clean up node registrations.
	pipe := r.client.Pipeline()
	pipe.Del(ctx, r.nodeAliveKey())
	pipe.Del(ctx, r.streamKey(r.nodeID))

	// Clean up connection registry entries for this node.
	r.localConns.Range(func(key, _ any) bool {
		connID := key.(string)
		pipe.Del(ctx, r.connKey(connID))
		return true
	})

	// Remove this node from all topic sets.
	r.localTopics.Range(func(key, _ any) bool {
		topic := key.(string)
		pipe.SRem(ctx, r.topicKey(topic), r.nodeID)
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

func (r *RedisClusterRelay) OnUnregister(ctx context.Context, connID string) error {
	r.localConns.Delete(connID)
	return r.client.Del(ctx, r.connKey(connID)).Err()
}

func (r *RedisClusterRelay) OnSubscribe(ctx context.Context, _, topic string) error {
	r.topicMu.Lock()
	val, loaded := r.localTopics.Load(topic)
	if !loaded {
		var count int64 = 1
		r.localTopics.Store(topic, &count)
		r.topicMu.Unlock()
		// First subscriber on this node — add node to topic set.
		if err := r.client.SAdd(ctx, r.topicKey(topic), r.nodeID).Err(); err != nil {
			return err
		}
		// Set/refresh TTL so orphan topic keys auto-expire after node crashes.
		if r.topicTTL > 0 {
			_ = r.client.Expire(ctx, r.topicKey(topic), jitterTTL(r.topicTTL)).Err()
		}
		// Tell other nodes their topicShared cache for this topic is stale —
		// we just joined as a (possibly new) remote node from their POV.
		// Best-effort: a dropped PUBLISH only delays correctness by the next
		// natural cache refresh, it doesn't corrupt anything.
		if err := r.client.Publish(ctx, r.membershipChannel(), membershipMsgAdd+topic).Err(); err != nil {
			r.logger.Warn("relay: membership publish (add) failed", "topic", topic, "error", err)
		}
		return nil
	}
	cnt := val.(*int64)
	*cnt++
	r.topicMu.Unlock()
	return nil
}

func (r *RedisClusterRelay) OnUnsubscribe(ctx context.Context, _, topic string, lastOnNode bool) error {
	if lastOnNode {
		r.localTopics.Delete(topic)
		// We left the topic — drop any local belief about it; downstream
		// PublishBroadcast on this node won't run for this topic anyway
		// (no local subscribers), but keep the map clean.
		r.topicShared.Delete(topic)
		if err := r.client.SRem(ctx, r.topicKey(topic), r.nodeID).Err(); err != nil {
			return err
		}
		if err := r.client.Publish(ctx, r.membershipChannel(), membershipMsgRemove+topic).Err(); err != nil {
			r.logger.Warn("relay: membership publish (remove) failed", "topic", topic, "error", err)
		}
		return nil
	}
	r.topicMu.Lock()
	if val, ok := r.localTopics.Load(topic); ok {
		cnt := val.(*int64)
		if *cnt > 0 {
			*cnt--
		}
	}
	r.topicMu.Unlock()
	return nil
}

func (r *RedisClusterRelay) PublishSend(ctx context.Context, connID string, data []byte) error {
	// 1. Look up target node.
	targetNode, err := r.client.Get(ctx, r.connKey(connID)).Result()
	if err == redis.Nil || targetNode == "" {
		// Connection not registered — buffer for later.
		return r.fallbackToPending(ctx, connID, data)
	}
	if err != nil {
		return r.fallbackToPending(ctx, connID, data)
	}

	// 2. If target is ourselves, deliver locally.
	if targetNode == r.nodeID {
		return r.handler.LocalSend(ctx, connID, data)
	}

	// 3. Check target node alive.
	alive, err := r.client.Exists(ctx, r.nodeAliveKeyFor(targetNode)).Result()
	if err != nil || alive == 0 {
		// Target node dead — clean up stale registry and buffer.
		_ = r.client.Del(ctx, r.connKey(connID)).Err()
		return r.fallbackToPending(ctx, connID, data)
	}

	// 4. Publish to target node's stream.
	envelope := map[string]interface{}{
		"action":     "send",
		"connID":     connID,
		"data":       base64.StdEncoding.EncodeToString(data),
		"sourceNode": r.nodeID,
	}
	err = r.client.XAdd(ctx, &redis.XAddArgs{
		Stream: r.streamKey(targetNode),
		MaxLen: r.streamMaxLen,
		Approx: true,
		Values: envelope,
	}).Err()
	if err != nil {
		return r.fallbackToPending(ctx, connID, data)
	}
	return nil
}

func (r *RedisClusterRelay) PublishBroadcast(ctx context.Context, topic string, data []byte, excludeIDs []string) error {
	// Fast path: if a previous SMembers told us this topic is local-only
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

	// Slow path: ask Redis who else subscribes to this topic, and cache
	// the result for subsequent broadcasts.
	nodeIDs, err := r.client.SMembers(ctx, r.topicKey(topic)).Result()
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

	// Fan out to each remote node via its stream.
	for _, nid := range nodeIDs {
		if nid == r.nodeID {
			continue // Already handled locally by Hub.
		}

		// Check if target node is alive before writing to its stream.
		// If dead, remove it from the topic set to prevent future wasted writes.
		alive, aliveErr := r.client.Exists(ctx, r.nodeAliveKeyFor(nid)).Result()
		if aliveErr != nil || alive == 0 {
			r.logger.Warn("broadcast: removing dead node from topic set",
				"deadNode", nid, "topic", topic)
			_ = r.client.SRem(ctx, r.topicKey(topic), nid).Err()
			continue
		}

		envelope := map[string]interface{}{
			"action":     "broadcast",
			"topic":      topic,
			"data":       encoded,
			"excludeIDs": excludeJSON,
			"sourceNode": r.nodeID,
		}
		if err := r.client.XAdd(ctx, &redis.XAddArgs{
			Stream: r.streamKey(nid),
			MaxLen: r.streamMaxLen,
			Approx: true,
			Values: envelope,
		}).Err(); err != nil {
			r.logger.Warn("broadcast: stream write failed", "targetNode", nid, "topic", topic, "error", err)
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
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

			// Refresh node alive — use Set (not Expire) to recreate the key
			// if it was deleted by another node that mistakenly thought we were dead.
			_ = r.client.Set(ctx, r.nodeAliveKey(), "1", r.nodeTTL).Err()

			// Refresh stream TTL so orphan streams auto-expire after node crashes.
			if r.streamTTL > 0 {
				_ = r.client.Expire(ctx, r.streamKey(r.nodeID), r.streamTTL).Err()
			}

			// Refresh connection registrations in a pipeline — use Set (not Expire)
			// to recreate keys that may have been cleaned up by other nodes during
			// a transient Redis outage longer than connTTL.
			pipe := r.client.Pipeline()
			r.localConns.Range(func(key, _ any) bool {
				connID := key.(string)
				pipe.Set(ctx, r.connKey(connID), r.nodeID, r.connTTL)
				return true
			})

			// Refresh topic TTL so orphan topic keys auto-expire after node crashes.
			if r.topicTTL > 0 {
				r.localTopics.Range(func(key, _ any) bool {
					topic := key.(string)
					pipe.Expire(ctx, r.topicKey(topic), jitterTTL(r.topicTTL))
					return true
				})
			}

			_, _ = pipe.Exec(ctx)

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

func (r *RedisClusterRelay) topicKey(topic string) string {
	return r.keyPrefix + "topic:" + topic
}

// membershipChannel is the single global pub/sub channel used to propagate
// topic-membership changes between nodes. Payload format:
//
//	"+<topic>"  — sender just became the first local subscriber for <topic>
//	"-<topic>"  — sender just dropped the last local subscriber for <topic>
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
			// the next broadcast to do a wasted SMembers.
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
	return err != nil && err.Error() == "BUSYGROUP Consumer Group name already exists"
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
