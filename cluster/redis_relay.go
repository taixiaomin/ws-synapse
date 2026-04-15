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
	"fmt"
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

	// localConns tracks connIDs registered on this node for TTL renewal.
	localConns sync.Map // connID → struct{}
	// localTopics tracks per-topic subscriber count on this node.
	localTopics sync.Map // topic → *int64 (atomic count via mutex)
	topicMu     sync.Mutex

	stopCh chan struct{}
	wg     sync.WaitGroup
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

	r.wg.Add(2)
	go r.heartbeatLoop()
	go r.consumeLoop()

	r.logger.Info("cluster relay started", "nodeID", r.nodeID)
	return nil
}

func (r *RedisClusterRelay) Stop() error {
	close(r.stopCh)
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
		return r.client.SAdd(ctx, r.topicKey(topic), r.nodeID).Err()
	}
	cnt := val.(*int64)
	*cnt++
	r.topicMu.Unlock()
	return nil
}

func (r *RedisClusterRelay) OnUnsubscribe(ctx context.Context, _, topic string, lastOnNode bool) error {
	if lastOnNode {
		r.localTopics.Delete(topic)
		return r.client.SRem(ctx, r.topicKey(topic), r.nodeID).Err()
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
	// Get all nodes that have subscribers for this topic.
	nodeIDs, err := r.client.SMembers(ctx, r.topicKey(topic)).Result()
	if err != nil || len(nodeIDs) == 0 {
		return err
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

			// Refresh connection registrations in a pipeline — use Set (not Expire)
			// to recreate keys that may have been cleaned up by other nodes during
			// a transient Redis outage longer than connTTL.
			pipe := r.client.Pipeline()
			r.localConns.Range(func(key, _ any) bool {
				connID := key.(string)
				pipe.Set(ctx, r.connKey(connID), r.nodeID, r.connTTL)
				return true
			})
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
			if err == redis.Nil {
				continue // No messages, normal.
			}
			// Check if we're shutting down.
			select {
			case <-r.stopCh:
				return
			default:
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

func isGroupExistsErr(err error) bool {
	return err != nil && err.Error() == "BUSYGROUP Consumer Group name already exists"
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
