// Package store provides built-in PendingStore implementations for ws-synapse.
//
//	import wsstore "github.com/taixiaomin/ws-synapse/store"
//
//	store := wsstore.NewRedisPendingStore(redisClient)
//	server := ws.NewServer(handler, ws.WithPendingStore(store))
package store

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/taixiaomin/ws-synapse/core"
)

// Default configuration values.
const (
	DefaultKeyPrefix   = "ws:pending:"
	DefaultMaxMessages = 500
	DefaultTTL         = 3 * time.Hour
)

// Wire-format prefix bytes for message type encoding.
// Each stored entry is: [1-byte type prefix][payload bytes]
const (
	prefixText   byte = 0x01
	prefixBinary byte = 0x02
)

// pushScript appends a message to the pending list, trims to the cap, and
// installs the TTL only when the key does not already have one.
//
// Why not plain Pipeline(RPUSH, LTRIM, EXPIRE): the old code re-applied
// EXPIRE on every push. For a connID that never came back, continued
// pushes from Hub.Send / broadcastDirect would silently renew the TTL
// forever — the key (and its memory) leak.
//
// With this script the TTL is set exactly once per key incarnation: at
// first push (or immediately after PopAll DELs the key). The key then
// expires s.ttl after its first incarnation, regardless of further pushes.
//
// Single-key script: Cluster-safe (all keys hash to the same slot).
var pushScript = redis.NewScript(`
redis.call('RPUSH', KEYS[1], ARGV[1])
redis.call('LTRIM', KEYS[1], -tonumber(ARGV[2]), -1)
if redis.call('TTL', KEYS[1]) == -1 then
	redis.call('EXPIRE', KEYS[1], ARGV[3])
end
return 1
`)

// ── Options ─────────────────────────────────────────────────────────────────

// Option configures a RedisPendingStore.
type Option func(*RedisPendingStore)

// WithKeyPrefix sets the Redis key prefix (default "ws:pending:").
func WithKeyPrefix(prefix string) Option {
	return func(s *RedisPendingStore) { s.keyPrefix = prefix }
}

// WithMaxMessages sets the maximum number of messages kept per connection
// (default 500). Oldest messages are trimmed on Push.
func WithMaxMessages(n int64) Option {
	return func(s *RedisPendingStore) { s.maxMessages = n }
}

// WithTTL sets the TTL for pending message keys (default 24h).
// Messages expire automatically if never consumed.
func WithTTL(d time.Duration) Option {
	return func(s *RedisPendingStore) { s.ttl = d }
}

// ── RedisPendingStore ───────────────────────────────────────────────────────

// RedisPendingStore implements core.PendingStore backed by Redis Lists.
// Each connection's pending messages are stored in a Redis List keyed by
// "{prefix}{connID}". Push appends via RPUSH + LTRIM + EXPIRE in a pipeline.
// PopAll atomically reads and deletes via LRANGE + DEL in a transaction.
//
// Wire format: each list entry is [1-byte type prefix][payload].
// This preserves the WebSocket message type (text/binary) across offline buffering.
type RedisPendingStore struct {
	client      redis.Cmdable
	keyPrefix   string
	maxMessages int64
	ttl         time.Duration
}

// NewRedisPendingStore creates a Redis-backed PendingStore.
// The client parameter accepts *redis.Client, *redis.ClusterClient, or any
// redis.Cmdable implementation.
func NewRedisPendingStore(client redis.Cmdable, opts ...Option) *RedisPendingStore {
	s := &RedisPendingStore{
		client:      client,
		keyPrefix:   DefaultKeyPrefix,
		maxMessages: DefaultMaxMessages,
		ttl:         DefaultTTL,
	}
	for _, fn := range opts {
		fn(s)
	}
	return s
}

// Push stores a text message for an offline connection (backward-compatible).
func (s *RedisPendingStore) Push(ctx context.Context, connID string, data []byte) error {
	return s.PushEnvelope(ctx, connID, core.PendingMessage{Data: data, MsgType: core.MsgTypeText})
}

// PushEnvelope stores a message with explicit type for an offline connection.
//
// TTL is installed only on first push to a fresh key (see pushScript). The key
// then expires s.ttl after its first incarnation; subsequent pushes don't
// renew it, so a connID that never reconnects can't leak memory indefinitely.
// The TTL is jittered ±10% per first incarnation to spread expiry across time.
func (s *RedisPendingStore) PushEnvelope(ctx context.Context, connID string, msg core.PendingMessage) error {
	key := s.key(connID)
	encoded := encode(msg)
	ttlSeconds := int(jitterTTL(s.ttl).Seconds())
	return pushScript.Run(ctx, s.client, []string{key}, encoded, s.maxMessages, ttlSeconds).Err()
}

// PopAll atomically retrieves and removes all pending messages for a connection.
// Uses a Redis transaction (MULTI/EXEC) with LRANGE + DEL.
func (s *RedisPendingStore) PopAll(ctx context.Context, connID string) ([]core.PendingMessage, error) {
	key := s.key(connID)

	var cmd *redis.StringSliceCmd
	_, err := s.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		cmd = pipe.LRange(ctx, key, 0, -1)
		pipe.Del(ctx, key)
		return nil
	})
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}

	vals, err := cmd.Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	if len(vals) == 0 {
		return nil, nil
	}

	out := make([]core.PendingMessage, len(vals))
	for i, v := range vals {
		out[i] = decode([]byte(v))
	}
	return out, nil
}

func (s *RedisPendingStore) key(connID string) string {
	return fmt.Sprintf("%s%s", s.keyPrefix, connID)
}

// encode prepends a 1-byte type prefix to the payload.
func encode(msg core.PendingMessage) []byte {
	prefix := prefixText
	if msg.MsgType == core.MsgTypeBinary {
		prefix = prefixBinary
	}
	buf := make([]byte, 1+len(msg.Data))
	buf[0] = prefix
	copy(buf[1:], msg.Data)
	return buf
}

// decode extracts message type from the 1-byte prefix.
// Gracefully handles legacy data (no prefix) by treating it as text.
func decode(raw []byte) core.PendingMessage {
	if len(raw) == 0 {
		return core.PendingMessage{MsgType: core.MsgTypeText}
	}
	switch raw[0] {
	case prefixText:
		return core.PendingMessage{Data: raw[1:], MsgType: core.MsgTypeText}
	case prefixBinary:
		return core.PendingMessage{Data: raw[1:], MsgType: core.MsgTypeBinary}
	default:
		// Legacy data without prefix — treat as text.
		return core.PendingMessage{Data: raw, MsgType: core.MsgTypeText}
	}
}

// jitterTTL returns d with ±10% random jitter to prevent cache stampede.
func jitterTTL(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	jitter := time.Duration(rand.Int63n(int64(d) / 5))
	return d - d/10 + jitter
}

// Compile-time check.
var _ core.PendingStore = (*RedisPendingStore)(nil)
