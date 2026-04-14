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
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/taixiaomin/ws-synapse/core"
)

// Default configuration values.
const (
	DefaultKeyPrefix   = "ws:pending:"
	DefaultMaxMessages = 500
	DefaultTTL         = 24 * time.Hour
)

// Wire-format prefix bytes for message type encoding.
// Each stored entry is: [1-byte type prefix][payload bytes]
const (
	prefixText   byte = 0x01
	prefixBinary byte = 0x02
)

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
func (s *RedisPendingStore) PushEnvelope(ctx context.Context, connID string, msg core.PendingMessage) error {
	key := s.key(connID)
	encoded := encode(msg)
	pipe := s.client.Pipeline()
	pipe.RPush(ctx, key, encoded)
	pipe.LTrim(ctx, key, -s.maxMessages, -1)
	pipe.Expire(ctx, key, s.ttl)
	_, err := pipe.Exec(ctx)
	return err
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

// Compile-time check.
var _ core.PendingStore = (*RedisPendingStore)(nil)
