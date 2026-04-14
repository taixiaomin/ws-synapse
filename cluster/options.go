package cluster

import (
	"time"

	"github.com/taixiaomin/ws-synapse/core"
)

const (
	defaultHeartbeatInterval = 30 * time.Second
	defaultConnTTL           = 120 * time.Second
	defaultNodeTTL           = 60 * time.Second
	defaultStreamMaxLen      = 1000
	defaultKeyPrefix         = "ws:"
	defaultBlockDuration     = 5 * time.Second
	defaultGroupName         = "relay"
)

// Option configures a RedisClusterRelay.
type Option func(*RedisClusterRelay)

// WithHeartbeatInterval sets how often the node heartbeat and conn TTL are refreshed (default 30s).
func WithHeartbeatInterval(d time.Duration) Option {
	return func(r *RedisClusterRelay) { r.heartbeatInterval = d }
}

// WithConnTTL sets the TTL for connection registry keys (default 120s).
// Must be > HeartbeatInterval to avoid premature expiry.
func WithConnTTL(d time.Duration) Option {
	return func(r *RedisClusterRelay) { r.connTTL = d }
}

// WithNodeTTL sets the TTL for the node alive key (default 60s).
func WithNodeTTL(d time.Duration) Option {
	return func(r *RedisClusterRelay) { r.nodeTTL = d }
}

// WithStreamMaxLen sets the approximate max length of per-node streams (default 1000).
func WithStreamMaxLen(n int64) Option {
	return func(r *RedisClusterRelay) { r.streamMaxLen = n }
}

// WithBlockDuration sets the XREADGROUP block duration (default 5s).
// Lower values reduce Stop() latency but increase Redis polling frequency.
func WithBlockDuration(d time.Duration) Option {
	return func(r *RedisClusterRelay) { r.blockDuration = d }
}

// WithKeyPrefix sets the Redis key prefix (default "ws:").
func WithKeyPrefix(prefix string) Option {
	return func(r *RedisClusterRelay) { r.keyPrefix = prefix }
}

// WithLogger sets the logger for the relay. Compatible with core.Logger.
func WithLogger(l core.Logger) Option {
	return func(r *RedisClusterRelay) { r.logger = l }
}
