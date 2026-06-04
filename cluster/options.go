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
	defaultTopicSharedTTL    = 5 * time.Second
	defaultStaleNodeSweepInterval = 5 * time.Minute
	defaultGroupName         = "relay"
	defaultTopicTTL          = 3 * time.Hour
	defaultStreamTTL         = 5 * time.Minute
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

// WithTopicSharedTTL sets how long PublishBroadcast may trust a cached
// local-only/shared topic decision before refreshing via SMembers.
func WithTopicSharedTTL(d time.Duration) Option {
	return func(r *RedisClusterRelay) { r.topicSharedTTL = d }
}

// WithTopicTTL sets the TTL for topic membership SET keys (default 3h).
// Topic keys are refreshed every heartbeat interval; the TTL acts as a
// safety net so orphan keys are cleaned up after node crashes.
func WithTopicTTL(d time.Duration) Option {
	return func(r *RedisClusterRelay) { r.topicTTL = d }
}

// WithStreamTTL sets the TTL for per-node stream keys (default 5min).
// Stream keys are refreshed every heartbeat interval; the TTL ensures
// orphan streams from crashed nodes are automatically reclaimed.
func WithStreamTTL(d time.Duration) Option {
	return func(r *RedisClusterRelay) { r.streamTTL = d }
}

// WithLogger sets the logger for the relay. Compatible with core.Logger.
func WithLogger(l core.Logger) Option {
	return func(r *RedisClusterRelay) { r.logger = l }
}

// WithStaleNodeSweepInterval sets how often the relay scans all topic sets
// to remove nodeIDs whose nodeAlive key has expired (default 5min). A node
// that crashes without running Stop() leaves orphan nodeIDs in topic sets;
// PublishBroadcast lazy-cleans them only for topics it broadcasts to, so a
// background sweep is needed for topics that are no longer broadcast.
// Pass 0 or a negative duration to disable the sweep entirely.
func WithStaleNodeSweepInterval(d time.Duration) Option {
	return func(r *RedisClusterRelay) { r.staleNodeSweepInterval = d }
}