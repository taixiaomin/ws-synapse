package token

import (
	"context"
	"crypto/subtle"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/taixiaomin/ws-synapse/core"
)

const (
	defaultRedisKeyPrefix = "ws:token:"
	defaultRedisTTL       = 24 * time.Hour
)

// RedisOption configures a RedisTokenProvider.
type RedisOption func(*RedisTokenProvider)

// WithRedisKeyPrefix sets the Redis key prefix (default "ws:token:").
func WithRedisKeyPrefix(prefix string) RedisOption {
	return func(p *RedisTokenProvider) { p.keyPrefix = prefix }
}

// WithRedisTTL sets the token TTL (default 24h).
func WithRedisTTL(d time.Duration) RedisOption {
	return func(p *RedisTokenProvider) { p.ttl = d }
}

// RedisTokenProvider implements core.TokenProvider backed by Redis.
// Suitable for multi-node deployments where tokens must be shared.
type RedisTokenProvider struct {
	client    redis.Cmdable
	keyPrefix string
	ttl       time.Duration
}

// NewRedisTokenProvider creates a Redis-backed token provider.
func NewRedisTokenProvider(client redis.Cmdable, opts ...RedisOption) *RedisTokenProvider {
	p := &RedisTokenProvider{
		client:    client,
		keyPrefix: defaultRedisKeyPrefix,
		ttl:       defaultRedisTTL,
	}
	for _, fn := range opts {
		fn(p)
	}
	return p
}

// Generate creates a cryptographically random token and stores it in Redis with TTL.
func (p *RedisTokenProvider) Generate(ctx context.Context, connID string) (string, error) {
	tok := GenerateRandomToken()
	key := p.key(connID)
	if err := p.client.Set(ctx, key, tok, p.ttl).Err(); err != nil {
		return "", fmt.Errorf("redis token generate: %w", err)
	}
	return tok, nil
}

// Validate checks that the provided token matches the stored token.
// Uses constant-time comparison to prevent timing attacks.
func (p *RedisTokenProvider) Validate(ctx context.Context, connID, token string) (bool, error) {
	key := p.key(connID)
	stored, err := p.client.Get(ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return false, nil
		}
		return false, fmt.Errorf("redis token validate: %w", err)
	}
	return subtle.ConstantTimeCompare([]byte(stored), []byte(token)) == 1, nil
}

// Revoke removes the token for the given connection.
func (p *RedisTokenProvider) Revoke(ctx context.Context, connID string) error {
	key := p.key(connID)
	if err := p.client.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("redis token revoke: %w", err)
	}
	return nil
}

func (p *RedisTokenProvider) key(connID string) string {
	return p.keyPrefix + connID
}

var _ core.TokenProvider = (*RedisTokenProvider)(nil)
