package token

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func setupRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	return mr, client
}

func TestRedisTokenProvider(t *testing.T) {
	ctx := context.Background()

	t.Run("Generate then Validate succeeds", func(t *testing.T) {
		_, client := setupRedis(t)
		p := NewRedisTokenProvider(client)

		tok, err := p.Generate(ctx, "conn1")
		if err != nil {
			t.Fatalf("Generate: %v", err)
		}

		ok, err := p.Validate(ctx, "conn1", tok)
		if err != nil {
			t.Fatalf("Validate: %v", err)
		}
		if !ok {
			t.Fatal("expected valid token")
		}
	})

	t.Run("Validate with wrong token fails", func(t *testing.T) {
		_, client := setupRedis(t)
		p := NewRedisTokenProvider(client)

		_, err := p.Generate(ctx, "conn1")
		if err != nil {
			t.Fatalf("Generate: %v", err)
		}

		ok, err := p.Validate(ctx, "conn1", "wrong-token")
		if err != nil {
			t.Fatalf("Validate: %v", err)
		}
		if ok {
			t.Fatal("expected invalid token")
		}
	})

	t.Run("Revoke then Validate fails", func(t *testing.T) {
		_, client := setupRedis(t)
		p := NewRedisTokenProvider(client)

		_, err := p.Generate(ctx, "conn1")
		if err != nil {
			t.Fatalf("Generate: %v", err)
		}

		if err := p.Revoke(ctx, "conn1"); err != nil {
			t.Fatalf("Revoke: %v", err)
		}

		ok, err := p.Validate(ctx, "conn1", "anything")
		if err != nil {
			t.Fatalf("Validate: %v", err)
		}
		if ok {
			t.Fatal("expected invalid after revoke")
		}
	})

	t.Run("Generate overwrites previous token", func(t *testing.T) {
		_, client := setupRedis(t)
		p := NewRedisTokenProvider(client)

		tok1, err := p.Generate(ctx, "conn1")
		if err != nil {
			t.Fatalf("Generate 1: %v", err)
		}

		tok2, err := p.Generate(ctx, "conn1")
		if err != nil {
			t.Fatalf("Generate 2: %v", err)
		}

		// Old token should be invalid.
		ok, _ := p.Validate(ctx, "conn1", tok1)
		if ok {
			t.Fatal("old token should be invalid after re-generate")
		}

		// New token should be valid.
		ok, _ = p.Validate(ctx, "conn1", tok2)
		if !ok {
			t.Fatal("new token should be valid")
		}
	})

	t.Run("TTL expiration invalidates token", func(t *testing.T) {
		mr, client := setupRedis(t)
		p := NewRedisTokenProvider(client, WithRedisTTL(10*time.Second))

		tok, err := p.Generate(ctx, "conn1")
		if err != nil {
			t.Fatalf("Generate: %v", err)
		}

		// Still valid before expiry.
		ok, _ := p.Validate(ctx, "conn1", tok)
		if !ok {
			t.Fatal("expected valid before TTL")
		}

		// Fast-forward past TTL.
		mr.FastForward(11 * time.Second)

		ok, err = p.Validate(ctx, "conn1", tok)
		if err != nil {
			t.Fatalf("Validate after TTL: %v", err)
		}
		if ok {
			t.Fatal("expected invalid after TTL expiry")
		}
	})

	t.Run("Custom key prefix", func(t *testing.T) {
		mr, client := setupRedis(t)
		p := NewRedisTokenProvider(client, WithRedisKeyPrefix("custom:prefix:"))

		_, err := p.Generate(ctx, "conn1")
		if err != nil {
			t.Fatalf("Generate: %v", err)
		}

		// Verify the key in Redis uses our custom prefix.
		if !mr.Exists("custom:prefix:conn1") {
			t.Fatal("expected key with custom prefix to exist in Redis")
		}
	})
}
