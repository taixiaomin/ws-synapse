package token

import (
	"context"
	"testing"
)

func TestMemoryTokenProvider(t *testing.T) {
	ctx := context.Background()

	t.Run("Generate then Validate succeeds", func(t *testing.T) {
		p := NewMemoryTokenProvider()
		tok, err := p.Generate(ctx, "conn1")
		if err != nil {
			t.Fatalf("Generate: %v", err)
		}
		if len(tok) != 32 {
			t.Fatalf("token length = %d, want 32 hex chars", len(tok))
		}
		if ok, _ := p.Validate(ctx, "conn1", tok); !ok {
			t.Fatal("expected valid token")
		}
	})

	t.Run("Validate is scoped to connID", func(t *testing.T) {
		p := NewMemoryTokenProvider()
		tok, _ := p.Generate(ctx, "conn1")
		if ok, _ := p.Validate(ctx, "conn2", tok); ok {
			t.Fatal("token of conn1 must not validate for conn2")
		}
		if ok, _ := p.Validate(ctx, "conn1", "wrong-token"); ok {
			t.Fatal("wrong token must not validate")
		}
	})

	t.Run("Revoke invalidates the token", func(t *testing.T) {
		p := NewMemoryTokenProvider()
		tok, _ := p.Generate(ctx, "conn1")
		if err := p.Revoke(ctx, "conn1"); err != nil {
			t.Fatalf("Revoke: %v", err)
		}
		if ok, _ := p.Validate(ctx, "conn1", tok); ok {
			t.Fatal("expected invalid after revoke")
		}
	})

	t.Run("Generate overwrites previous token", func(t *testing.T) {
		p := NewMemoryTokenProvider()
		tok1, _ := p.Generate(ctx, "conn1")
		tok2, _ := p.Generate(ctx, "conn1")
		if tok1 == tok2 {
			t.Fatal("tokens must be random")
		}
		if ok, _ := p.Validate(ctx, "conn1", tok1); ok {
			t.Fatal("old token should be invalid after re-generate")
		}
		if ok, _ := p.Validate(ctx, "conn1", tok2); !ok {
			t.Fatal("new token should be valid")
		}
	})
}
