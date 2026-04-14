package token

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"sync"

	"github.com/taixiaomin/ws-synapse/core"
)

// MemoryTokenProvider is a simple in-memory TokenProvider backed by sync.Map.
// Suitable for single-node deployments. For multi-node setups, implement
// a custom TokenProvider with an external store (e.g., Redis).
type MemoryTokenProvider struct {
	tokens sync.Map // connID → token string
}

// NewMemoryTokenProvider creates an in-memory token provider.
func NewMemoryTokenProvider() *MemoryTokenProvider {
	return &MemoryTokenProvider{}
}

// Generate creates a cryptographically random token and stores it for the given connID.
func (m *MemoryTokenProvider) Generate(_ context.Context, connID string) (string, error) {
	token := generateRandomToken()
	m.tokens.Store(connID, token)
	return token, nil
}

// Validate checks that the provided token matches the stored token for connID.
// Uses constant-time comparison to prevent timing attacks.
func (m *MemoryTokenProvider) Validate(_ context.Context, connID, token string) (bool, error) {
	stored, ok := m.tokens.Load(connID)
	if !ok {
		return false, nil
	}
	expected := stored.(string)
	return subtle.ConstantTimeCompare([]byte(expected), []byte(token)) == 1, nil
}

// Revoke removes the token for the given connID.
func (m *MemoryTokenProvider) Revoke(_ context.Context, connID string) error {
	m.tokens.Delete(connID)
	return nil
}

// generateRandomToken produces a cryptographically random 32-character hex string.
func generateRandomToken() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// GenerateRandomToken is exported for internal use
func GenerateRandomToken() string {
	return generateRandomToken()
}

// Ensure MemoryTokenProvider implements core.TokenProvider
var _ core.TokenProvider = (*MemoryTokenProvider)(nil)
