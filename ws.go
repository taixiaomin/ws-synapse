// Package ws provides a production-ready WebSocket server library.
//
// It is designed as a standalone Go module that can be consumed by any project
// without pulling in framework-specific dependencies.
package ws

import (
	"github.com/taixiaomin/ws-synapse/core"
	"github.com/taixiaomin/ws-synapse/token"
)

// ── Re-export all public types and functions from core ──

// Core types
type Conn = core.Conn
type Hub = core.Hub
type Message = core.Message
type MessageEnvelope = core.MessageEnvelope
type Server = core.Server
type ServerOption = core.ServerOption

// Interfaces
type EventHandler = core.EventHandler
type TopicEventHandler = core.TopicEventHandler
type MessageParser = core.MessageParser
type Logger = core.Logger
type MetricsCollector = core.MetricsCollector
type PendingStore = core.PendingStore
type TokenProvider = core.TokenProvider

// Logger Implementations
type NopLogger = core.NopLogger

// Token Provider
type MemoryTokenProvider = token.MemoryTokenProvider

// ── Errors ──
var (
	ErrSendChannelFull   = core.ErrSendChannelFull
	ErrMaxConnsReached   = core.ErrMaxConnsReached
	ErrMessageDropped    = core.ErrMessageDropped
	ErrRateLimited       = core.ErrRateLimited
)

// ── Constructors ──
var (
	NewHub                = core.NewHub
	NewConn               = core.NewConn
	NewServer             = core.NewServer
	NewSlogAdapter        = core.NewSlogAdapter
	NewMemoryTokenProvider = token.NewMemoryTokenProvider
)

// ── Hub Options ──
var (
	WithHubLogger        = core.WithHubLogger
	WithHubMaxConns      = core.WithHubMaxConns
	WithHubMetrics       = core.WithHubMetrics
	WithHubPendingStore  = core.WithHubPendingStore
	WithHubTopicHandler  = core.WithHubTopicHandler
	WithHubTokenProvider = core.WithHubTokenProvider
)

// ── Server Options ──
var (
	WithTokenParam         = core.WithTokenParam
	WithSendChSize         = core.WithSendChSize
	WithWriteTimeout       = core.WithWriteTimeout
	WithPingInterval       = core.WithPingInterval
	WithPendingStore       = core.WithPendingStore
	WithInsecureSkipVerify = core.WithInsecureSkipVerify
	WithAllowedOrigins     = core.WithAllowedOrigins
	WithMaxMessageSize     = core.WithMaxMessageSize
	WithAuthenticator      = core.WithAuthenticator
	WithConnIDExtractor    = core.WithConnIDExtractor
	WithMaxConnections     = core.WithMaxConnections
	WithRateLimit          = core.WithRateLimit
	WithTokenProvider      = core.WithTokenProvider
	WithMessageParser      = core.WithMessageParser
	WithDrainTimeout       = core.WithDrainTimeout
	WithLogger             = core.WithLogger
	WithMetrics            = core.WithMetrics
	WithReconnectToken     = core.WithReconnectToken
)

// ── Token ──
var (
	GenerateRandomToken = token.GenerateRandomToken
)


