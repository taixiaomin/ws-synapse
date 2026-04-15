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

// Conn Core types
type Conn = core.Conn
type ConnInfo = core.ConnInfo
type Hub = core.Hub
type Message = core.Message
type MessageEnvelope = core.MessageEnvelope
type Server = core.Server
type ServerOption = core.ServerOption
type UpgradeInfo = core.UpgradeInfo
type PreparedMessage = core.PreparedMessage
type PendingMessage = core.PendingMessage

// EventHandler Interfaces
type EventHandler = core.EventHandler
type TopicEventHandler = core.TopicEventHandler
type MessageParser = core.MessageParser
type Logger = core.Logger
type MetricsCollector = core.MetricsCollector
type PendingStore = core.PendingStore
type TokenProvider = core.TokenProvider
type ClusterRelay = core.ClusterRelay
type RelayHandler = core.RelayHandler

// NopLogger Logger Implementations
type NopLogger = core.NopLogger

// MemoryTokenProvider Token Provider
type MemoryTokenProvider = token.MemoryTokenProvider

// ── Constants ──
const (
	MsgTypeText   = core.MsgTypeText
	MsgTypeBinary = core.MsgTypeBinary
)

// ── Errors ──
var (
	ErrSendChannelFull = core.ErrSendChannelFull
	ErrConnClosed      = core.ErrConnClosed
	ErrMaxConnsReached = core.ErrMaxConnsReached
	ErrMessageDropped  = core.ErrMessageDropped
	ErrHubShutdown     = core.ErrHubShutdown
	ErrRateLimited     = core.ErrRateLimited
)

// ── Constructors ──
var (
	NewHub                      = core.NewHub
	NewConn                     = core.NewConn
	NewServer                   = core.NewServer
	NewSlogAdapter              = core.NewSlogAdapter
	NewMemoryTokenProvider      = token.NewMemoryTokenProvider
	NewRedisTokenProvider       = token.NewRedisTokenProvider
	NewPreparedMessage          = core.NewPreparedMessage
	NewPreparedMessageFromBytes = core.NewPreparedMessageFromBytes
)

// ── Hub Options ──
var (
	WithHubLogger        = core.WithHubLogger
	WithHubMaxConns      = core.WithHubMaxConns
	WithHubMetrics       = core.WithHubMetrics
	WithHubPendingStore  = core.WithHubPendingStore
	WithHubTopicHandler  = core.WithHubTopicHandler
	WithHubTokenProvider = core.WithHubTokenProvider
	WithHubDrainTimeout  = core.WithHubDrainTimeout
	WithHubClusterRelay  = core.WithHubClusterRelay
)

// ── Server Options ──
var (
	WithTokenParam         = core.WithTokenParam
	WithSendChannelSize    = core.WithSendChannelSize
	WithWriteTimeout       = core.WithWriteTimeout
	WithPingInterval       = core.WithPingInterval
	WithPendingStore       = core.WithPendingStore
	WithInsecureSkipVerify = core.WithInsecureSkipVerify
	WithAllowedOrigins     = core.WithAllowedOrigins
	WithMaxMessageSize     = core.WithMaxMessageSize
	WithAuthenticator      = core.WithAuthenticator
	WithConnIDExtractor    = core.WithConnIDExtractor
	WithOnUpgrade          = core.WithOnUpgrade
	WithMaxConnections     = core.WithMaxConnections
	WithRateLimit          = core.WithRateLimit
	WithTokenProvider      = core.WithTokenProvider
	WithMessageParser      = core.WithMessageParser
	WithClusterRelay       = core.WithClusterRelay
	WithDrainTimeout       = core.WithDrainTimeout
	WithLogger             = core.WithLogger
	WithMetrics            = core.WithMetrics
	WithReconnectToken     = core.WithReconnectToken
)

// GenerateRandomToken ── Token ──
var (
	GenerateRandomToken = token.GenerateRandomToken
)
