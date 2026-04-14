package core

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// ErrRateLimited is returned when an inbound message is rejected by the rate limiter.
var ErrRateLimited = errors.New("ws: rate limited")

// jsonError writes a JSON error response with the correct Content-Type.
func jsonError(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	b, _ := json.Marshal(msg)
	_, _ = fmt.Fprintf(w, `{"error":%s}`, b)
}

// Server is the WebSocket entry point. It upgrades HTTP connections,
// manages read/write pumps, and dispatches lifecycle events to the EventHandler.
type Server struct {
	hub     *Hub
	handler EventHandler
	opts    ServerOptions
	metrics MetricsCollector
	parser  MessageParser
	wg      sync.WaitGroup // tracks active read/write pump goroutines
}

// UpgradeInfo is the return value of the OnUpgrade hook.
type UpgradeInfo struct {
	ConnID   string                 // required: connection identifier
	Metadata map[string]interface{} // optional: auto-injected into Conn.meta via Set()
}

// ServerOptions holds configuration for a Server.
type ServerOptions struct {
	// query param for reconnect token, default "token"
	tokenParam string

	// outbound channel buffer, default 256
	sendChannelSize int

	// per-write timeout, default 10s
	writeTimeout time.Duration

	// heartbeat ping interval, default 54s
	pingInterval time.Duration

	// offline message store, nil = disabled
	pendingStore PendingStore

	// skip origin check, default false (secure by default)
	insecureSkipVerify bool

	// allowed origin patterns (used when insecureSkipVerify is false)
	allowedOrigins []string

	// max inbound message size in bytes, default 32KB
	maxMessageSize int64

	// authenticator is called before WebSocket upgrade. Return non-nil error to reject.
	authenticator func(r *http.Request) error

	// connIDExtractor extracts the connection ID from an HTTP request.
	connIDExtractor func(r *http.Request) (string, error)

	// onUpgrade is a unified upgrade hook that replaces authenticator + connIDExtractor.
	// When set, authenticator and connIDExtractor are ignored by HandleHTTP.
	// Return error to reject the connection (HTTP 401).
	onUpgrade func(r *http.Request) (*UpgradeInfo, error)

	// max concurrent connections, 0 = unlimited (default 0)
	maxConnections int64

	// inbound message rate limit per connection (messages/sec), 0 = unlimited
	rateLimitPerSec float64

	// rate limit burst size (default 10)
	rateLimitBurst int

	// token provider for reconnect token management; nil = no token validation
	tokenProvider TokenProvider

	// messageParser extracts the "type" from inbound messages.
	messageParser MessageParser

	// cluster relay for multi-node message routing; nil = single-node mode
	clusterRelay ClusterRelay

	// drain timeout for pending messages on disconnect (default 5s)
	drainTimeout time.Duration

	// logger, default slog-based logger
	logger Logger

	// metrics collector, nil = no-op
	metrics MetricsCollector
}

// ServerOption configures the Server.
type ServerOption func(*ServerOptions)

// defaultServerOptions returns default options for a Server.
func defaultServerOptions() ServerOptions {
	return ServerOptions{
		tokenParam:         "token",
		sendChannelSize:    256,
		writeTimeout:       10 * time.Second,
		pingInterval:       54 * time.Second,
		insecureSkipVerify: false,
		maxMessageSize:     32 * 1024, // 32KB
		rateLimitBurst:     10,
		drainTimeout:       5 * time.Second,
		logger:             defaultLogger(),
	}
}

// validate clamps or fixes invalid option values to prevent panics at runtime.
func (o *ServerOptions) validate() {
	if o.sendChannelSize < 1 {
		o.sendChannelSize = 1
	}
	if o.writeTimeout <= 0 {
		o.writeTimeout = 10 * time.Second
	}
	if o.pingInterval <= 0 {
		o.pingInterval = 54 * time.Second
	}
	if o.maxMessageSize <= 0 {
		o.maxMessageSize = 32 * 1024
	}
	if o.rateLimitBurst < 1 && o.rateLimitPerSec > 0 {
		o.rateLimitBurst = 1
	}
	if o.drainTimeout <= 0 {
		o.drainTimeout = 5 * time.Second
	}
	if o.logger == nil {
		o.logger = defaultLogger()
	}
}

// WithTokenParam sets the query parameter name for reconnect token (default "token").
func WithTokenParam(param string) ServerOption {
	return func(o *ServerOptions) { o.tokenParam = param }
}

// WithSendChSize sets the outbound message channel buffer size (default 256).
func WithSendChSize(size int) ServerOption {
	return func(o *ServerOptions) { o.sendChannelSize = size }
}

// WithWriteTimeout sets the per-write timeout (default 10s).
func WithWriteTimeout(d time.Duration) ServerOption {
	return func(o *ServerOptions) { o.writeTimeout = d }
}

// WithPingInterval sets the heartbeat ping interval (default 54s).
func WithPingInterval(d time.Duration) ServerOption {
	return func(o *ServerOptions) { o.pingInterval = d }
}

// WithPendingStore enables offline message buffering with the given store.
func WithPendingStore(store PendingStore) ServerOption {
	return func(o *ServerOptions) { o.pendingStore = store }
}

// WithInsecureSkipVerify controls WebSocket origin checking (default false).
func WithInsecureSkipVerify(skip bool) ServerOption {
	return func(o *ServerOptions) { o.insecureSkipVerify = skip }
}

// WithAllowedOrigins sets the allowed origin patterns for WebSocket connections.
func WithAllowedOrigins(origins ...string) ServerOption {
	return func(o *ServerOptions) { o.allowedOrigins = origins }
}

// WithMaxMessageSize sets the maximum inbound message size in bytes (default 32KB).
func WithMaxMessageSize(size int64) ServerOption {
	return func(o *ServerOptions) { o.maxMessageSize = size }
}

// WithAuthenticator sets a function to authenticate the HTTP request before WebSocket upgrade.
func WithAuthenticator(fn func(r *http.Request) error) ServerOption {
	return func(o *ServerOptions) { o.authenticator = fn }
}

// WithConnIDExtractor sets a function to extract the connection ID from an HTTP request.
func WithConnIDExtractor(fn func(r *http.Request) (string, error)) ServerOption {
	return func(o *ServerOptions) { o.connIDExtractor = fn }
}

// WithOnUpgrade sets a unified upgrade hook that performs authentication, extracts
// the connection ID, and optionally provides metadata to inject into the Conn.
// When set, WithAuthenticator and WithConnIDExtractor are ignored by HandleHTTP.
// Return a non-nil error to reject the connection (HTTP 401).
func WithOnUpgrade(fn func(r *http.Request) (*UpgradeInfo, error)) ServerOption {
	return func(o *ServerOptions) { o.onUpgrade = fn }
}

// WithMaxConnections sets the maximum number of concurrent WebSocket connections.
func WithMaxConnections(n int64) ServerOption {
	return func(o *ServerOptions) { o.maxConnections = n }
}

// WithRateLimit configures per-connection inbound message rate limiting.
func WithRateLimit(perSec float64, burst int) ServerOption {
	return func(o *ServerOptions) {
		o.rateLimitPerSec = perSec
		o.rateLimitBurst = burst
	}
}

// WithTokenProvider sets a TokenProvider for server-side reconnect token generation and validation.
func WithTokenProvider(tp TokenProvider) ServerOption {
	return func(o *ServerOptions) { o.tokenProvider = tp }
}

// WithMessageParser sets a custom MessageParser for extracting message types from inbound data.
func WithMessageParser(p MessageParser) ServerOption {
	return func(o *ServerOptions) { o.messageParser = p }
}

// WithClusterRelay enables distributed mode with cross-node message routing.
func WithClusterRelay(r ClusterRelay) ServerOption {
	return func(o *ServerOptions) { o.clusterRelay = r }
}

// WithDrainTimeout sets the maximum time to drain pending messages on disconnect (default 5s).
func WithDrainTimeout(d time.Duration) ServerOption {
	return func(o *ServerOptions) { o.drainTimeout = d }
}

// WithLogger sets a structured logger for the Server and Hub.
func WithLogger(l Logger) ServerOption {
	return func(o *ServerOptions) { o.logger = l }
}

// WithMetrics sets a MetricsCollector for the Server and Hub.
func WithMetrics(m MetricsCollector) ServerOption {
	return func(o *ServerOptions) { o.metrics = m }
}

// ── UpgradeOption configures per-connection upgrade behavior ────────────────

// UpgradeOption configures a single WebSocket upgrade call.
type UpgradeOption func(*upgradeOpts)

type upgradeOpts struct {
	token string // explicit reconnect token; empty = read from query string
}

// WithReconnectToken provides the reconnect token explicitly instead of reading it from the query string.
func WithReconnectToken(token string) UpgradeOption {
	return func(o *upgradeOpts) { o.token = token }
}

// ── Default JSON MessageParser ─────────────────────────────────────────────

// jsonTypeParser is the default MessageParser that extracts the "type" field from a JSON message.
type jsonTypeParser struct{}

func (jsonTypeParser) ParseType(data []byte) string {
	var envelope struct {
		Type string `json:"type"`
	}
	if json.Unmarshal(data, &envelope) == nil {
		return envelope.Type
	}
	return ""
}
