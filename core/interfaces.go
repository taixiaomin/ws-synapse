package core

import "context"

// EventHandler defines lifecycle callbacks that business code implements.
type EventHandler interface {
	// OnConnect is fired when a new connection is accepted, BEFORE it is
	// registered in the Hub. conn.Hub() is available for Subscribe/Send etc.
	// Return a non-nil error to reject the connection.
	OnConnect(ctx context.Context, conn *Conn) error

	// OnMessage is fired for every inbound message from the client.
	// msg.Type is pre-parsed by the configured MessageParser; msg.Raw holds
	// the full original bytes for business code to unmarshal.
	OnMessage(ctx context.Context, conn *Conn, msg *Message) error

	// OnDisconnect is fired after the connection is removed from the Hub.
	OnDisconnect(ctx context.Context, conn *Conn)

	// OnError is fired when an error occurs (read error, OnMessage error,
	// rate limiting, etc.). The Server continues reading after this call.
	OnError(ctx context.Context, conn *Conn, err error)
}

// TopicEventHandler is an optional interface that EventHandler implementations
// may also implement to receive topic lifecycle callbacks.
// The Hub checks for this interface at runtime; it is NOT required.
type TopicEventHandler interface {
	// OnSubscribe is called after a connection is added to a topic.
	OnSubscribe(ctx context.Context, connID, topic string)

	// OnUnsubscribe is called after a connection is removed from a topic.
	// This includes automatic unsubscription on disconnect.
	OnUnsubscribe(ctx context.Context, connID, topic string)
}

// MessageParser extracts the message type from raw inbound bytes.
// The default implementation parses a JSON "type" field.
type MessageParser interface {
	// ParseType extracts a message type string from raw bytes.
	// Return "" if the type cannot be determined.
	ParseType(data []byte) string
}

// Message is the inbound envelope received from the client.
type Message struct {
	Type string `json:"type"` // pre-parsed by MessageParser
	Raw  []byte `json:"-"`    // full original bytes for business to unmarshal
}

// Logger is the structured logging interface used by the ws package.
type Logger interface {
	Info(msg string, keysAndValues ...any)
	Warn(msg string, keysAndValues ...any)
	Error(msg string, keysAndValues ...any)
}

// MetricsCollector abstracts WebSocket server metrics collection.
type MetricsCollector interface {
	// IncConnections is called when a new connection is registered.
	IncConnections()
	// DecConnections is called when a connection is unregistered.
	DecConnections()
	// IncMessagesIn is called for every inbound message received.
	IncMessagesIn()
	// IncMessagesOut is called for every outbound message written.
	IncMessagesOut()
	// IncDrops is called when a message is dropped (offline with no pending store).
	IncDrops()
	// IncErrors is called on read/write/handler errors.
	IncErrors()
	// IncReconnects is called on successful token-based reconnections.
	IncReconnects()
}

// MsgTypeText and MsgTypeBinary mirror websocket.MessageText / MessageBinary
// without importing the websocket package in the interface layer.
const (
	MsgTypeText   = 1
	MsgTypeBinary = 2
)

// PendingMessage wraps pending data with its WebSocket message type.
type PendingMessage struct {
	Data    []byte
	MsgType int // MsgTypeText (default) or MsgTypeBinary
}

// PendingStore abstracts offline message persistence.
type PendingStore interface {
	// Push stores a text message for an offline connection (backward-compatible).
	Push(ctx context.Context, connID string, data []byte) error
	// PushEnvelope stores a message with explicit type for an offline connection.
	PushEnvelope(ctx context.Context, connID string, msg PendingMessage) error
	// PopAll atomically retrieves and removes all pending messages for a connection.
	PopAll(ctx context.Context, connID string) ([]PendingMessage, error)
}

// TokenProvider manages reconnect tokens for WebSocket connections.
type TokenProvider interface {
	// Generate creates a new reconnect token for the given connection ID.
	Generate(ctx context.Context, connID string) (string, error)
	// Validate checks whether the provided token is valid for the given connection ID.
	Validate(ctx context.Context, connID, token string) (bool, error)
	// Revoke removes the stored token for a connection (called on permanent disconnect).
	Revoke(ctx context.Context, connID string) error
}

// ClusterRelay abstracts cross-node message routing for multi-node deployments.
// Implementations must be safe for concurrent use from multiple goroutines.
// When no relay is configured, the Hub operates in single-node mode.
type ClusterRelay interface {
	// PublishSend routes a point-to-point message to the node hosting connID.
	// Falls back to PendingStore if the target node is unreachable.
	PublishSend(ctx context.Context, connID string, data []byte) error
	// PublishBroadcast routes a topic broadcast to nodes with subscribers.
	PublishBroadcast(ctx context.Context, topic string, data []byte, excludeIDs []string) error
	// OnRegister is called after a connection registers on this node.
	OnRegister(ctx context.Context, connID string) error
	// OnUnregister is called after a connection unregisters from this node.
	OnUnregister(ctx context.Context, connID string) error
	// OnSubscribe is called after a connection subscribes to a topic on this node.
	OnSubscribe(ctx context.Context, connID, topic string) error
	// OnUnsubscribe is called after a connection unsubscribes. lastOnNode is true
	// when this was the last subscriber for that topic on this node.
	OnUnsubscribe(ctx context.Context, connID, topic string, lastOnNode bool) error
	// Start begins listening for cross-node messages. handler receives inbound messages.
	Start(handler RelayHandler) error
	// Stop gracefully shuts down the relay and cleans up registrations.
	Stop() error
}

// RelayHandler is the callback interface that Hub implements to receive
// cross-node messages dispatched by a ClusterRelay.
type RelayHandler interface {
	// LocalSend delivers data to a locally connected client.
	LocalSend(ctx context.Context, connID string, data []byte) error
	// LocalBroadcast delivers data to all local subscribers of a topic.
	LocalBroadcast(ctx context.Context, topic string, data []byte, excludeIDs []string)
}
