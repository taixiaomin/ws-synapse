package core

import "context"

// EventHandler defines lifecycle callbacks that business code implements.
type EventHandler interface {
	// OnConnect is fired when a new connection is accepted, BEFORE it is
	// registered in the Hub. The Conn is usable for Send and metadata.
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

// PendingStore abstracts offline message persistence.
type PendingStore interface {
	// Push stores a message for an offline connection.
	Push(ctx context.Context, connID string, data []byte) error
	// PopAll atomically retrieves and removes all pending messages for a connection.
	PopAll(ctx context.Context, connID string) ([][]byte, error)
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

