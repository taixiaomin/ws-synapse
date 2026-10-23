package core

import (
	"context"
	"errors"
	"io"

	"github.com/coder/websocket"
)

// CloseReason says why a connection ended.
//
// The read pump, the write pump and the Hub can each end a connection, and once one of
// them has, the others usually fail too (a closed socket makes every later read and
// write error out). Only the first cause is recorded, so OnDisconnect sees the real
// reason rather than the knock-on error of whichever pump noticed last.
type CloseReason string

const (
	// CloseReasonNone means the connection has not ended yet.
	CloseReasonNone CloseReason = ""
	// CloseReasonClientClose means the peer sent a close frame; CloseInfo.Code carries its
	// status (1000 normal, 1001 going away, 1005 no status, 4000-4999 application).
	CloseReasonClientClose CloseReason = "client_close"
	// CloseReasonClientGone means the peer's TCP connection went away without a close
	// frame: network loss, a killed process, a load balancer reaping the socket.
	CloseReasonClientGone CloseReason = "client_gone"
	// CloseReasonPingTimeout means the heartbeat ping got no pong within the pong timeout.
	// The client is connected but not reading, or reading too slowly.
	CloseReasonPingTimeout CloseReason = "ping_timeout"
	// CloseReasonWriteTimeout means a write did not complete within the write timeout,
	// usually because the client stopped reading and the TCP send buffer filled up.
	CloseReasonWriteTimeout CloseReason = "write_timeout"
	// CloseReasonWriteError means a write or ping failed for a reason other than a timeout.
	CloseReasonWriteError CloseReason = "write_error"
	// CloseReasonReadLimit means the client sent a message larger than the max message size.
	CloseReasonReadLimit CloseReason = "read_limit"
	// CloseReasonReadError means reading failed for any other reason.
	CloseReasonReadError CloseReason = "read_error"
	// CloseReasonReplaced means a new connection with the same connection ID took over.
	CloseReasonReplaced CloseReason = "replaced"
	// CloseReasonServerClose means application code closed the connection through Hub.CloseConn.
	CloseReasonServerClose CloseReason = "server_close"
	// CloseReasonServerShutdown means the server was shutting down.
	CloseReasonServerShutdown CloseReason = "server_shutdown"
)

// Abnormal reports whether the reason points at a fault worth a warning rather than an
// ordinary end of a session (the client leaving, a reconnect, a deploy).
func (r CloseReason) Abnormal() bool {
	switch r {
	case CloseReasonPingTimeout, CloseReasonWriteTimeout, CloseReasonWriteError,
		CloseReasonReadLimit, CloseReasonReadError:
		return true
	}
	return false
}

// CloseInfo describes how a connection ended.
type CloseInfo struct {
	// Reason is the first recorded cause; CloseReasonNone while the connection is open.
	Reason CloseReason
	// Code is the peer's close status for CloseReasonClientClose, -1 otherwise.
	Code int
	// Err is the error behind the reason, nil when there was none.
	Err error
}

// CloseReasonCollector is an optional extension of MetricsCollector. A collector that
// also implements it is told the reason of every connection that ends.
type CloseReasonCollector interface {
	IncCloses(reason CloseReason)
}

// CloseInfo returns how the connection ended. Before it ends, Reason is CloseReasonNone.
// Inside OnDisconnect the reason is always set.
func (c *Conn) CloseInfo() CloseInfo {
	if info := c.closeInfo.Load(); info != nil {
		return *info
	}
	return CloseInfo{Code: -1}
}

// recordClose stores the cause of the connection's end. Only the first call has effect.
func (c *Conn) recordClose(reason CloseReason, code int, err error) {
	c.closeInfo.CompareAndSwap(nil, &CloseInfo{Reason: reason, Code: code, Err: err})
}

// readCloseReason classifies the error that ended the read pump.
func readCloseReason(err error) (CloseReason, int) {
	if code := websocket.CloseStatus(err); code != -1 {
		return CloseReasonClientClose, int(code)
	}
	switch {
	case errors.Is(err, websocket.ErrMessageTooBig):
		return CloseReasonReadLimit, -1
	case errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return CloseReasonClientGone, -1
	}
	// net.ErrClosed or a cancelled context land here too: the socket was closed on this
	// side, and whoever closed it has already recorded the real reason.
	return CloseReasonReadError, -1
}

// writeCloseReason classifies the error that ended the write pump. ping is true when the
// failure came from the heartbeat ping, whose deadline is the pong timeout.
func writeCloseReason(err error, ping bool) CloseReason {
	if errors.Is(err, context.DeadlineExceeded) {
		if ping {
			return CloseReasonPingTimeout
		}
		return CloseReasonWriteTimeout
	}
	return CloseReasonWriteError
}
