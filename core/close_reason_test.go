package core

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// disconnectRecorder captures what OnDisconnect sees for every connection.
type disconnectRecorder struct {
	connected chan *Conn
	ended     chan CloseInfo
	duration  chan time.Duration
}

func newDisconnectRecorder() *disconnectRecorder {
	return &disconnectRecorder{
		connected: make(chan *Conn, 4),
		ended:     make(chan CloseInfo, 4),
		duration:  make(chan time.Duration, 4),
	}
}

func (h *disconnectRecorder) OnConnect(_ context.Context, c *Conn) error {
	h.connected <- c
	return nil
}
func (h *disconnectRecorder) OnMessage(context.Context, *Conn, *Message) error { return nil }
func (h *disconnectRecorder) OnError(context.Context, *Conn, error)            {}
func (h *disconnectRecorder) OnDisconnect(_ context.Context, c *Conn) {
	h.duration <- time.Since(c.ConnectedAt())
	h.ended <- c.CloseInfo()
}

func (h *disconnectRecorder) waitConnected(t *testing.T) *Conn {
	t.Helper()
	select {
	case c := <-h.connected:
		return c
	case <-time.After(3 * time.Second):
		t.Fatal("OnConnect not called")
		return nil
	}
}

func (h *disconnectRecorder) waitEnded(t *testing.T) CloseInfo {
	t.Helper()
	select {
	case info := <-h.ended:
		return info
	case <-time.After(10 * time.Second):
		t.Fatal("OnDisconnect not called")
		return CloseInfo{}
	}
}

// reasonCounter implements MetricsCollector plus the optional CloseReasonCollector.
type reasonCounter struct {
	nilMetrics
	mu     sync.Mutex
	counts map[CloseReason]int
}

func (m *reasonCounter) IncCloses(reason CloseReason) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counts[reason]++
}

func (m *reasonCounter) count(reason CloseReason) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.counts[reason]
}

// startServer runs srv behind an httptest server; every upgrade gets connID when it is
// non-empty, otherwise a fresh one.
func startServer(t *testing.T, srv *Server, connID string) string {
	t.Helper()
	hs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if connID != "" {
			srv.upgrade(w, r, connID, nil, nil)
			return
		}
		srv.Upgrade(w, r)
	}))
	t.Cleanup(hs.Close)
	return "ws" + strings.TrimPrefix(hs.URL, "http")
}

func dial(t *testing.T, url string) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = c.CloseNow() })
	return c
}

func newRecordingServer(h *disconnectRecorder, opts ...ServerOption) *Server {
	return NewServer(h, append([]ServerOption{WithLogger(NopLogger{})}, opts...)...)
}

func assertReason(t *testing.T, info CloseInfo, want CloseReason, wantCode int) {
	t.Helper()
	if info.Reason != want || info.Code != wantCode {
		t.Fatalf("close = %q code %d (err %v), want %q code %d", info.Reason, info.Code, info.Err, want, wantCode)
	}
}

// --- tests ---

func TestCloseReason_ClientCloseCarriesCode(t *testing.T) {
	h := newDisconnectRecorder()
	url := startServer(t, newRecordingServer(h), "")
	client := dial(t, url)
	h.waitConnected(t)

	_ = client.Close(websocket.StatusCode(4001), "bye")

	assertReason(t, h.waitEnded(t), CloseReasonClientClose, 4001)
}

func TestCloseReason_ClientGoneWithoutCloseFrame(t *testing.T) {
	h := newDisconnectRecorder()
	url := startServer(t, newRecordingServer(h), "")
	client := dial(t, url)
	h.waitConnected(t)

	_ = client.CloseNow()

	info := h.waitEnded(t)
	assertReason(t, info, CloseReasonClientGone, -1)
	if info.Reason.Abnormal() {
		t.Fatal("a client going away is not abnormal")
	}
}

// The QA X1 case: the client is connected but its read loop is not running, so the
// heartbeat ping never gets its pong.
func TestCloseReason_PingTimeoutWhenClientStopsReading(t *testing.T) {
	h := newDisconnectRecorder()
	srv := newRecordingServer(h, WithPingInterval(200*time.Millisecond), WithPongTimeout(300*time.Millisecond))
	url := startServer(t, srv, "")
	dial(t, url) // never reads, so the client library never answers the ping
	h.waitConnected(t)

	info := h.waitEnded(t)
	assertReason(t, info, CloseReasonPingTimeout, -1)
	if !info.Reason.Abnormal() {
		t.Fatal("a ping timeout is abnormal")
	}
	if d := <-h.duration; d < 400*time.Millisecond || d > 3*time.Second {
		t.Fatalf("session lasted %v, want about ping interval + pong timeout", d)
	}
}

// A client that stops reading while the server keeps sending fills the TCP buffers until
// a write cannot finish within the write timeout.
func TestCloseReason_WriteTimeoutWhenSendBufferFills(t *testing.T) {
	h := newDisconnectRecorder()
	srv := newRecordingServer(h, WithWriteTimeout(300*time.Millisecond), WithPingInterval(time.Hour),
		WithSendChannelSize(1024))
	url := startServer(t, srv, "")
	dial(t, url) // never reads
	c := h.waitConnected(t)

	chunk := make([]byte, 256<<10)
	for i := 0; i < 1024 && c.CloseInfo().Reason == CloseReasonNone; i++ {
		c.Send(chunk)
		time.Sleep(time.Millisecond)
	}

	assertReason(t, h.waitEnded(t), CloseReasonWriteTimeout, -1)
}

func TestCloseReason_ReadLimit(t *testing.T) {
	h := newDisconnectRecorder()
	url := startServer(t, newRecordingServer(h, WithMaxMessageSize(1024)), "")
	client := dial(t, url)
	h.waitConnected(t)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = client.Write(ctx, websocket.MessageText, make([]byte, 4096))

	assertReason(t, h.waitEnded(t), CloseReasonReadLimit, -1)
}

func TestCloseReason_ReplacedBySameConnID(t *testing.T) {
	h := newDisconnectRecorder()
	url := startServer(t, newRecordingServer(h), "dealer-42")
	dial(t, url)
	h.waitConnected(t)

	dial(t, url)
	h.waitConnected(t)

	assertReason(t, h.waitEnded(t), CloseReasonReplaced, -1)
}

func TestCloseReason_ServerCloseThroughHub(t *testing.T) {
	h := newDisconnectRecorder()
	srv := newRecordingServer(h)
	url := startServer(t, srv, "")
	dial(t, url)
	c := h.waitConnected(t)

	srv.Hub().CloseConn(c.ID(), "kicked")

	assertReason(t, h.waitEnded(t), CloseReasonServerClose, -1)
}

func TestCloseReason_ServerShutdown(t *testing.T) {
	h := newDisconnectRecorder()
	srv := newRecordingServer(h)
	url := startServer(t, srv, "")
	dial(t, url)
	h.waitConnected(t)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(ctx)

	assertReason(t, h.waitEnded(t), CloseReasonServerShutdown, -1)
}

func TestCloseReason_OpenConnectionHasNoReason(t *testing.T) {
	h := newDisconnectRecorder()
	url := startServer(t, newRecordingServer(h), "")
	dial(t, url)
	c := h.waitConnected(t)

	assertReason(t, c.CloseInfo(), CloseReasonNone, -1)
	if c.ConnectedAt().IsZero() {
		t.Fatal("ConnectedAt not set")
	}
}

func TestCloseReason_CollectorCountsEveryClose(t *testing.T) {
	h := newDisconnectRecorder()
	m := &reasonCounter{counts: map[CloseReason]int{}}
	url := startServer(t, newRecordingServer(h, WithMetrics(m)), "")
	client := dial(t, url)
	h.waitConnected(t)

	_ = client.CloseNow()
	h.waitEnded(t)

	if got := m.count(CloseReasonClientGone); got != 1 {
		t.Fatalf("client_gone count = %d, want 1", got)
	}
}

func TestPongTimeoutDefaultsToWriteTimeout(t *testing.T) {
	srv := NewServer(newDisconnectRecorder(), WithLogger(NopLogger{}), WithWriteTimeout(7*time.Second))
	if srv.opts.pongTimeout != 7*time.Second {
		t.Fatalf("pong timeout = %v, want the write timeout 7s", srv.opts.pongTimeout)
	}
	srv = NewServer(newDisconnectRecorder(), WithLogger(NopLogger{}), WithPongTimeout(20*time.Second))
	if srv.opts.pongTimeout != 20*time.Second {
		t.Fatalf("pong timeout = %v, want 20s", srv.opts.pongTimeout)
	}
}
