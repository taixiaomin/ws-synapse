package core

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// ── Test handlers ────────────────────────────────────────────────────────────

// baseHandler implements EventHandler only. OnConnect sends "welcome" and
// tries to subscribe to "early" (which must fail: the conn is not registered).
type baseHandler struct {
	connected    chan *Conn
	earlySubOK   chan bool
	registeredOK chan bool
}

func newBaseHandler() *baseHandler {
	return &baseHandler{
		connected:    make(chan *Conn, 1),
		earlySubOK:   make(chan bool, 1),
		registeredOK: make(chan bool, 1),
	}
}

func (h *baseHandler) OnConnect(ctx context.Context, c *Conn) error {
	h.earlySubOK <- c.Subscribe(ctx, "early")
	c.Send([]byte("welcome"))
	h.connected <- c
	return nil
}
func (h *baseHandler) OnMessage(context.Context, *Conn, *Message) error { return nil }
func (h *baseHandler) OnDisconnect(context.Context, *Conn)              {}
func (h *baseHandler) OnError(context.Context, *Conn, error)            {}

// registeredHandler also implements RegisteredEventHandler: it subscribes to
// "seller:topic" and pushes a snapshot.
type registeredHandler struct{ *baseHandler }

func (h registeredHandler) OnRegistered(ctx context.Context, c *Conn) {
	h.registeredOK <- c.Subscribe(ctx, "seller:topic")
	c.Send([]byte("snapshot"))
}

// panickingHandler panics in OnRegistered.
type panickingHandler struct{ *baseHandler }

func (h panickingHandler) OnRegistered(context.Context, *Conn) { panic("boom") }

// ── Helpers ──────────────────────────────────────────────────────────────────

func dialServer(t *testing.T, handler EventHandler) (*Server, *websocket.Conn) {
	t.Helper()
	srv := NewServer(handler, WithLogger(NopLogger{}))
	hs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		srv.Upgrade(w, r)
	}))
	t.Cleanup(hs.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(hs.URL, "http"), nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = client.Close(websocket.StatusNormalClosure, "") })
	return srv, client
}

func readText(t *testing.T, c *websocket.Conn) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, data, err := c.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return string(data)
}

func recvBool(t *testing.T, ch <-chan bool, what string) bool {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(3 * time.Second):
		t.Fatalf("%s not called", what)
		return false
	}
}

func waitConn(t *testing.T, h *baseHandler) *Conn {
	t.Helper()
	select {
	case c := <-h.connected:
		return c
	case <-time.After(3 * time.Second):
		t.Fatal("OnConnect not called")
		return nil
	}
}

// ── Tests ────────────────────────────────────────────────────────────────────

func TestOnRegistered_SubscribeSucceedsAndReceivesBroadcast(t *testing.T) {
	h := newBaseHandler()
	srv, client := dialServer(t, registeredHandler{h})
	waitConn(t, h)

	if ok := recvBool(t, h.earlySubOK, "OnConnect"); ok {
		t.Error("Subscribe in OnConnect should fail: conn is not registered yet")
	}
	if ok := recvBool(t, h.registeredOK, "OnRegistered"); !ok {
		t.Fatal("Subscribe in OnRegistered should succeed")
	}

	if got := readText(t, client); got != "welcome" {
		t.Fatalf("first message = %q, want welcome", got)
	}
	if got := readText(t, client); got != "snapshot" {
		t.Fatalf("second message = %q, want snapshot (OnConnect output must precede OnRegistered output)", got)
	}

	srv.Hub().Broadcast(context.Background(), "seller:topic", []byte("update"))
	if got := readText(t, client); got != "update" {
		t.Fatalf("broadcast = %q, want update", got)
	}
}

func TestOnRegistered_NotImplemented_ConnectionWorksAsBefore(t *testing.T) {
	h := newBaseHandler()
	srv, client := dialServer(t, h)
	c := waitConn(t, h)

	if got := readText(t, client); got != "welcome" {
		t.Fatalf("first message = %q, want welcome", got)
	}
	if err := srv.Hub().Send(context.Background(), c.ID(), []byte("direct")); err != nil {
		t.Fatalf("send: %v", err)
	}
	if got := readText(t, client); got != "direct" {
		t.Fatalf("direct = %q, want direct", got)
	}
}

func TestOnRegistered_PanicKeepsConnectionUp(t *testing.T) {
	h := newBaseHandler()
	srv, client := dialServer(t, panickingHandler{h})
	c := waitConn(t, h)

	if got := readText(t, client); got != "welcome" {
		t.Fatalf("first message = %q, want welcome", got)
	}
	if err := srv.Hub().Send(context.Background(), c.ID(), []byte("still-up")); err != nil {
		t.Fatalf("send after panic: %v", err)
	}
	if got := readText(t, client); got != "still-up" {
		t.Fatalf("message after panic = %q, want still-up", got)
	}
}
