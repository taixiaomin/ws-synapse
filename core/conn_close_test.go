package core

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// evictingHandler answers any message by closing with an application status code.
type evictingHandler struct{ *baseHandler }

func (h evictingHandler) OnMessage(_ context.Context, c *Conn, _ *Message) error {
	return c.CloseWithStatus(4092, "denied")
}

func TestCloseWithStatus(t *testing.T) {
	_, client := dialServer(t, evictingHandler{newBaseHandler()})
	if got := readText(t, client); got != "welcome" {
		t.Fatalf("first message = %q, want welcome", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := client.Write(ctx, websocket.MessageText, []byte("hi")); err != nil {
		t.Fatalf("write: %v", err)
	}

	_, _, err := client.Read(ctx)
	var ce websocket.CloseError
	if !errors.As(err, &ce) || ce.Code != 4092 || ce.Reason != "denied" {
		t.Fatalf("close = %v, want 4092 denied", err)
	}
}
