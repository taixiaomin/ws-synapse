package store

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/taixiaomin/ws-synapse/core"
)

func setup(t *testing.T, opts ...Option) (*RedisPendingStore, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { client.Close() })
	return NewRedisPendingStore(client, opts...), mr
}

func TestPushText_PopAllReturnsMsgTypeText(t *testing.T) {
	s, _ := setup(t)
	ctx := context.Background()

	if err := s.Push(ctx, "c1", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	msgs, err := s.PopAll(ctx, "c1")
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 {
		t.Fatalf("got %d messages, want 1", len(msgs))
	}
	if string(msgs[0].Data) != "hello" || msgs[0].MsgType != core.MsgTypeText {
		t.Fatalf("got Data=%q MsgType=%d, want hello/%d", msgs[0].Data, msgs[0].MsgType, core.MsgTypeText)
	}
}

func TestPushEnvelopeBinary_PopAllReturnsMsgTypeBinary(t *testing.T) {
	s, _ := setup(t)
	ctx := context.Background()

	err := s.PushEnvelope(ctx, "c1", core.PendingMessage{Data: []byte{0xDE, 0xAD}, MsgType: core.MsgTypeBinary})
	if err != nil {
		t.Fatal(err)
	}
	msgs, err := s.PopAll(ctx, "c1")
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 {
		t.Fatalf("got %d messages, want 1", len(msgs))
	}
	if msgs[0].MsgType != core.MsgTypeBinary {
		t.Fatalf("MsgType=%d, want %d", msgs[0].MsgType, core.MsgTypeBinary)
	}
	if len(msgs[0].Data) != 2 || msgs[0].Data[0] != 0xDE || msgs[0].Data[1] != 0xAD {
		t.Fatalf("Data=%x, want dead", msgs[0].Data)
	}
}

func TestMixedMessages_PreserveOrderAndTypes(t *testing.T) {
	s, _ := setup(t)
	ctx := context.Background()

	s.Push(ctx, "c1", []byte("text1"))
	s.PushEnvelope(ctx, "c1", core.PendingMessage{Data: []byte{0xFF}, MsgType: core.MsgTypeBinary})
	s.Push(ctx, "c1", []byte("text2"))

	msgs, err := s.PopAll(ctx, "c1")
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 3 {
		t.Fatalf("got %d messages, want 3", len(msgs))
	}

	expect := []struct {
		data    string
		msgType int
	}{
		{"text1", core.MsgTypeText},
		{string([]byte{0xFF}), core.MsgTypeBinary},
		{"text2", core.MsgTypeText},
	}
	for i, e := range expect {
		if string(msgs[i].Data) != e.data || msgs[i].MsgType != e.msgType {
			t.Errorf("[%d] got Data=%q MsgType=%d, want %q/%d", i, msgs[i].Data, msgs[i].MsgType, e.data, e.msgType)
		}
	}
}

func TestPopAll_Atomic_SecondCallReturnsNil(t *testing.T) {
	s, _ := setup(t)
	ctx := context.Background()

	s.Push(ctx, "c1", []byte("msg"))

	msgs, _ := s.PopAll(ctx, "c1")
	if len(msgs) != 1 {
		t.Fatalf("first PopAll: got %d, want 1", len(msgs))
	}

	msgs, err := s.PopAll(ctx, "c1")
	if err != nil {
		t.Fatal(err)
	}
	if msgs != nil {
		t.Fatalf("second PopAll: got %v, want nil", msgs)
	}
}

func TestMaxMessages_Trimming(t *testing.T) {
	s, _ := setup(t, WithMaxMessages(3))
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		s.Push(ctx, "c1", []byte{byte('a' + i)})
	}

	msgs, err := s.PopAll(ctx, "c1")
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 3 {
		t.Fatalf("got %d messages, want 3", len(msgs))
	}
	// Should keep the last 3: c, d, e
	for i, expected := range []byte{'c', 'd', 'e'} {
		if len(msgs[i].Data) != 1 || msgs[i].Data[0] != expected {
			t.Errorf("[%d] got %q, want %q", i, msgs[i].Data, string(expected))
		}
	}
}

func TestLegacyData_DecodedAsText(t *testing.T) {
	s, mr := setup(t)
	ctx := context.Background()

	// Manually RPUSH raw bytes without the 1-byte type prefix (legacy format).
	key := DefaultKeyPrefix + "c1"
	mr.RPush(key, "legacy-message")

	msgs, err := s.PopAll(ctx, "c1")
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 {
		t.Fatalf("got %d messages, want 1", len(msgs))
	}
	if string(msgs[0].Data) != "legacy-message" {
		t.Fatalf("Data=%q, want legacy-message", msgs[0].Data)
	}
	if msgs[0].MsgType != core.MsgTypeText {
		t.Fatalf("MsgType=%d, want %d", msgs[0].MsgType, core.MsgTypeText)
	}
}

func TestPopAll_Empty_ReturnsNil(t *testing.T) {
	s, _ := setup(t)
	ctx := context.Background()

	msgs, err := s.PopAll(ctx, "nonexistent")
	if err != nil {
		t.Fatal(err)
	}
	if msgs != nil {
		t.Fatalf("got %v, want nil", msgs)
	}
}

func TestTTL_ExpiresMessages(t *testing.T) {
	s, mr := setup(t, WithTTL(10*time.Second))
	ctx := context.Background()

	s.Push(ctx, "c1", []byte("ephemeral"))

	mr.FastForward(11 * time.Second)

	msgs, err := s.PopAll(ctx, "c1")
	if err != nil {
		t.Fatal(err)
	}
	if msgs != nil {
		t.Fatalf("got %v after TTL, want nil", msgs)
	}
}
