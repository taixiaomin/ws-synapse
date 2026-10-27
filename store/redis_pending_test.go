package store

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/taixiaomin/ws-synapse/core"
)

func mustPush(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("push: %v", err)
	}
}

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

	mustPush(t, s.Push(ctx, "c1", []byte("text1")))
	mustPush(t, s.PushEnvelope(ctx, "c1", core.PendingMessage{Data: []byte{0xFF}, MsgType: core.MsgTypeBinary}))
	mustPush(t, s.Push(ctx, "c1", []byte("text2")))

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

	mustPush(t, s.Push(ctx, "c1", []byte("msg")))

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
		mustPush(t, s.Push(ctx, "c1", []byte{byte('a' + i)}))
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

	mustPush(t, s.Push(ctx, "c1", []byte("ephemeral")))
	if !mr.Exists(DefaultKeyPrefix + "c1") {
		t.Fatal("message was not stored")
	}

	mr.FastForward(11 * time.Second) // TTL is jittered to [9s, 11s)

	msgs, err := s.PopAll(ctx, "c1")
	if err != nil {
		t.Fatal(err)
	}
	if msgs != nil {
		t.Fatalf("got %v after TTL, want nil", msgs)
	}
}

// PushFront must return popped messages to the head, in their original order,
// ahead of anything pushed after the pop — not behind it, as RPUSH did.
func TestPushFront_PreservesOrderAheadOfNewerMessages(t *testing.T) {
	s, _ := setup(t)
	ctx := context.Background()

	for _, m := range []string{"m1", "m2", "m3"} {
		if err := s.Push(ctx, "c1", []byte(m)); err != nil {
			t.Fatal(err)
		}
	}
	popped, err := s.PopAll(ctx, "c1")
	if err != nil {
		t.Fatal(err)
	}
	// m1 was written; m2, m3 failed. Meanwhile m4 arrived.
	if err := s.Push(ctx, "c1", []byte("m4")); err != nil {
		t.Fatal(err)
	}
	if err := s.PushFront(ctx, "c1", popped[1:]); err != nil {
		t.Fatal(err)
	}

	got, err := s.PopAll(ctx, "c1")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"m2", "m3", "m4"}
	if len(got) != len(want) {
		t.Fatalf("got %d messages, want %d", len(got), len(want))
	}
	for i, w := range want {
		if string(got[i].Data) != w {
			t.Fatalf("message %d = %q, want %q (full: %q)", i, got[i].Data, w, got)
		}
	}
}

// PushFront keeps the type prefix, honours the cap and installs a TTL on a
// fresh key.
func TestPushFront_TypeCapAndTTL(t *testing.T) {
	s, mr := setup(t, WithMaxMessages(2), WithTTL(time.Minute))
	ctx := context.Background()

	msgs := []core.PendingMessage{
		{Data: []byte("a"), MsgType: core.MsgTypeText},
		{Data: []byte("b"), MsgType: core.MsgTypeBinary},
		{Data: []byte("c"), MsgType: core.MsgTypeText},
	}
	if err := s.PushFront(ctx, "c1", msgs); err != nil {
		t.Fatal(err)
	}
	if ttl := mr.TTL(DefaultKeyPrefix + "c1"); ttl <= 0 {
		t.Fatalf("expected a TTL on a fresh key, got %v", ttl)
	}
	got, err := s.PopAll(ctx, "c1")
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || string(got[0].Data) != "b" || got[0].MsgType != core.MsgTypeBinary || string(got[1].Data) != "c" {
		t.Fatalf("got %+v, want newest two [b(binary) c]", got)
	}
}

// The TTL is installed once per key incarnation; later pushes must not renew
// it, or a connID that never comes back keeps its key (and memory) forever.
func TestTTL_NotRenewedBySubsequentPushes(t *testing.T) {
	s, mr := setup(t, WithTTL(10*time.Second))
	ctx := context.Background()
	key := DefaultKeyPrefix + "c1"

	mustPush(t, s.Push(ctx, "c1", []byte("first")))
	first := mr.TTL(key)
	if first <= 0 {
		t.Fatalf("first push must set a TTL, got %v", first)
	}

	mr.FastForward(5 * time.Second)
	mustPush(t, s.Push(ctx, "c1", []byte("second")))
	if got := mr.TTL(key); got > first-5*time.Second {
		t.Fatalf("TTL was renewed by the second push: %v, want <= %v", got, first-5*time.Second)
	}

	mr.FastForward(first - 5*time.Second)
	if mr.Exists(key) {
		t.Fatal("key must expire at its first incarnation's TTL")
	}
}
