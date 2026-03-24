package store_test

import (
	"testing"

	"erionn-mq/internal/store"
)

func newStore() *store.MemoryMessageStore {
	return store.NewMemoryMessageStore()
}

func makeMsg(body string) store.Message {
	return store.Message{Body: []byte(body)}
}

// --------------------------------------------------------------------------
// Enqueue / Len
// --------------------------------------------------------------------------

func TestMemoryStore_Enqueue_AssignsSequentialTags(t *testing.T) {
	s := newStore()
	for i := uint64(1); i <= 5; i++ {
		tag, err := s.Enqueue(makeMsg("x"))
		if err != nil {
			t.Fatal(err)
		}
		if tag != i {
			t.Fatalf("expected tag=%d, got %d", i, tag)
		}
	}
}

func TestMemoryStore_Len_ReflectsOnlyReadyMessages(t *testing.T) {
	s := newStore()
	s.Enqueue(makeMsg("a"))
	s.Enqueue(makeMsg("b"))
	s.Enqueue(makeMsg("c"))

	if s.Len() != 3 {
		t.Fatalf("expected Len=3, got %d", s.Len())
	}

	// Dequeue one → moves to unacked, should reduce Len.
	s.Dequeue()
	if s.Len() != 2 {
		t.Fatalf("after dequeue, expected Len=2, got %d", s.Len())
	}
}

// --------------------------------------------------------------------------
// Dequeue
// --------------------------------------------------------------------------

func TestMemoryStore_Dequeue_FIFOOrder(t *testing.T) {
	s := newStore()
	s.Enqueue(makeMsg("first"))
	s.Enqueue(makeMsg("second"))
	s.Enqueue(makeMsg("third"))

	for _, want := range []string{"first", "second", "third"} {
		msg, ok, err := s.Dequeue()
		if err != nil || !ok {
			t.Fatalf("expected message %q, got ok=%v err=%v", want, ok, err)
		}
		if string(msg.Body) != want {
			t.Fatalf("expected body=%q, got %q", want, string(msg.Body))
		}
	}
}

func TestMemoryStore_Dequeue_EmptyQueueReturnsFalse(t *testing.T) {
	s := newStore()
	_, ok, err := s.Dequeue()
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected ok=false on empty queue")
	}
}

// --------------------------------------------------------------------------
// Ack
// --------------------------------------------------------------------------

func TestMemoryStore_Ack_RemovesFromUnacked(t *testing.T) {
	s := newStore()
	s.Enqueue(makeMsg("msg"))
	msg, _, _ := s.Dequeue()

	if err := s.Ack(msg.DeliveryTag); err != nil {
		t.Fatal(err)
	}
	// Acking the same tag again should fail.
	if err := s.Ack(msg.DeliveryTag); err == nil {
		t.Fatal("expected error double-acking, got nil")
	}
}

func TestMemoryStore_Ack_UnknownTagErrors(t *testing.T) {
	s := newStore()
	if err := s.Ack(999); err == nil {
		t.Fatal("expected error acking unknown tag")
	}
}

// --------------------------------------------------------------------------
// Nack + requeue
// --------------------------------------------------------------------------

func TestMemoryStore_Nack_Requeue_PrependToHead(t *testing.T) {
	s := newStore()
	s.Enqueue(makeMsg("first"))
	s.Enqueue(makeMsg("second"))

	// Dequeue "first", nack+requeue it.
	first, _, _ := s.Dequeue()
	if err := s.Nack(first.DeliveryTag, true); err != nil {
		t.Fatal(err)
	}

	// Next dequeue should return the requeued "first" again (head-of-line).
	next, ok, _ := s.Dequeue()
	if !ok {
		t.Fatal("expected a message after nack+requeue")
	}
	if string(next.Body) != "first" {
		t.Fatalf("expected requeued 'first', got %q", string(next.Body))
	}
	if !next.Redelivered {
		t.Fatal("requeued message should have Redelivered=true")
	}
}

func TestMemoryStore_Nack_NoRequeue_Discards(t *testing.T) {
	s := newStore()
	s.Enqueue(makeMsg("msg"))
	msg, _, _ := s.Dequeue()

	if err := s.Nack(msg.DeliveryTag, false); err != nil {
		t.Fatal(err)
	}
	if s.Len() != 0 {
		t.Fatalf("nack without requeue should discard message, Len=%d", s.Len())
	}
}

func TestMemoryStore_Nack_UnknownTagErrors(t *testing.T) {
	s := newStore()
	if err := s.Nack(999, true); err == nil {
		t.Fatal("expected error nacking unknown tag")
	}
}

func TestMemoryStore_Dequeue_ReturnsImmutableCopy(t *testing.T) {
	s := newStore()
	body := []byte("hello")
	headers := map[string]any{"nested": map[string]any{"k": []byte("v")}}

	if _, err := s.Enqueue(store.Message{Body: body, Headers: headers}); err != nil {
		t.Fatal(err)
	}
	body[0] = 'X'
	headers["nested"].(map[string]any)["k"].([]byte)[0] = 'Z'

	msg, ok, err := s.Dequeue()
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected dequeued message")
	}
	if string(msg.Body) != "hello" {
		t.Fatalf("expected stored body to be immutable, got %q", string(msg.Body))
	}
	if string(msg.Headers["nested"].(map[string]any)["k"].([]byte)) != "v" {
		t.Fatalf("expected stored headers to be immutable, got %#v", msg.Headers)
	}

	msg.Body[0] = 'Y'
	msg.Headers["nested"].(map[string]any)["k"].([]byte)[0] = 'Q'
	if err := s.Nack(msg.DeliveryTag, true); err != nil {
		t.Fatal(err)
	}

	requeued, ok, err := s.Dequeue()
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected requeued message")
	}
	if string(requeued.Body) != "hello" {
		t.Fatalf("expected requeued body to remain immutable, got %q", string(requeued.Body))
	}
}
