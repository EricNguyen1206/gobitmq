package store_test

import (
	"path/filepath"
	"testing"
	"time"

	"erionn-mq/internal/store"
)

func TestDurableMessageStore_PersistsReadyMessagesAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queue.gob")

	s, err := store.NewDurableMessageStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	when := time.Unix(1700000000, 0).UTC()
	if _, err := s.Enqueue(store.Message{
		Body: []byte("hello"),
		Headers: map[string]any{
			"nested": map[string]any{"bytes": []byte("v")},
			"time":   when,
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := store.NewDurableMessageStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	msg, ok, err := reopened.Dequeue()
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected persisted message")
	}
	if string(msg.Body) != "hello" {
		t.Fatalf("unexpected body: %q", string(msg.Body))
	}
	if string(msg.Headers["nested"].(map[string]any)["bytes"].([]byte)) != "v" {
		t.Fatalf("unexpected nested headers: %#v", msg.Headers)
	}
	if got, ok := msg.Headers["time"].(time.Time); !ok || !got.Equal(when) {
		t.Fatalf("unexpected time header: %#v", msg.Headers["time"])
	}
}

func TestDurableMessageStore_RequeuesUnackedMessagesOnRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queue.gob")

	s, err := store.NewDurableMessageStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Enqueue(store.Message{Body: []byte("first")}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Enqueue(store.Message{Body: []byte("second")}); err != nil {
		t.Fatal(err)
	}
	msg, ok, err := s.Dequeue()
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected dequeued message")
	}
	if string(msg.Body) != "first" {
		t.Fatalf("unexpected first body: %q", string(msg.Body))
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := store.NewDurableMessageStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	requeued, ok, err := reopened.Dequeue()
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected requeued message")
	}
	if string(requeued.Body) != "first" {
		t.Fatalf("expected requeued first message, got %q", string(requeued.Body))
	}
	if !requeued.Redelivered {
		t.Fatal("expected requeued message to be marked redelivered")
	}

	next, ok, err := reopened.Dequeue()
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected second message")
	}
	if string(next.Body) != "second" {
		t.Fatalf("expected second body, got %q", string(next.Body))
	}
}

func TestDurableMessageStore_AckedMessagesStayRemovedAfterRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "queue.gob")

	s, err := store.NewDurableMessageStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Enqueue(store.Message{Body: []byte("done")}); err != nil {
		t.Fatal(err)
	}
	msg, ok, err := s.Dequeue()
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected dequeued message")
	}
	if err := s.Ack(msg.DeliveryTag); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := store.NewDurableMessageStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	if reopened.Len() != 0 {
		t.Fatalf("expected empty queue after restart, got %d", reopened.Len())
	}
}
