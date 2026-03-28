package broker_test

import (
	"path/filepath"
	"testing"

	"erionn-mq/internal/broker"
	"erionn-mq/internal/store"
)

func TestDurableBroker_RestoresDurableTopologyAndMessages(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "broker")

	br, err := broker.NewDurableBroker(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := br.DeclareExchange("orders", broker.ExchangeDirect, true, false, false); err != nil {
		t.Fatal(err)
	}
	if _, err := br.DeclareQueue("jobs", true, false, false, nil); err != nil {
		t.Fatal(err)
	}
	if err := br.BindQueue("orders", "jobs", "jobs.created", nil); err != nil {
		t.Fatal(err)
	}
	if err := br.Publish("orders", "jobs.created", store.Message{Body: []byte("hello")}); err != nil {
		t.Fatal(err)
	}
	if err := br.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := broker.NewDurableBroker(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	if _, err := reopened.GetExchange("orders"); err != nil {
		t.Fatal(err)
	}
	routes, err := reopened.Route("orders", "jobs.created")
	if err != nil {
		t.Fatal(err)
	}
	if len(routes) != 1 || routes[0] != "jobs" {
		t.Fatalf("unexpected restored bindings: %v", routes)
	}

	queue, err := reopened.GetQueue("jobs")
	if err != nil {
		t.Fatal(err)
	}
	if queue.Len() != 1 {
		t.Fatalf("expected one restored message, got %d", queue.Len())
	}
	msg, ok, err := queue.Dequeue()
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected restored message")
	}
	if string(msg.Body) != "hello" {
		t.Fatalf("unexpected restored body: %q", string(msg.Body))
	}
}

func TestDurableBroker_DeleteQueueRemovesPersistedState(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "broker")

	br, err := broker.NewDurableBroker(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := br.DeclareQueue("jobs", true, false, false, nil); err != nil {
		t.Fatal(err)
	}
	queue, err := br.GetQueue("jobs")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Enqueue(store.Message{Body: []byte("hello")}); err != nil {
		t.Fatal(err)
	}
	if err := br.DeleteQueue("jobs"); err != nil {
		t.Fatal(err)
	}
	if err := br.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := broker.NewDurableBroker(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	if _, err := reopened.GetQueue("jobs"); err == nil {
		t.Fatal("expected deleted queue to stay removed after restart")
	}
}
