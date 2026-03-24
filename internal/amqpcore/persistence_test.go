package amqpcore_test

import (
	"path/filepath"
	"testing"

	"erionn-mq/internal/amqpcore"
)

func TestDurableBroker_RestoresDurableTopologyAndMessages(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "broker")

	broker, err := amqpcore.NewDurableBroker(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := broker.DeclareExchange("orders", amqpcore.ExchangeDirect, true, false, false); err != nil {
		t.Fatal(err)
	}
	if _, err := broker.DeclareQueue("jobs", true, false, false, nil); err != nil {
		t.Fatal(err)
	}
	if err := broker.BindQueue("orders", "jobs", "jobs.created", nil); err != nil {
		t.Fatal(err)
	}
	if err := broker.Publish("orders", "jobs.created", amqpcore.Message{Body: []byte("hello")}); err != nil {
		t.Fatal(err)
	}
	if err := broker.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := amqpcore.NewDurableBroker(dataDir)
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

	broker, err := amqpcore.NewDurableBroker(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := broker.DeclareQueue("jobs", true, false, false, nil); err != nil {
		t.Fatal(err)
	}
	queue, err := broker.GetQueue("jobs")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := queue.Enqueue(amqpcore.Message{Body: []byte("hello")}); err != nil {
		t.Fatal(err)
	}
	if err := broker.DeleteQueue("jobs"); err != nil {
		t.Fatal(err)
	}
	if err := broker.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := amqpcore.NewDurableBroker(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()

	if _, err := reopened.GetQueue("jobs"); err == nil {
		t.Fatal("expected deleted queue to stay removed after restart")
	}
}
