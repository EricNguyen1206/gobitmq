package broker_test

import (
	"testing"

	"erionn-mq/internal/broker"
	"erionn-mq/internal/logstore"
	"erionn-mq/internal/partition"
)

func newBrokerWithTopic(t *testing.T, topic string, partitionIDs ...int) *broker.Broker {
	t.Helper()
	b := broker.New()
	parts := make([]*partition.Partition, 0, len(partitionIDs))
	for _, id := range partitionIDs {
		parts = append(parts, partition.New(topic, id, logstore.NewMemoryLog()))
	}
	if err := b.CreateTopic(topic, parts); err != nil {
		t.Fatalf("CreateTopic: %v", err)
	}
	return b
}

func TestBroker_Produce_ReturnsSequentialOffsets(t *testing.T) {
	b := newBrokerWithTopic(t, "orders", 0)

	for i := int64(0); i < 5; i++ {
		offset, err := b.Produce("orders", 0, nil, []byte("value"))
		if err != nil {
			t.Fatalf("Produce[%d] unexpected error: %v", i, err)
		}
		if offset != i {
			t.Fatalf("expected offset=%d, got %d", i, offset)
		}
	}
}

func TestBroker_Fetch_ReturnsProducedRecords(t *testing.T) {
	b := newBrokerWithTopic(t, "events", 0)

	messages := []string{"alpha", "beta", "gamma"}
	for _, msg := range messages {
		b.Produce("events", 0, []byte("k"), []byte(msg)) //nolint:errcheck
	}

	records, hw, err := b.Fetch("events", 0, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if hw != 2 {
		t.Fatalf("expected high watermark=2, got %d", hw)
	}
	if len(records) != len(messages) {
		t.Fatalf("expected %d records, got %d", len(messages), len(records))
	}
	for i, r := range records {
		if string(r.Value) != messages[i] {
			t.Fatalf("record[%d]: expected value=%q, got %q", i, messages[i], string(r.Value))
		}
	}
}

func TestBroker_Fetch_PartialFromOffset(t *testing.T) {
	b := newBrokerWithTopic(t, "logs", 0)

	for i := 0; i < 5; i++ {
		b.Produce("logs", 0, nil, []byte("msg")) //nolint:errcheck
	}

	records, _, err := b.Fetch("logs", 0, 3, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records from offset=3, got %d", len(records))
	}
	if records[0].Offset != 3 {
		t.Fatalf("expected first record offset=3, got %d", records[0].Offset)
	}
}

func TestBroker_Produce_TopicNotFound(t *testing.T) {
	b := broker.New()
	_, err := b.Produce("nonexistent", 0, nil, []byte("x"))
	if err == nil {
		t.Fatal("expected error for nonexistent topic, got nil")
	}
}

func TestBroker_CreateTopic_DuplicateErrors(t *testing.T) {
	b := newBrokerWithTopic(t, "dup", 0)
	p := partition.New("dup", 1, logstore.NewMemoryLog())
	err := b.CreateTopic("dup", []*partition.Partition{p})
	if err == nil {
		t.Fatal("expected error when creating duplicate topic, got nil")
	}
}
