package core_test

import (
	"fmt"
	"testing"

	"erionn-mq/internal/core"
	"erionn-mq/internal/store"
)

// helper: build a broker, declare queues, create bindings for a test scenario.
func newBroker() *core.Broker {
	return core.NewBroker(func() store.MessageStore {
		return store.NewMemoryMessageStore()
	})
}

// --------------------------------------------------------------------------
// Direct exchange routing
// --------------------------------------------------------------------------

func TestDirectExchange_ExactKeyMatch(t *testing.T) {
	b := newBroker()
	b.DeclareQueue("q-orders", false, false, false, nil)
	b.DeclareQueue("q-events", false, false, false, nil)
	b.BindQueue("amq.direct", "q-orders", "orders", nil)
	b.BindQueue("amq.direct", "q-events", "events", nil)

	queues, err := b.Route("amq.direct", "orders")
	if err != nil {
		t.Fatal(err)
	}
	if len(queues) != 1 || queues[0] != "q-orders" {
		t.Fatalf("direct 'orders' → expected [q-orders], got %v", queues)
	}
}

func TestDirectExchange_NoMatchReturnsEmpty(t *testing.T) {
	b := newBroker()
	b.DeclareQueue("q-orders", false, false, false, nil)
	b.BindQueue("amq.direct", "q-orders", "orders", nil)

	queues, err := b.Route("amq.direct", "unrelated-key")
	if err != nil {
		t.Fatal(err)
	}
	if len(queues) != 0 {
		t.Fatalf("direct should not match 'unrelated-key', got %v", queues)
	}
}

func TestDirectExchange_DefaultExchangeRoutesByQueueName(t *testing.T) {
	// The default exchange ("") is a direct exchange. For the default exchange,
	// RabbitMQ automatically binds every queue with a routing key equal to its name.
	b := newBroker()
	b.DeclareQueue("my-queue", false, false, false, nil)
	// Simulate the auto-bind by the broker (tested separately for the broker).
	b.BindQueue("", "my-queue", "my-queue", nil)

	queues, err := b.Route("", "my-queue")
	if err != nil {
		t.Fatal(err)
	}
	if len(queues) != 1 || queues[0] != "my-queue" {
		t.Fatalf("default exchange should route 'my-queue' key → [my-queue], got %v", queues)
	}
}

// --------------------------------------------------------------------------
// Fanout exchange routing
// --------------------------------------------------------------------------

func TestFanoutExchange_RoutesToAllBoundQueues(t *testing.T) {
	b := newBroker()
	b.DeclareQueue("q1", false, false, false, nil)
	b.DeclareQueue("q2", false, false, false, nil)
	b.DeclareQueue("q3", false, false, false, nil)
	b.BindQueue("amq.fanout", "q1", "", nil)
	b.BindQueue("amq.fanout", "q2", "", nil)
	b.BindQueue("amq.fanout", "q3", "", nil)

	queues, err := b.Route("amq.fanout", "ignored-key")
	if err != nil {
		t.Fatal(err)
	}
	if len(queues) != 3 {
		t.Fatalf("fanout should route to all 3 queues, got %v", queues)
	}
}

func TestFanoutExchange_IgnoresRoutingKey(t *testing.T) {
	b := newBroker()
	b.DeclareQueue("qf", false, false, false, nil)
	b.BindQueue("amq.fanout", "qf", "", nil)

	for _, key := range []string{"", "any.key", "whatever"} {
		queues, _ := b.Route("amq.fanout", key)
		if len(queues) != 1 {
			t.Fatalf("fanout should route regardless of key %q, got %v", key, queues)
		}
	}
}

// --------------------------------------------------------------------------
// Topic exchange routing
// --------------------------------------------------------------------------

var topicRoutingCases = []struct {
	pattern     string
	key         string
	shouldMatch bool
}{
	// Exact match
	{"a.b.c", "a.b.c", true},
	{"a.b.c", "a.b.d", false},
	// '*' matches one word
	{"a.*.c", "a.b.c", true},
	{"a.*.c", "a.b.d", false},
	{"a.*.c", "a.b.c.d", false}, // too many words
	// '#' matches zero or more words
	{"a.#", "a", true},
	{"a.#", "a.b", true},
	{"a.#", "a.b.c.d", true},
	{"#", "a.b.c", true},
	{"#", "", true},
	// Combined
	{"*.b.#", "x.b.c.d", true},
	{"*.b.#", "x.y.b.c", false},
	{"kern.*", "kern.info", true},
	{"kern.*", "kern.info.extra", false},
	{"#.error", "system.disk.error", true},
	{"#.error", "error", true},
	{"#.error", "system.info", false},
}

func TestTopicExchange_RoutingMatrix(t *testing.T) {
	b := newBroker()

	// Declare one queue per test case so we can verify each independently.
	for i, tc := range topicRoutingCases {
		qName := fmt.Sprintf("q%d", i)
		b.DeclareQueue(qName, false, false, false, nil)
		if err := b.BindQueue("amq.topic", qName, tc.pattern, nil); err != nil {
			t.Fatalf("case %d BindQueue: %v", i, err)
		}
	}

	for i, tc := range topicRoutingCases {
		qName := fmt.Sprintf("q%d", i)
		queues, err := b.Route("amq.topic", tc.key)
		if err != nil {
			t.Fatalf("case %d Route: %v", i, err)
		}

		matched := contains(queues, qName)
		if matched != tc.shouldMatch {
			t.Errorf("topic pattern=%q key=%q: expected match=%v, got match=%v (queues=%v)",
				tc.pattern, tc.key, tc.shouldMatch, matched, queues)
		}
	}
}

func contains(ss []string, s string) bool {
	for _, v := range ss {
		if v == s {
			return true
		}
	}
	return false
}

// --------------------------------------------------------------------------
// Broker.Publish end-to-end
// --------------------------------------------------------------------------

func TestBroker_Publish_DeliverToMatchingQueues(t *testing.T) {
	b := newBroker()
	b.DeclareQueue("qa", false, false, false, nil)
	b.DeclareQueue("qb", false, false, false, nil)
	b.BindQueue("amq.direct", "qa", "key-a", nil)
	b.BindQueue("amq.direct", "qb", "key-b", nil)

	msg := store.Message{Body: []byte("hello")}
	if err := b.Publish("amq.direct", "key-a", msg); err != nil {
		t.Fatal(err)
	}

	qa, _ := b.GetQueue("qa")
	qb, _ := b.GetQueue("qb")

	if qa.Len() != 1 {
		t.Fatalf("qa should have 1 message, got %d", qa.Len())
	}
	if qb.Len() != 0 {
		t.Fatalf("qb should have 0 messages, got %d", qb.Len())
	}
}

// --------------------------------------------------------------------------
// Default exchanges pre-registration
// --------------------------------------------------------------------------

func TestBroker_DefaultExchangesExist(t *testing.T) {
	b := newBroker()
	for _, name := range []string{"", "amq.direct", "amq.topic", "amq.fanout"} {
		if _, err := b.GetExchange(name); err != nil {
			t.Errorf("default exchange %q missing: %v", name, err)
		}
	}
}

func TestBroker_DeclareQueue_AutoBindsDefaultExchange(t *testing.T) {
	b := newBroker()
	if _, err := b.DeclareQueue("auto-q", false, false, false, nil); err != nil {
		t.Fatal(err)
	}

	queues, err := b.Route("", "auto-q")
	if err != nil {
		t.Fatal(err)
	}
	if len(queues) != 1 || queues[0] != "auto-q" {
		t.Fatalf("default exchange should auto-bind declared queue, got %v", queues)
	}
}

func TestBroker_DeclareExchange_RedeclareMismatchErrors(t *testing.T) {
	b := newBroker()
	if _, err := b.DeclareExchange("logs", core.ExchangeDirect, false, false, false); err != nil {
		t.Fatal(err)
	}
	if _, err := b.DeclareExchange("logs", core.ExchangeTopic, false, false, false); err == nil {
		t.Fatal("expected redeclare mismatch error, got nil")
	}
}

func TestBroker_DeclareQueue_RedeclareMismatchErrors(t *testing.T) {
	b := newBroker()
	if _, err := b.DeclareQueue("jobs", false, false, false, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := b.DeclareQueue("jobs", true, false, false, nil); err == nil {
		t.Fatal("expected redeclare mismatch error, got nil")
	}
}

func TestBroker_DeclareQueue_RedeclareTreatsNilAndEmptyArgsAsEqual(t *testing.T) {
	b := newBroker()
	if _, err := b.DeclareQueue("jobs", false, false, false, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := b.DeclareQueue("jobs", false, false, false, map[string]any{}); err != nil {
		t.Fatalf("expected nil and empty args to be equivalent, got %v", err)
	}
}
