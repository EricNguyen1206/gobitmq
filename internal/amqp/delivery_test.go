package amqp

import (
	"testing"

	"erionn-mq/internal/broker"
	"erionn-mq/internal/store"
)

func newTestChannelState() *channelState {
	return &channelState{
		broker:          broker.NewBroker(func() store.MessageStore { return store.NewMemoryMessageStore() }),
		channel:         &broker.Channel{Consumers: make(map[string]*broker.ConsumerSubscription)},
		inFlight:        make(map[uint64]deliveryRef),
		consumers:       make(map[string]*consumerState),
		nextDeliveryTag: 1,
		nextPublishSeq:  1,
	}
}

func TestReserveDelivery_AutoAckFalse_TracksInFlight(t *testing.T) {
	ch := newTestChannelState()

	tag, reserved := ch.reserveDelivery("c1", "q1", 100, store.Message{Body: []byte("hello")}, false)
	if tag != 1 {
		t.Fatalf("expected delivery tag 1, got %d", tag)
	}
	if !reserved {
		t.Fatal("expected reserved=true for non-auto-ack")
	}
	if len(ch.inFlight) != 1 {
		t.Fatalf("expected 1 in-flight, got %d", len(ch.inFlight))
	}
	ref, ok := ch.inFlight[1]
	if !ok {
		t.Fatal("expected in-flight entry for tag 1")
	}
	if ref.queueName != "q1" || ref.storeTag != 100 {
		t.Fatalf("wrong ref: queueName=%q storeTag=%d", ref.queueName, ref.storeTag)
	}
	if ch.nextDeliveryTag != 2 {
		t.Fatalf("expected nextDeliveryTag=2, got %d", ch.nextDeliveryTag)
	}
}

func TestReserveDelivery_AutoAckTrue_SkipsTracking(t *testing.T) {
	ch := newTestChannelState()

	tag, reserved := ch.reserveDelivery("c1", "q1", 100, store.Message{Body: []byte("hello")}, true)
	if tag != 1 {
		t.Fatalf("expected delivery tag 1, got %d", tag)
	}
	if reserved {
		t.Fatal("expected reserved=false for auto-ack")
	}
	if len(ch.inFlight) != 0 {
		t.Fatalf("expected 0 in-flight for auto-ack, got %d", len(ch.inFlight))
	}
}

func TestReserveDelivery_IncrementsConsumerUnacked(t *testing.T) {
	ch := newTestChannelState()
	ch.consumers["c1"] = &consumerState{tag: "c1", queueName: "q1"}

	ch.reserveDelivery("c1", "q1", 10, store.Message{}, false)
	ch.reserveDelivery("c1", "q1", 11, store.Message{}, false)

	if ch.consumers["c1"].unacked != 2 {
		t.Fatalf("expected consumer unacked=2, got %d", ch.consumers["c1"].unacked)
	}
}

func TestReleaseDelivery_RemovesInFlight(t *testing.T) {
	ch := newTestChannelState()
	ch.consumers["c1"] = &consumerState{tag: "c1", queueName: "q1", unacked: 1}
	ch.inFlight[1] = deliveryRef{queueName: "q1", storeTag: 10, consumerTag: "c1"}

	ch.releaseDelivery(1)

	if len(ch.inFlight) != 0 {
		t.Fatalf("expected 0 in-flight after release, got %d", len(ch.inFlight))
	}
	if ch.consumers["c1"].unacked != 0 {
		t.Fatalf("expected consumer unacked=0 after release, got %d", ch.consumers["c1"].unacked)
	}
}

func TestReleaseDelivery_UnknownTag_NoOp(t *testing.T) {
	ch := newTestChannelState()

	ch.releaseDelivery(999)

	if len(ch.inFlight) != 0 {
		t.Fatalf("expected 0 in-flight, got %d", len(ch.inFlight))
	}
}

func TestReleaseDelivery_AlreadyReleased_NoOp(t *testing.T) {
	ch := newTestChannelState()
	ch.consumers["c1"] = &consumerState{tag: "c1", queueName: "q1", unacked: 1}
	ch.inFlight[1] = deliveryRef{queueName: "q1", storeTag: 10, consumerTag: "c1"}

	ch.releaseDelivery(1)
	ch.releaseDelivery(1)

	if ch.consumers["c1"].unacked != 0 {
		t.Fatalf("expected consumer unacked=0 after double release, got %d", ch.consumers["c1"].unacked)
	}
}

func TestTakeRefs_SingleAck(t *testing.T) {
	ch := newTestChannelState()
	ch.consumers["c1"] = &consumerState{tag: "c1", queueName: "q1", unacked: 1}
	ch.inFlight[1] = deliveryRef{queueName: "q1", storeTag: 10, consumerTag: "c1"}
	ch.inFlight[2] = deliveryRef{queueName: "q1", storeTag: 20, consumerTag: "c1"}

	refs, err := ch.takeRefs(1, false)
	if err != nil {
		t.Fatalf("takeRefs(1, false): %v", err)
	}
	if len(refs) != 1 {
		t.Fatalf("expected 1 ref, got %d", len(refs))
	}
	if refs[0].storeTag != 10 {
		t.Fatalf("expected storeTag=10, got %d", refs[0].storeTag)
	}
	if len(ch.inFlight) != 1 {
		t.Fatalf("expected 1 remaining in-flight, got %d", len(ch.inFlight))
	}
	if ch.consumers["c1"].unacked != 0 {
		t.Fatalf("expected consumer unacked=0, got %d", ch.consumers["c1"].unacked)
	}
}

func TestTakeRefs_MultipleAck(t *testing.T) {
	ch := newTestChannelState()
	ch.consumers["c1"] = &consumerState{tag: "c1", queueName: "q1"}
	ch.consumers["c2"] = &consumerState{tag: "c2", queueName: "q2"}
	ch.inFlight[1] = deliveryRef{queueName: "q1", storeTag: 10, consumerTag: "c1"}
	ch.inFlight[2] = deliveryRef{queueName: "q2", storeTag: 20, consumerTag: "c2"}
	ch.inFlight[3] = deliveryRef{queueName: "q1", storeTag: 30, consumerTag: "c1"}
	ch.consumers["c1"].unacked = 2
	ch.consumers["c2"].unacked = 1

	refs, err := ch.takeRefs(2, true)
	if err != nil {
		t.Fatalf("takeRefs(2, true): %v", err)
	}
	if len(refs) != 2 {
		t.Fatalf("expected 2 refs, got %d", len(refs))
	}
	if len(ch.inFlight) != 1 {
		t.Fatalf("expected 1 remaining in-flight, got %d", len(ch.inFlight))
	}
	if _, ok := ch.inFlight[3]; !ok {
		t.Fatal("expected tag 3 to remain in-flight")
	}
	if ch.consumers["c1"].unacked != 1 {
		t.Fatalf("expected c1 unacked=1 (tag 3 still in-flight), got %d", ch.consumers["c1"].unacked)
	}
	if ch.consumers["c2"].unacked != 0 {
		t.Fatalf("expected c2 unacked=0, got %d", ch.consumers["c2"].unacked)
	}
}

func TestTakeRefs_MultipleAck_UnknownTag(t *testing.T) {
	ch := newTestChannelState()
	ch.inFlight[100] = deliveryRef{queueName: "q1", storeTag: 50, consumerTag: "c1"}

	_, err := ch.takeRefs(99, true)
	if err == nil {
		t.Fatal("expected error for unknown delivery tag")
	}
}

func TestTakeRefs_SingleAck_UnknownTag(t *testing.T) {
	ch := newTestChannelState()

	_, err := ch.takeRefs(42, false)
	if err == nil {
		t.Fatal("expected error for unknown delivery tag")
	}
}

func TestAckRefs_MultipleAckZero_AcksAll(t *testing.T) {
	ch := newTestChannelState()
	ch.consumers["c1"] = &consumerState{tag: "c1", queueName: "q1", unacked: 3}
	ch.inFlight[1] = deliveryRef{queueName: "q1", storeTag: 10, consumerTag: "c1"}
	ch.inFlight[2] = deliveryRef{queueName: "q2", storeTag: 20, consumerTag: "c1"}
	ch.inFlight[3] = deliveryRef{queueName: "q1", storeTag: 30, consumerTag: "c1"}

	refs, err := ch.ackRefs(0, true)
	if err != nil {
		t.Fatalf("ackRefs(0, true): %v", err)
	}
	if len(refs) != 3 {
		t.Fatalf("expected 3 refs, got %d", len(refs))
	}
	if len(ch.inFlight) != 0 {
		t.Fatalf("expected 0 in-flight, got %d", len(ch.inFlight))
	}
	if ch.consumers["c1"].unacked != 0 {
		t.Fatalf("expected consumer unacked=0, got %d", ch.consumers["c1"].unacked)
	}
}

func TestNackRefs_DelegatesToTakeRefs(t *testing.T) {
	ch := newTestChannelState()
	ch.inFlight[1] = deliveryRef{queueName: "q1", storeTag: 10, consumerTag: "c1"}

	refs, err := ch.nackRefs(1, false)
	if err != nil {
		t.Fatalf("nackRefs(1, false): %v", err)
	}
	if len(refs) != 1 {
		t.Fatalf("expected 1 ref, got %d", len(refs))
	}
	if refs[0].queueName != "q1" {
		t.Fatalf("expected queueName=q1, got %q", refs[0].queueName)
	}
	if len(ch.inFlight) != 0 {
		t.Fatalf("expected 0 in-flight, got %d", len(ch.inFlight))
	}
}

func TestReserveDelivery_SequentialTags(t *testing.T) {
	ch := newTestChannelState()

	tag1, _ := ch.reserveDelivery("c1", "q1", 1, store.Message{}, false)
	tag2, _ := ch.reserveDelivery("c1", "q1", 2, store.Message{}, false)
	tag3, _ := ch.reserveDelivery("c1", "q1", 3, store.Message{}, false)

	if tag1 != 1 || tag2 != 2 || tag3 != 3 {
		t.Fatalf("expected tags 1,2,3 got %d,%d,%d", tag1, tag2, tag3)
	}
}
