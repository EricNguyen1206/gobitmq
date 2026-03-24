package store

import (
	"fmt"
	"sync"
)

// MemoryMessageStore is an in-memory implementation of MessageStore.
//
// Ready messages live in a FIFO queue. Unacked messages are held in a
// separate map keyed by delivery tag so that Ack/Nack can find them in O(1).
type MemoryMessageStore struct {
	mu      sync.Mutex
	nextTag uint64
	ready   []Message          // FIFO queue of messages ready for delivery
	unacked map[uint64]Message // messages awaiting Ack/Nack
}

// NewMemoryMessageStore creates an empty in-memory store.
func NewMemoryMessageStore() *MemoryMessageStore {
	return &MemoryMessageStore{
		nextTag: 1,
		ready:   make([]Message, 0),
		unacked: make(map[uint64]Message),
	}
}

// Enqueue appends msg to the ready queue and assigns a delivery tag.
func (s *MemoryMessageStore) Enqueue(msg Message) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	msg = cloneMessage(msg)
	msg.DeliveryTag = s.nextTag
	s.nextTag++
	s.ready = append(s.ready, msg)
	return msg.DeliveryTag, nil
}

// Dequeue removes the head of the ready queue, moves it to unacked, and
// returns it. Returns (zero, false, nil) when the queue is empty.
func (s *MemoryMessageStore) Dequeue() (Message, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.ready) == 0 {
		return Message{}, false, nil
	}

	msg := s.ready[0]
	s.ready = s.ready[1:]
	s.unacked[msg.DeliveryTag] = msg
	return cloneMessage(msg), true, nil
}

// Ack removes an unacked message permanently.
func (s *MemoryMessageStore) Ack(deliveryTag uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.unacked[deliveryTag]; !ok {
		return fmt.Errorf("unknown delivery tag %d", deliveryTag)
	}
	delete(s.unacked, deliveryTag)
	return nil
}

// Nack negatively acknowledges a message.
// If requeue is true the message is prepended to the ready queue (AMQP semantics:
// "the message is requeued to the same position if possible").
// If requeue is false the message is simply discarded.
func (s *MemoryMessageStore) Nack(deliveryTag uint64, requeue bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	msg, ok := s.unacked[deliveryTag]
	if !ok {
		return fmt.Errorf("unknown delivery tag %d", deliveryTag)
	}
	delete(s.unacked, deliveryTag)

	if requeue {
		msg.Redelivered = true
		// Prepend so the message is retried before new messages (head-of-line).
		s.ready = append([]Message{msg}, s.ready...)
	}
	return nil
}

// Len returns the count of ready (not-yet-delivered) messages.
func (s *MemoryMessageStore) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.ready)
}

// Stats returns ready/unacked counts.
func (s *MemoryMessageStore) Stats() QueueStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return QueueStats{Ready: len(s.ready), Unacked: len(s.unacked)}
}
