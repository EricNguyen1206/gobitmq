package store

import (
	"sync"
)

type MemoryMessageStore struct {
	mu    sync.Mutex
	state queueState
}

func NewMemoryMessageStore() *MemoryMessageStore {
	return &MemoryMessageStore{state: newQueueState()}
}

func (s *MemoryMessageStore) Enqueue(msg Message) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.enqueue(msg).DeliveryTag, nil
}

func (s *MemoryMessageStore) Dequeue() (Message, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	msg, ok := s.state.dequeue()
	return msg, ok, nil
}

func (s *MemoryMessageStore) Ack(deliveryTag uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.state.ack(deliveryTag)
	return err
}

func (s *MemoryMessageStore) Nack(deliveryTag uint64, requeue bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.state.nack(deliveryTag, requeue)
	return err
}

func (s *MemoryMessageStore) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.len()
}

func (s *MemoryMessageStore) Stats() QueueStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.stats()
}
