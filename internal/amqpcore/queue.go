package amqpcore

import (
	"erionn-mq/internal/store"
)

// Queue represents an AMQP queue entity.
type Queue struct {
	Name       string
	Durable    bool
	Exclusive  bool
	AutoDelete bool // delete when last consumer cancels
	Args       map[string]any

	store store.MessageStore
}

// newQueue constructs a Queue backed by the provided store.
func newQueue(name string, durable, exclusive, autoDelete bool, args map[string]any, s store.MessageStore) *Queue {
	return &Queue{
		Name:       name,
		Durable:    durable,
		Exclusive:  exclusive,
		AutoDelete: autoDelete,
		Args:       cloneArgs(args),
		store:      s,
	}
}

// Enqueue delivers a message into the queue's backing store.
func (q *Queue) Enqueue(msg store.Message) (uint64, error) {
	return q.store.Enqueue(msg)
}

// Dequeue removes and returns the next ready message.
func (q *Queue) Dequeue() (store.Message, bool, error) {
	return q.store.Dequeue()
}

// Ack acknowledges delivery of a message.
func (q *Queue) Ack(deliveryTag uint64) error {
	return q.store.Ack(deliveryTag)
}

// Nack negatively acknowledges a message.
func (q *Queue) Nack(deliveryTag uint64, requeue bool) error {
	return q.store.Nack(deliveryTag, requeue)
}

// Len returns the number of ready messages.
func (q *Queue) Len() int {
	return q.store.Len()
}
