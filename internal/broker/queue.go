package broker

import "erionn-mq/internal/store"

type Queue struct {
	Name       string
	Durable    bool
	Exclusive  bool
	AutoDelete bool
	Args       map[string]any
	store      store.MessageStore
}

func newQueue(name string, durable, exclusive, autoDelete bool, args map[string]any, s store.MessageStore) *Queue {
	return &Queue{
		Name:       name,
		Durable:    durable,
		Exclusive:  exclusive,
		AutoDelete: autoDelete,
		Args:       store.CloneTable(args),
		store:      s,
	}
}

func (q *Queue) Enqueue(msg store.Message) (uint64, error) {
	return q.store.Enqueue(msg)
}

func (q *Queue) Dequeue() (store.Message, bool, error) {
	return q.store.Dequeue()
}

func (q *Queue) Ack(deliveryTag uint64) error {
	return q.store.Ack(deliveryTag)
}

func (q *Queue) Nack(deliveryTag uint64, requeue bool) error {
	return q.store.Nack(deliveryTag, requeue)
}

func (q *Queue) Len() int {
	return q.store.Len()
}
