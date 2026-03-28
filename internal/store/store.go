package store

type MessageStore interface {
	Enqueue(msg Message) (deliveryTag uint64, err error)
	Dequeue() (Message, bool, error)
	Ack(deliveryTag uint64) error
	Nack(deliveryTag uint64, requeue bool) error
	Len() int
	Stats() QueueStats
}

type Message struct {
	DeliveryTag uint64
	Exchange    string
	RoutingKey  string

	ContentType   string
	CorrelationID string
	ReplyTo       string
	DeliveryMode  uint8
	Headers       map[string]any
	Body          []byte
	Redelivered   bool
}

type QueueStats struct {
	Ready   int
	Unacked int
}
