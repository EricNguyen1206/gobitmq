// Package store defines the MessageStore abstraction for AMQP queue storage.
// It is intentionally decoupled from AMQP wire details so that different
// backends (in-memory, WAL-based, etc.) can be plugged in without touching
// the broker or protocol layers.
package store

// MessageStore manages messages for a single queue.
// All methods must be safe for concurrent use.
type MessageStore interface {
	// Enqueue adds a message to the tail of the queue.
	// Returns the assigned delivery tag (monotonically increasing per store).
	Enqueue(msg Message) (deliveryTag uint64, err error)

	// Dequeue removes and returns the next ready message for delivery.
	// Returns (zero, false, nil) when the queue is empty.
	Dequeue() (Message, bool, error)

	// Ack acknowledges a delivered message, removing it permanently.
	Ack(deliveryTag uint64) error

	// Nack negatively acknowledges a message.
	// If requeue is true the message is re-inserted at the head of the queue;
	// otherwise it is discarded (dead-letter routing handled by the broker layer).
	Nack(deliveryTag uint64, requeue bool) error

	// Len returns the total number of ready messages (not including unacked ones).
	Len() int

	// Stats returns counts for ready and unacked messages.
	Stats() QueueStats
}

// Message is the unit stored and delivered by a queue.
// The DeliveryTag field is assigned by the store on Enqueue.
type Message struct {
	DeliveryTag uint64
	Exchange    string
	RoutingKey  string

	// Per-message properties (subset of AMQP Basic.Properties).
	ContentType   string
	CorrelationID string
	ReplyTo       string
	DeliveryMode  uint8 // 1 = transient, 2 = persistent
	Headers       map[string]any
	Body          []byte

	// internal: set to true when this is a redelivery
	Redelivered bool
}

// QueueStats summarizes queue depth for management/metrics.
type QueueStats struct {
	Ready   int
	Unacked int
}
