package amqpcore

// Channel represents an AMQP channel multiplexed over a single Connection.
// Phase 2 will add the full protocol state machine; for now it holds
// the fields that amqpcore routing / subscription logic needs.
type Channel struct {
	ID                    uint16
	Connection            *Connection
	PrefetchCount         int                              // QoS — max unacked messages per channel when global=true
	ConsumerPrefetchCount int                              // QoS — max unacked messages per consumer when global=false
	Consumers             map[string]*ConsumerSubscription // consumer tag → subscription
}

// newChannel creates a Channel attached to the given connection.
func newChannel(id uint16, conn *Connection) *Channel {
	return &Channel{
		ID:         id,
		Connection: conn,
		Consumers:  make(map[string]*ConsumerSubscription),
	}
}

// ConsumerSubscription represents a consumer registered via Basic.Consume.
type ConsumerSubscription struct {
	Tag     string // unique consumer tag within the channel
	Queue   string // queue name being consumed
	Channel *Channel
	AutoAck bool
	// Deliver is called by the broker for each message dispatched to this consumer.
	// It must be non-blocking or manage its own goroutine.
	Deliver func(msg Message)
}
