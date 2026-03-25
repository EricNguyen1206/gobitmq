package amqpcore

type Channel struct {
	ID                    uint16
	Connection            *Connection
	PrefetchCount         int
	ConsumerPrefetchCount int
	Consumers             map[string]*ConsumerSubscription
}

func newChannel(id uint16, conn *Connection) *Channel {
	return &Channel{
		ID:         id,
		Connection: conn,
		Consumers:  make(map[string]*ConsumerSubscription),
	}
}

type ConsumerSubscription struct {
	Tag     string
	Queue   string
	Channel *Channel
	AutoAck bool
	Deliver func(msg Message)
}
