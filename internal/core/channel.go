package core

import "erionn-mq/internal/store"

type Channel struct {
	ID                    uint16
	Connection            *Connection
	PrefetchCount         int
	ConsumerPrefetchCount int
	Consumers             map[string]*ConsumerSubscription
}

type ConsumerSubscription struct {
	Tag     string
	Queue   string
	Channel *Channel
	AutoAck bool
	Deliver func(msg store.Message)
}
