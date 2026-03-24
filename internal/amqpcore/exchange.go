// Package amqpcore contains the AMQP broker domain model.
// It is purely in-process logic — no wire protocol knowledge here.
package amqpcore

// ExchangeType enumerates the supported exchange routing algorithms.
type ExchangeType string

const (
	ExchangeDirect  ExchangeType = "direct"
	ExchangeTopic   ExchangeType = "topic"
	ExchangeFanout  ExchangeType = "fanout"
	ExchangeHeaders ExchangeType = "headers" // reserved, not yet routed
)

// Exchange represents an AMQP exchange entity.
type Exchange struct {
	Name       string
	Type       ExchangeType
	Durable    bool
	AutoDelete bool // delete when last binding is removed
	Internal   bool // cannot be published to directly by clients
}

// newExchange constructs an Exchange. Use the broker's DeclareExchange instead.
func newExchange(name string, kind ExchangeType, durable, autoDelete, internal bool) *Exchange {
	return &Exchange{
		Name:       name,
		Type:       kind,
		Durable:    durable,
		AutoDelete: autoDelete,
		Internal:   internal,
	}
}
