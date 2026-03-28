package broker

import "fmt"

type ExchangeType string

const (
	ExchangeDirect  ExchangeType = "direct"
	ExchangeTopic   ExchangeType = "topic"
	ExchangeFanout  ExchangeType = "fanout"
	exchangeHeaders ExchangeType = "headers"
)

type Exchange struct {
	Name       string
	Type       ExchangeType
	Durable    bool
	AutoDelete bool
	Internal   bool
}

func newExchange(name string, kind ExchangeType, durable, autoDelete, internal bool) *Exchange {
	return &Exchange{
		Name:       name,
		Type:       kind,
		Durable:    durable,
		AutoDelete: autoDelete,
		Internal:   internal,
	}
}

func ParseExchangeType(value string) (ExchangeType, error) {
	switch ExchangeType(value) {
	case ExchangeDirect, ExchangeFanout, ExchangeTopic:
		return ExchangeType(value), nil
	case exchangeHeaders:
		return "", fmt.Errorf("exchange type %q is not supported yet", value)
	default:
		return "", fmt.Errorf("unsupported exchange type %q", value)
	}
}
