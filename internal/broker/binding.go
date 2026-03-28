package broker

import (
	"strings"
)

type Binding struct {
	ExchangeName string
	QueueName    string
	RoutingKey   string
	Args         map[string]any
}

func (b *Binding) matches(exchangeType ExchangeType, routingKey string) bool {
	switch exchangeType {
	case ExchangeDirect:
		return b.RoutingKey == routingKey
	case ExchangeFanout:
		return true
	case ExchangeTopic:
		return matchTopic(b.RoutingKey, routingKey)
	default:
		return false
	}
}

func matchTopic(pattern, key string) bool {
	patternParts := strings.Split(pattern, ".")
	keyParts := strings.Split(key, ".")
	return matchParts(patternParts, keyParts)
}

func matchParts(pattern, key []string) bool {
	if len(pattern) == 0 && len(key) == 0 {
		return true
	}
	if len(pattern) == 0 {
		return false
	}

	if pattern[0] == "#" {
		if matchParts(pattern[1:], key) {
			return true
		}
		if len(key) == 0 {
			return false
		}
		return matchParts(pattern, key[1:])
	}

	if len(key) == 0 {
		return false
	}

	if pattern[0] == "*" || pattern[0] == key[0] {
		return matchParts(pattern[1:], key[1:])
	}

	return false
}
