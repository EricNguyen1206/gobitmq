package amqpcore

import (
	"sort"
	"time"

	"erionn-mq/internal/store"
)

type BrokerSnapshot struct {
	GeneratedAt  time.Time      `json:"generated_at"`
	MessageStats MessageStats   `json:"message_stats"`
	Exchanges    []ExchangeView `json:"exchanges"`
	Queues       []QueueView    `json:"queues"`
	Bindings     []BindingView  `json:"bindings"`
}

type ExchangeView struct {
	Name       string         `json:"name"`
	VHost      string         `json:"vhost"`
	Type       string         `json:"type"`
	Durable    bool           `json:"durable"`
	AutoDelete bool           `json:"auto_delete"`
	Internal   bool           `json:"internal"`
	Arguments  map[string]any `json:"arguments"`
}

type QueueView struct {
	Name          string         `json:"name"`
	VHost         string         `json:"vhost"`
	Durable       bool           `json:"durable"`
	AutoDelete    bool           `json:"auto_delete"`
	Exclusive     bool           `json:"exclusive"`
	Arguments     map[string]any `json:"arguments"`
	Messages      int            `json:"messages"`
	MessagesReady int            `json:"messages_ready"`
	MessagesUnack int            `json:"messages_unacknowledged"`
	Consumers     int            `json:"consumers"`
}

type BindingView struct {
	Source          string         `json:"source"`
	Destination     string         `json:"destination"`
	DestinationType string         `json:"destination_type"`
	RoutingKey      string         `json:"routing_key"`
	Arguments       map[string]any `json:"arguments"`
	VHost           string         `json:"vhost"`
}

func (b *Broker) Snapshot() BrokerSnapshot {
	b.mu.RLock()
	defer b.mu.RUnlock()

	exchanges := make([]ExchangeView, 0, len(b.exchanges))
	for _, ex := range b.exchanges {
		exchanges = append(exchanges, ExchangeView{
			Name:       ex.Name,
			VHost:      "/",
			Type:       string(ex.Type),
			Durable:    ex.Durable,
			AutoDelete: ex.AutoDelete,
			Internal:   ex.Internal,
			Arguments:  map[string]any{},
		})
	}
	queues := make([]QueueView, 0, len(b.queues))
	for _, q := range b.queues {
		stats := q.store.Stats()
		queues = append(queues, QueueView{
			Name:          q.Name,
			VHost:         "/",
			Durable:       q.Durable,
			AutoDelete:    q.AutoDelete,
			Exclusive:     q.Exclusive,
			Arguments:     store.CloneTable(q.Args),
			Messages:      stats.Ready + stats.Unacked,
			MessagesReady: stats.Ready,
			MessagesUnack: stats.Unacked,
			Consumers:     0,
		})
	}
	bindings := make([]BindingView, 0, len(b.bindings))
	for _, bind := range b.bindings {
		bindings = append(bindings, BindingView{
			Source:          bind.ExchangeName,
			Destination:     bind.QueueName,
			DestinationType: "queue",
			RoutingKey:      bind.RoutingKey,
			Arguments:       store.CloneTable(bind.Args),
			VHost:           "/",
		})
	}

	sort.Slice(exchanges, func(i, j int) bool { return exchanges[i].Name < exchanges[j].Name })
	sort.Slice(queues, func(i, j int) bool { return queues[i].Name < queues[j].Name })
	sort.Slice(bindings, func(i, j int) bool {
		if bindings[i].Source == bindings[j].Source {
			if bindings[i].Destination == bindings[j].Destination {
				return bindings[i].RoutingKey < bindings[j].RoutingKey
			}
			return bindings[i].Destination < bindings[j].Destination
		}
		return bindings[i].Source < bindings[j].Source
	})

	return BrokerSnapshot{
		GeneratedAt:  time.Now().UTC(),
		MessageStats: b.snapshotStats(),
		Exchanges:    exchanges,
		Queues:       queues,
		Bindings:     bindings,
	}
}
