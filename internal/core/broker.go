package core

import (
	"errors"
	"fmt"
	"reflect"
	"sync"

	"erionn-mq/internal/store"
)

type Broker struct {
	mu           sync.RWMutex
	exchanges    map[string]*Exchange
	queues       map[string]*Queue
	bindings     []*Binding
	metrics      brokerMetrics
	storeFactory QueueStoreFactory
	metadataPath string
}

type QueueConfig struct {
	Name       string
	Durable    bool
	Exclusive  bool
	AutoDelete bool
	Args       map[string]any
}

type QueueStoreFactory func(QueueConfig) (store.MessageStore, error)

func NewBroker(storeFactory func() store.MessageStore) *Broker {
	if storeFactory == nil {
		storeFactory = func() store.MessageStore {
			return store.NewMemoryMessageStore()
		}
	}
	return NewBrokerWithQueueStoreFactory(func(QueueConfig) (store.MessageStore, error) {
		return storeFactory(), nil
	})
}

func NewBrokerWithQueueStoreFactory(storeFactory QueueStoreFactory) *Broker {
	if storeFactory == nil {
		storeFactory = func(QueueConfig) (store.MessageStore, error) {
			return store.NewMemoryMessageStore(), nil
		}
	}
	b := &Broker{
		exchanges:    make(map[string]*Exchange),
		queues:       make(map[string]*Queue),
		bindings:     make([]*Binding, 0),
		storeFactory: storeFactory,
	}
	b.registerDefaultExchanges()
	return b
}

func (b *Broker) registerDefaultExchanges() {
	defaults := []struct {
		name string
		kind ExchangeType
	}{
		{"", ExchangeDirect},
		{"amq.direct", ExchangeDirect},
		{"amq.topic", ExchangeTopic},
		{"amq.fanout", ExchangeFanout},
	}
	for _, d := range defaults {
		b.exchanges[d.name] = newExchange(d.name, d.kind, true, false, false)
	}
}

func (b *Broker) DeclareExchange(name string, kind ExchangeType, durable, autoDelete, internal bool) (*Exchange, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if ex, ok := b.exchanges[name]; ok {
		if ex.Type != kind || ex.Durable != durable || ex.AutoDelete != autoDelete || ex.Internal != internal {
			return nil, fmt.Errorf("exchange %q already declared with different attributes", name)
		}
		return ex, nil
	}
	ex := newExchange(name, kind, durable, autoDelete, internal)
	b.exchanges[name] = ex
	if err := b.persistMetadataLocked(); err != nil {
		return nil, err
	}
	return ex, nil
}

func (b *Broker) GetExchange(name string) (*Exchange, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	ex, ok := b.exchanges[name]
	if !ok {
		return nil, fmt.Errorf("exchange %q not found", name)
	}
	return ex, nil
}

func (b *Broker) DeclareQueue(name string, durable, exclusive, autoDelete bool, args map[string]any) (*Queue, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	args = store.CloneTable(args)
	if q, ok := b.queues[name]; ok {
		if q.Durable != durable || q.Exclusive != exclusive || q.AutoDelete != autoDelete || !sameArgs(q.Args, args) {
			return nil, fmt.Errorf("queue %q already declared with different attributes", name)
		}
		return q, nil
	}
	store, err := b.storeFactory(QueueConfig{
		Name:       name,
		Durable:    durable,
		Exclusive:  exclusive,
		AutoDelete: autoDelete,
		Args:       store.CloneTable(args),
	})
	if err != nil {
		return nil, err
	}
	q := newQueue(name, durable, exclusive, autoDelete, args, store)
	b.queues[name] = q
	if err := b.bindQueueLocked("", name, name, nil); err != nil {
		return nil, err
	}
	if err := b.persistMetadataLocked(); err != nil {
		return nil, err
	}
	return q, nil
}

func sameArgs(a, b map[string]any) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	return reflect.DeepEqual(a, b)
}

func (b *Broker) GetQueue(name string) (*Queue, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	q, ok := b.queues[name]
	if !ok {
		return nil, fmt.Errorf("queue %q not found", name)
	}
	return q, nil
}

func (b *Broker) DeleteQueue(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	q, ok := b.queues[name]
	if !ok {
		return fmt.Errorf("queue %q not found", name)
	}
	if destroyer, ok := q.store.(interface{ Destroy() error }); ok {
		if err := destroyer.Destroy(); err != nil {
			return err
		}
	}
	delete(b.queues, name)
	remaining := b.bindings[:0]
	for _, bind := range b.bindings {
		if bind.QueueName != name {
			remaining = append(remaining, bind)
		}
	}
	b.bindings = remaining
	if err := b.persistMetadataLocked(); err != nil {
		return err
	}
	return nil
}

func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	var errs []error
	for _, q := range b.queues {
		if closer, ok := q.store.(interface{ Close() error }); ok {
			if err := closer.Close(); err != nil {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

func (b *Broker) BindQueue(exchangeName, queueName, routingKey string, args map[string]any) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.bindQueueLocked(exchangeName, queueName, routingKey, store.CloneTable(args)); err != nil {
		return err
	}
	return b.persistMetadataLocked()
}

func (b *Broker) bindQueueLocked(exchangeName, queueName, routingKey string, args map[string]any) error {
	if _, ok := b.exchanges[exchangeName]; !ok {
		return fmt.Errorf("exchange %q not found", exchangeName)
	}
	if _, ok := b.queues[queueName]; !ok {
		return fmt.Errorf("queue %q not found", queueName)
	}
	for _, bind := range b.bindings {
		if bind.ExchangeName == exchangeName &&
			bind.QueueName == queueName &&
			bind.RoutingKey == routingKey {
			return nil
		}
	}
	b.bindings = append(b.bindings, &Binding{
		ExchangeName: exchangeName,
		QueueName:    queueName,
		RoutingKey:   routingKey,
		Args:         store.CloneTable(args),
	})
	return nil
}

func (b *Broker) Publish(exchangeName, routingKey string, msg store.Message) error {
	b.mu.RLock()
	ex, ok := b.exchanges[exchangeName]
	if !ok {
		b.mu.RUnlock()
		return fmt.Errorf("exchange %q not found", exchangeName)
	}
	b.RecordPublish()
	var targets []string
	for _, bind := range b.bindings {
		if bind.ExchangeName == exchangeName && bind.matches(ex.Type, routingKey) {
			targets = append(targets, bind.QueueName)
		}
	}
	b.mu.RUnlock()
	var errs []error
	for _, qName := range targets {
		b.mu.RLock()
		q, ok := b.queues[qName]
		b.mu.RUnlock()
		if !ok {
			continue
		}
		msgCopy := msg
		msgCopy.Exchange = exchangeName
		msgCopy.RoutingKey = routingKey
		if _, err := q.Enqueue(msgCopy); err != nil {
			errs = append(errs, fmt.Errorf("queue %q: %w", qName, err))
		}
	}
	return errors.Join(errs...)
}

func (b *Broker) Route(exchangeName, routingKey string) ([]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	ex, ok := b.exchanges[exchangeName]
	if !ok {
		return nil, fmt.Errorf("exchange %q not found", exchangeName)
	}
	var queues []string
	for _, bind := range b.bindings {
		if bind.ExchangeName == exchangeName && bind.matches(ex.Type, routingKey) {
			queues = append(queues, bind.QueueName)
		}
	}
	return queues, nil
}

func (b *Broker) DeadLetter(queueName string, msg store.Message, reason string) error {
	b.mu.RLock()
	q, ok := b.queues[queueName]
	if !ok {
		b.mu.RUnlock()
		return fmt.Errorf("queue %q not found", queueName)
	}
	args := store.CloneTable(q.Args)
	b.mu.RUnlock()
	rawExchange, ok := args["x-dead-letter-exchange"]
	if !ok {
		return nil
	}
	dlx, ok := rawExchange.(string)
	if !ok {
		return fmt.Errorf("queue %q has non-string x-dead-letter-exchange", queueName)
	}
	routingKey := msg.RoutingKey
	if rawRoutingKey, ok := args["x-dead-letter-routing-key"]; ok {
		dlxRoutingKey, ok := rawRoutingKey.(string)
		if !ok {
			return fmt.Errorf("queue %q has non-string x-dead-letter-routing-key", queueName)
		}
		routingKey = dlxRoutingKey
	}
	deadLetter := store.CloneMessage(msg)
	deadLetter.DeliveryTag = 0
	deadLetter.Redelivered = false
	if deadLetter.Headers == nil {
		deadLetter.Headers = make(map[string]any)
	}
	deadLetter.Headers["x-death-reason"] = reason
	deadLetter.Headers["x-death-source-queue"] = queueName
	return b.Publish(dlx, routingKey, deadLetter)
}
