package amqpcore

import (
	"errors"
	"fmt"
	"reflect"
	"sync"

	"erionn-mq/internal/store"
)

// Broker is the central coordinator for the AMQP domain model.
//
// It manages exchanges, queues, and bindings, and implements the
// routing logic that connects publishers to consumers.
// All public methods are safe for concurrent use.
type Broker struct {
	mu        sync.RWMutex
	exchanges map[string]*Exchange
	queues    map[string]*Queue
	bindings  []*Binding

	// storeFactory creates a fresh MessageStore for each newly declared queue.
	storeFactory func() store.MessageStore
}

// NewBroker creates a Broker pre-populated with the default AMQP exchanges.
//
// storeFactory is called once per DeclareQueue call; pass
// store.NewMemoryMessageStore for the in-memory implementation.
func NewBroker(storeFactory func() store.MessageStore) *Broker {
	b := &Broker{
		exchanges:    make(map[string]*Exchange),
		queues:       make(map[string]*Queue),
		bindings:     make([]*Binding, 0),
		storeFactory: storeFactory,
	}
	b.registerDefaultExchanges()
	return b
}

// registerDefaultExchanges pre-declares the four mandatory AMQP exchanges.
func (b *Broker) registerDefaultExchanges() {
	defaults := []struct {
		name string
		kind ExchangeType
	}{
		{"", ExchangeDirect}, // default direct exchange
		{"amq.direct", ExchangeDirect},
		{"amq.topic", ExchangeTopic},
		{"amq.fanout", ExchangeFanout},
	}
	for _, d := range defaults {
		b.exchanges[d.name] = newExchange(d.name, d.kind, true, false, false)
	}
}

// --- Exchange ---

// DeclareExchange creates or verifies an exchange.
// If the exchange already exists with the same attributes, it is a no-op (idempotent).
// Attempting to redeclare with different attributes returns an error.
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
	return ex, nil
}

// GetExchange returns the named exchange or an error if it does not exist.
func (b *Broker) GetExchange(name string) (*Exchange, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	ex, ok := b.exchanges[name]
	if !ok {
		return nil, fmt.Errorf("exchange %q not found", name)
	}
	return ex, nil
}

// --- Queue ---

// DeclareQueue creates or verifies a queue. Idempotent for matching attributes.
func (b *Broker) DeclareQueue(name string, durable, exclusive, autoDelete bool, args map[string]any) (*Queue, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if q, ok := b.queues[name]; ok {
		if q.Durable != durable || q.Exclusive != exclusive || q.AutoDelete != autoDelete || !reflect.DeepEqual(q.Args, args) {
			return nil, fmt.Errorf("queue %q already declared with different attributes", name)
		}
		return q, nil
	}

	q := newQueue(name, durable, exclusive, autoDelete, args, b.storeFactory())
	b.queues[name] = q
	b.bindQueueLocked("", name, name, nil)
	return q, nil
}

// GetQueue returns the named queue or an error if it does not exist.
func (b *Broker) GetQueue(name string) (*Queue, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	q, ok := b.queues[name]
	if !ok {
		return nil, fmt.Errorf("queue %q not found", name)
	}
	return q, nil
}

// DeleteQueue removes a queue and all of its bindings.
func (b *Broker) DeleteQueue(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.queues[name]; !ok {
		return fmt.Errorf("queue %q not found", name)
	}
	delete(b.queues, name)

	// Remove all bindings that reference this queue.
	remaining := b.bindings[:0]
	for _, bind := range b.bindings {
		if bind.QueueName != name {
			remaining = append(remaining, bind)
		}
	}
	b.bindings = remaining
	return nil
}

// --- Binding ---

// BindQueue creates a binding from exchange → queue with the given routing key.
// Idempotent: binding the same (exchange, queue, key) twice is a no-op.
func (b *Broker) BindQueue(exchangeName, queueName, routingKey string, args map[string]any) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.bindQueueLocked(exchangeName, queueName, routingKey, args)
}

func (b *Broker) bindQueueLocked(exchangeName, queueName, routingKey string, args map[string]any) error {
	if _, ok := b.exchanges[exchangeName]; !ok {
		return fmt.Errorf("exchange %q not found", exchangeName)
	}
	if _, ok := b.queues[queueName]; !ok {
		return fmt.Errorf("queue %q not found", queueName)
	}

	// Idempotency check.
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
		Args:         args,
	})
	return nil
}

// --- Routing / Publish ---

// Publish routes msg through the named exchange to all matching queues.
// Returns an error only for infrastructure failures (exchange not found, store error).
// Routing to zero queues is not an error — it follows the fire-and-forget
// semantics of AMQP Basic.Publish without mandatory/immediate flags.
func (b *Broker) Publish(exchangeName, routingKey string, msg Message) error {
	b.mu.RLock()
	ex, ok := b.exchanges[exchangeName]
	if !ok {
		b.mu.RUnlock()
		return fmt.Errorf("exchange %q not found", exchangeName)
	}

	// Collect matching bindings under the read lock.
	var targets []string
	for _, bind := range b.bindings {
		if bind.ExchangeName == exchangeName && bind.matches(ex.Type, routingKey) {
			targets = append(targets, bind.QueueName)
		}
	}
	b.mu.RUnlock()

	// Enqueue into each matched queue (outside the lock to avoid holding it
	// while calling potentially slow store operations).
	var errs []error
	for _, qName := range targets {
		b.mu.RLock()
		q, ok := b.queues[qName]
		b.mu.RUnlock()
		if !ok {
			continue // queue was deleted between lock releases — skip
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

// Route returns the names of all queues that the given exchange+routingKey
// would deliver to, without actually enqueuing any message.
// Useful for testing and management API inspection.
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
