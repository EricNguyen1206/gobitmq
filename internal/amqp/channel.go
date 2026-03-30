package amqp

import (
	"fmt"
	"sync"
	"sync/atomic"

	"erionn-mq/internal/broker"
	"erionn-mq/internal/store"
)

type channelState struct {
	broker  *broker.Broker
	channel *broker.Channel

	mu              sync.Mutex
	nextDeliveryTag uint64
	nextPublishSeq  uint64
	confirmMode     bool
	inFlight        map[uint64]deliveryRef
	consumers       map[string]*consumerState
}

type deliveryRef struct {
	queueName   string
	storeTag    uint64
	consumerTag string
	msg         store.Message
}

type consumerState struct {
	tag       string
	queueName string
	autoAck   bool
	unacked   int
	stop      chan struct{}
	stopped   atomic.Bool
}

func (c *serverConn) requireChannel(id uint16) (*channelState, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	ch, ok := c.channels[id]
	if !ok {
		return nil, fmt.Errorf("amqp: channel %d is not open", id)
	}
	return ch, nil
}

func (c *serverConn) removeChannel(id uint16) (*channelState, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	ch, ok := c.channels[id]
	if !ok {
		return nil, fmt.Errorf("amqp: channel %d is not open", id)
	}
	delete(c.channels, id)
	return ch, nil
}

func (c *serverConn) handleChanOpenRequest(id uint16) error {
	if id == 0 {
		return fmt.Errorf("amqp: channel 0 is reserved for connection methods")
	}

	c.mu.Lock()
	if _, exists := c.channels[id]; !exists {
		c.channels[id] = &channelState{
			broker: c.broker,
			channel: &broker.Channel{
				ID:         id,
				Connection: c.amqpConn,
				Consumers:  make(map[string]*broker.ConsumerSubscription),
			},
			nextDeliveryTag: 1,
			nextPublishSeq:  1,
			inFlight:        make(map[uint64]deliveryRef),
			consumers:       make(map[string]*consumerState),
		}
	}
	c.mu.Unlock()

	return c.sendMethod(id, ChanOpenResponse{})
}

func (c *serverConn) handleChanCloseRequest(id uint16) error {
	ch, err := c.removeChannel(id)
	if err != nil {
		return err
	}
	ch.stopAllConsumers()
	return c.sendMethod(id, ChanCloseResponse{})
}

func (ch *channelState) addConsumerToChannel(consumer *consumerState) error {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	if _, exists := ch.consumers[consumer.tag]; exists {
		return fmt.Errorf("amqp: consumer %q already exists", consumer.tag)
	}
	ch.consumers[consumer.tag] = consumer
	ch.channel.Consumers[consumer.tag] = &broker.ConsumerSubscription{
		Tag:     consumer.tag,
		Queue:   consumer.queueName,
		Channel: ch.channel,
		AutoAck: consumer.autoAck,
	}
	return nil
}

func (ch *channelState) removeConsumerFromChannel(tag string) (*consumerState, bool) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	consumer, ok := ch.consumers[tag]
	if !ok {
		return nil, false
	}
	delete(ch.consumers, tag)
	delete(ch.channel.Consumers, tag)
	return consumer, true
}

func (ch *channelState) stopAllConsumers() {
	ch.mu.Lock()
	consumers := make([]*consumerState, 0, len(ch.consumers))
	for _, consumer := range ch.consumers {
		consumers = append(consumers, consumer)
	}
	// Copy inFlight messages to prevent race condition when stopping consumers
	refs := make([]deliveryRef, 0, len(ch.inFlight))
	for _, ref := range ch.inFlight {
		refs = append(refs, ref)
	}
	ch.consumers = make(map[string]*consumerState)
	ch.channel.Consumers = make(map[string]*broker.ConsumerSubscription)
	ch.inFlight = make(map[uint64]deliveryRef)
	for _, consumer := range consumers {
		consumer.stopConsuming() // close channel 'stop' to exit consumeLoop
	}
	ch.mu.Unlock()
	// Nack requeue all in-flight messages
	for _, ref := range refs {
		q, err := ch.broker.GetQueue(ref.queueName)
		if err != nil {
			continue
		}
		_ = q.Nack(ref.storeTag, true)
	}
}

func (ch *channelState) consumerCount(queueName string) uint32 {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	var count uint32
	for _, consumer := range ch.consumers {
		if consumer.queueName == queueName {
			count++
		}
	}
	return count
}

func (c *consumerState) stopConsuming() {
	if !c.stopped.CompareAndSwap(false, true) {
		return
	}
	close(c.stop)
}
