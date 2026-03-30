package amqp

import (
	"fmt"
	"time"
)

func (c *serverConn) handleBasicConsumeRequest(channel uint16, m BasicConsumeRequest) error {
	ch, err := c.requireChannel(channel)
	if err != nil {
		return err
	}
	if _, err := c.broker.GetQueue(m.Queue); err != nil {
		return err
	}

	tag := m.ConsumerTag
	if tag == "" {
		tag = fmt.Sprintf("ctag-%d-%d", channel, c.server.nextConsumerID.Add(1))
	}

	consumer := &consumerState{
		tag:       tag,
		queueName: m.Queue,
		autoAck:   m.NoAck,
		stop:      make(chan struct{}),
	}

	if err := ch.addConsumerToChannel(consumer); err != nil {
		return err
	}

	go c.consumeLoop(channel, ch, consumer)

	if m.NoWait {
		return nil
	}
	return c.sendMethod(channel, BasicConsumeResponse{ConsumerTag: tag})
}

func (c *serverConn) handleBasicCancelRequest(channel uint16, m BasicCancelRequest) error {
	ch, err := c.requireChannel(channel)
	if err != nil {
		return err
	}

	consumer, ok := ch.removeConsumerFromChannel(m.ConsumerTag)
	if !ok {
		return fmt.Errorf("amqp: consumer %q not found", m.ConsumerTag)
	}
	consumer.stopConsuming()

	if m.NoWait {
		return nil
	}
	return c.sendMethod(channel, BasicCancelResponse{ConsumerTag: m.ConsumerTag})
}

func (c *serverConn) handleBasicQosRequest(channel uint16, m BasicQosRequest) error {
	ch, err := c.requireChannel(channel)
	if err != nil {
		return err
	}

	ch.mu.Lock()
	if m.Global {
		ch.channel.PrefetchCount = int(m.PrefetchCount)
	} else {
		ch.channel.ConsumerPrefetchCount = int(m.PrefetchCount)
	}
	ch.mu.Unlock()

	return c.sendMethod(channel, BasicQosResponse{})
}

func (c *serverConn) consumeLoop(channelID uint16, ch *channelState, consumer *consumerState) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-c.done:
			return
		case <-consumer.stop:
			return
		default:
		}

		if !ch.canDispatch(consumer.tag) {
			select {
			case <-c.done:
				return
			case <-consumer.stop:
				return
			case <-ticker.C:
			}
			continue
		}

		queue, err := c.broker.GetQueue(consumer.queueName)
		if err != nil {
			return
		}

		msg, ok, err := queue.Dequeue()
		if err != nil {
			select {
			case <-c.done:
				return
			case <-consumer.stop:
				return
			case <-ticker.C:
			}
			continue
		}
		if !ok {
			select {
			case <-c.done:
				return
			case <-consumer.stop:
				return
			case <-ticker.C:
			}
			continue
		}

		select {
		case <-c.done:
			_ = queue.Nack(msg.DeliveryTag, true)
			return
		case <-consumer.stop:
			_ = queue.Nack(msg.DeliveryTag, true)
			return
		default:
		}

		deliveryTag, reserved := ch.reserveDelivery(consumer.tag, consumer.queueName, msg.DeliveryTag, msg, consumer.autoAck)

		select {
		case <-c.done:
			if reserved {
				ch.releaseDelivery(deliveryTag)
			}
			_ = queue.Nack(msg.DeliveryTag, true)
			return
		case <-consumer.stop:
			if reserved {
				ch.releaseDelivery(deliveryTag)
			}
			_ = queue.Nack(msg.DeliveryTag, true)
			return
		default:
		}

		if err := c.sendDelivery(channelID, consumer.tag, deliveryTag, msg); err != nil {
			if reserved {
				ch.releaseDelivery(deliveryTag)
			}
			_ = queue.Nack(msg.DeliveryTag, true)
			return
		}

		c.broker.RecordDeliver()
		if msg.Redelivered {
			c.broker.RecordRedeliver()
		}

		if consumer.autoAck {
			_ = queue.Ack(msg.DeliveryTag)
			c.broker.RecordAck()
		}
	}
}

func (ch *channelState) canDispatch(consumerTag string) bool {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	if ch.channel.PrefetchCount > 0 && len(ch.inFlight) >= ch.channel.PrefetchCount {
		return false
	}
	if ch.channel.ConsumerPrefetchCount <= 0 {
		return true
	}
	consumer, ok := ch.consumers[consumerTag]
	if !ok {
		return true
	}
	return consumer.unacked < ch.channel.ConsumerPrefetchCount
}
