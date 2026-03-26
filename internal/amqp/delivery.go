package amqp

import (
	"errors"
	"fmt"

	"erionn-mq/internal/store"
)

func (c *serverConn) handleBasicAck(channel uint16, m BasicAck) error {
	ch, err := c.requireChannel(channel)
	if err != nil {
		return err
	}

	refs, err := ch.ackRefs(m.DeliveryTag, m.Multiple)
	if err != nil {
		return err
	}
	for _, ref := range refs {
		q, err := c.broker.GetQueue(ref.queueName)
		if err != nil {
			return err
		}
		if err := q.Ack(ref.storeTag); err != nil {
			return err
		}
		c.broker.RecordAck()
	}
	return nil
}

func (c *serverConn) handleBasicNack(channel uint16, m BasicNack) error {
	return c.handleNegativeAck(channel, m.DeliveryTag, m.Multiple, m.Requeue, "nack", c.broker.RecordNack)
}

func (c *serverConn) handleBasicReject(channel uint16, m BasicReject) error {
	return c.handleNegativeAck(channel, m.DeliveryTag, false, m.Requeue, "reject", c.broker.RecordReject)
}

func (c *serverConn) handleNegativeAck(channel uint16, deliveryTag uint64, multiple, requeue bool, reason string, record func()) error {
	ch, err := c.requireChannel(channel)
	if err != nil {
		return err
	}

	refs, err := ch.nackRefs(deliveryTag, multiple)
	if err != nil {
		return err
	}

	var errs []error
	for _, ref := range refs {
		q, err := c.broker.GetQueue(ref.queueName)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if err := q.Nack(ref.storeTag, requeue); err != nil {
			errs = append(errs, err)
			continue
		}
		record()
		if !requeue {
			if err := c.broker.DeadLetter(ref.queueName, ref.msg, reason); err != nil {
				errs = append(errs, err)
			}
		}
	}

	return errors.Join(errs...)
}

func (ch *channelState) reserveDelivery(consumerTag, queueName string, storeTag uint64, msg store.Message, autoAck bool) (uint64, bool) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	tag := ch.nextDeliveryTag
	ch.nextDeliveryTag++
	if !autoAck {
		if consumer, ok := ch.consumers[consumerTag]; ok {
			consumer.unacked++
		}
		ch.inFlight[tag] = deliveryRef{queueName: queueName, storeTag: storeTag, consumerTag: consumerTag, msg: msg}
	}
	return tag, !autoAck
}

func (ch *channelState) releaseDelivery(deliveryTag uint64) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	ref, ok := ch.inFlight[deliveryTag]
	if !ok {
		return
	}
	delete(ch.inFlight, deliveryTag)
	if consumer, ok := ch.consumers[ref.consumerTag]; ok && consumer.unacked > 0 {
		consumer.unacked--
	}
}

func (ch *channelState) ackRefs(deliveryTag uint64, multiple bool) ([]deliveryRef, error) {
	return ch.takeRefs(deliveryTag, multiple)
}

func (ch *channelState) nackRefs(deliveryTag uint64, multiple bool) ([]deliveryRef, error) {
	return ch.takeRefs(deliveryTag, multiple)
}

func (ch *channelState) takeRefs(deliveryTag uint64, multiple bool) ([]deliveryRef, error) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	if multiple {
		refs := make([]deliveryRef, 0)
		for tag, ref := range ch.inFlight {
			if deliveryTag == 0 || tag <= deliveryTag {
				refs = append(refs, ref)
				if consumer, ok := ch.consumers[ref.consumerTag]; ok && consumer.unacked > 0 {
					consumer.unacked--
				}
				delete(ch.inFlight, tag)
			}
		}
		if deliveryTag == 0 {
			return refs, nil
		}
		if len(refs) == 0 {
			return nil, fmt.Errorf("amqp: unknown delivery tag %d", deliveryTag)
		}
		return refs, nil
	}

	ref, ok := ch.inFlight[deliveryTag]
	if !ok {
		return nil, fmt.Errorf("amqp: unknown delivery tag %d", deliveryTag)
	}
	delete(ch.inFlight, deliveryTag)
	if consumer, ok := ch.consumers[ref.consumerTag]; ok && consumer.unacked > 0 {
		consumer.unacked--
	}
	return []deliveryRef{ref}, nil
}
