package amqp

import (
	"fmt"

	"erionn-mq/internal/store"
)

func (c *serverConn) handleBasicPublish(channel uint16, m BasicPublish) error {
	if _, err := c.requireChannel(channel); err != nil {
		return err
	}
	ex, err := c.broker.GetExchange(m.Exchange)
	if err != nil {
		return err
	}
	if ex.Internal {
		return fmt.Errorf("amqp: exchange %q is internal", m.Exchange)
	}

	header, body, err := c.readPublishedContent(channel)
	if err != nil {
		return err
	}
	if header.ClassID != classBasic {
		return fmt.Errorf("amqp: publish content header class=%d, want %d", header.ClassID, classBasic)
	}

	msg := store.Message{
		ContentType:   header.Properties.ContentType,
		CorrelationID: header.Properties.CorrelationID,
		ReplyTo:       header.Properties.ReplyTo,
		DeliveryMode:  header.Properties.DeliveryMode,
		Body:          body,
	}
	if len(header.Properties.Headers) > 0 {
		msg.Headers = map[string]any(header.Properties.Headers)
	}

	confirmSeq := c.nextConfirmSeq(channel)
	if err := c.broker.Publish(m.Exchange, m.RoutingKey, msg); err != nil {
		if confirmSeq > 0 {
			_ = c.sendMethod(channel, BasicNack{DeliveryTag: confirmSeq, Requeue: false})
		}
		return err
	}
	if confirmSeq > 0 {
		if err := c.sendMethod(channel, BasicAck{DeliveryTag: confirmSeq}); err != nil {
			return err
		}
	}
	return nil
}

func (c *serverConn) handleConfirmSelectRequest(channel uint16, m ConfirmSelectRequest) error {
	ch, err := c.requireChannel(channel)
	if err != nil {
		return err
	}

	ch.mu.Lock()
	ch.confirmMode = true
	if ch.nextPublishSeq == 0 {
		ch.nextPublishSeq = 1
	}
	ch.mu.Unlock()

	if m.NoWait {
		return nil
	}
	return c.sendMethod(channel, ConfirmSelectResponse{})
}

func (c *serverConn) nextConfirmSeq(channel uint16) uint64 {
	ch, err := c.requireChannel(channel)
	if err != nil {
		return 0
	}

	ch.mu.Lock()
	defer ch.mu.Unlock()
	if !ch.confirmMode {
		return 0
	}
	seq := ch.nextPublishSeq
	ch.nextPublishSeq++
	return seq
}
