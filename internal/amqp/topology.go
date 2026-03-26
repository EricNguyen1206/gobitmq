package amqp

import (
	"fmt"

	"erionn-mq/internal/core"
)

func (c *serverConn) handleExchangeDeclare(channel uint16, m ExchangeDeclare) error {
	if _, err := c.requireChannel(channel); err != nil {
		return err
	}

	kind, err := core.ParseExchangeType(m.Type)
	if err != nil {
		return err
	}

	if m.Passive {
		if _, err := c.broker.GetExchange(m.Exchange); err != nil {
			return err
		}
	} else {
		if _, err := c.broker.DeclareExchange(m.Exchange, kind, m.Durable, m.AutoDelete, m.Internal); err != nil {
			return err
		}
	}

	if m.NoWait {
		return nil
	}
	return c.sendMethod(channel, ExchangeDeclareOk{})
}

func (c *serverConn) handleQueueDeclare(channel uint16, m QueueDeclare) error {
	if _, err := c.requireChannel(channel); err != nil {
		return err
	}

	queueName := m.Queue
	if queueName == "" {
		queueName = fmt.Sprintf("amq.gen-%d", c.server.nextQueueID.Add(1))
	}

	var q *core.Queue
	var err error
	if m.Passive {
		q, err = c.broker.GetQueue(queueName)
	} else {
		q, err = c.broker.DeclareQueue(queueName, m.Durable, m.Exclusive, m.AutoDelete, map[string]any(m.Arguments))
	}
	if err != nil {
		return err
	}

	if m.NoWait {
		return nil
	}

	return c.sendMethod(channel, QueueDeclareOk{
		Queue:         q.Name,
		MessageCount:  uint32(q.Len()),
		ConsumerCount: c.server.consumerCount(q.Name),
	})
}

func (c *serverConn) handleQueueBind(channel uint16, m QueueBind) error {
	if _, err := c.requireChannel(channel); err != nil {
		return err
	}
	if m.Exchange == "" {
		return fmt.Errorf("amqp: cannot bind the default exchange")
	}
	if err := c.broker.BindQueue(m.Exchange, m.Queue, m.RoutingKey, map[string]any(m.Arguments)); err != nil {
		return err
	}
	if m.NoWait {
		return nil
	}
	return c.sendMethod(channel, QueueBindOk{})
}
