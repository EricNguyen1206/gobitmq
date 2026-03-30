package amqp

import (
	"fmt"

	"erionn-mq/internal/broker"
)

func (c *serverConn) handleExchDeclareRequest(channel uint16, m ExchDeclareRequest) error {
	if _, err := c.requireChannel(channel); err != nil {
		return err
	}

	kind, err := broker.ParseExchangeType(m.Type)
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
	return c.sendMethod(channel, ExchDeclareResponse{})
}

func (c *serverConn) handleQueueDeclareRequest(channel uint16, m QueueDeclareRequest) error {
	if _, err := c.requireChannel(channel); err != nil {
		return err
	}

	queueName := m.Queue
	if queueName == "" {
		queueName = fmt.Sprintf("amq.gen-%d", c.server.nextQueueID.Add(1))
	}

	var q *broker.Queue
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

	return c.sendMethod(channel, QueueDeclareResponse{
		Queue:         q.Name,
		MessageCount:  uint32(q.Len()),
		ConsumerCount: c.server.consumerCount(q.Name),
	})
}

func (c *serverConn) handleQueueBindRequest(channel uint16, m QueueBindRequest) error {
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
	return c.sendMethod(channel, QueueBindResponse{})
}
