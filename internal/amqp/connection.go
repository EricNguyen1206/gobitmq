package amqp

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"

	"erionn-mq/internal/broker"
	"erionn-mq/internal/store"
)

type serverConn struct {
	server   *Server
	broker   *broker.Broker
	netConn  net.Conn
	amqpConn *broker.Connection

	writeMu  sync.Mutex
	mu       sync.Mutex
	channels map[uint16]*channelState
	frameMax uint32

	closed atomic.Bool
	done   chan struct{}
}

func (c *serverConn) serve() error {
	if err := ReadProtocolHeader(c.netConn); err != nil {
		return err
	}

	if err := c.sendMethod(0, c.startMethod()); err != nil {
		return err
	}

	for {
		frame, err := ReadFrame(c.netConn, c.frameMax)
		if err != nil {
			return err
		}

		switch frame.Type {
		case FrameHeartbeat:
			continue
		case FrameMethod:
			method, err := DecodeMethodFrame(frame)
			if err != nil {
				return err
			}
			if err := c.handleMethod(frame.Channel, method); err != nil {
				return err
			}
		default:
			return fmt.Errorf("amqp: unexpected frame type %d", frame.Type)
		}
	}
}

func (c *serverConn) close() {
	if !c.closed.CompareAndSwap(false, true) {
		return
	}

	close(c.done)

	c.mu.Lock()
	channels := make([]*channelState, 0, len(c.channels))
	for _, ch := range c.channels {
		channels = append(channels, ch)
	}
	c.channels = make(map[uint16]*channelState)
	c.mu.Unlock()

	for _, ch := range channels {
		ch.stopAllConsumers()
	}

	_ = c.netConn.Close()

	c.server.mu.Lock()
	delete(c.server.connections, c.amqpConn.ID)
	c.server.mu.Unlock()
}

func (c *serverConn) startMethod() ConnectionStart {
	return ConnectionStart{
		VersionMajor: 0,
		VersionMinor: 9,
		ServerProperties: Table{
			"product":     "erionn-mq",
			"version":     "0.2.0",
			"platform":    "Go",
			"information": "EriOnn-MQ AMQP 0-9-1 MVP",
			"capabilities": Table{
				"authentication_failure_close": false,
				"basic.nack":                   true,
				"connection.blocked":           false,
				"consumer_cancel_notify":       false,
				"exchange_exchange_bindings":   false,
				"publisher_confirms":           true,
			},
		},
		Mechanisms: "PLAIN",
		Locales:    defaultLocale,
	}
}

func (c *serverConn) handleMethod(channel uint16, method Method) error {
	switch m := method.(type) {
	case ConnectionStartOk:
		return c.handleConnectionStartOk(m)
	case ConnectionTuneOk:
		return c.handleConnectionTuneOk(m)
	case ConnectionOpen:
		return c.handleConnectionOpen(m)
	case ConnectionClose:
		if err := c.sendMethod(0, ConnectionCloseOk{}); err != nil {
			return err
		}
		return errConnectionClosed
	case ChannelOpen:
		return c.handleChannelOpen(channel)
	case ChannelClose:
		return c.handleChannelClose(channel)
	case ExchangeDeclare:
		return c.handleExchangeDeclare(channel, m)
	case QueueDeclare:
		return c.handleQueueDeclare(channel, m)
	case QueueBind:
		return c.handleQueueBind(channel, m)
	case BasicPublish:
		return c.handleBasicPublish(channel, m)
	case BasicQos:
		return c.handleBasicQos(channel, m)
	case BasicConsume:
		return c.handleBasicConsume(channel, m)
	case BasicCancel:
		return c.handleBasicCancel(channel, m)
	case BasicAck:
		return c.handleBasicAck(channel, m)
	case BasicNack:
		return c.handleBasicNack(channel, m)
	case BasicReject:
		return c.handleBasicReject(channel, m)
	case ConfirmSelect:
		return c.handleConfirmSelect(channel, m)
	default:
		return fmt.Errorf("amqp: unsupported runtime method %T", method)
	}
}

func (c *serverConn) handleConnectionStartOk(m ConnectionStartOk) error {
	if strings.ToUpper(m.Mechanism) != "PLAIN" {
		return fmt.Errorf("amqp: unsupported auth mechanism %q", m.Mechanism)
	}
	if !strings.Contains(string(m.Response), "\x00") {
		return fmt.Errorf("amqp: invalid PLAIN auth response")
	}

	return c.sendMethod(0, ConnectionTune{
		ChannelMax: 0,
		FrameMax:   defaultFrameMax,
		Heartbeat:  0,
	})
}

func (c *serverConn) handleConnectionTuneOk(m ConnectionTuneOk) error {
	if m.FrameMax != 0 {
		c.frameMax = m.FrameMax
	}
	return nil
}

func (c *serverConn) handleConnectionOpen(m ConnectionOpen) error {
	c.amqpConn.VHost = m.VirtualHost
	return c.sendMethod(0, ConnectionOpenOk{})
}

func (c *serverConn) sendMethod(channel uint16, method Method) error {
	frame, err := EncodeMethodFrame(channel, method)
	if err != nil {
		return err
	}
	return c.writeFrames(frame)
}

func (c *serverConn) sendDelivery(channel uint16, consumerTag string, deliveryTag uint64, msg store.Message) error {
	deliverFrame, err := EncodeMethodFrame(channel, BasicDeliver{
		ConsumerTag: consumerTag,
		DeliveryTag: deliveryTag,
		Redelivered: msg.Redelivered,
		Exchange:    msg.Exchange,
		RoutingKey:  msg.RoutingKey,
	})
	if err != nil {
		return err
	}

	headerFrame, err := EncodeContentHeaderFrame(channel, ContentHeader{
		ClassID:  classBasic,
		BodySize: uint64(len(msg.Body)),
		Properties: BasicProperties{
			ContentType:   msg.ContentType,
			CorrelationID: msg.CorrelationID,
			ReplyTo:       msg.ReplyTo,
			DeliveryMode:  msg.DeliveryMode,
			Headers:       Table(msg.Headers),
		},
	})
	if err != nil {
		return err
	}

	frames := []Frame{deliverFrame, headerFrame}
	maxPayload := len(msg.Body)
	if c.frameMax > 8 {
		maxPayload = int(c.frameMax - 8)
	}
	if maxPayload <= 0 {
		maxPayload = len(msg.Body)
	}
	for start := 0; start < len(msg.Body); start += maxPayload {
		end := start + maxPayload
		if end > len(msg.Body) {
			end = len(msg.Body)
		}
		frames = append(frames, Frame{Type: FrameBody, Channel: channel, Payload: append([]byte(nil), msg.Body[start:end]...)})
	}

	return c.writeFrames(frames...)
}

func (c *serverConn) writeFrames(frames ...Frame) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	for _, frame := range frames {
		if err := WriteFrame(c.netConn, frame); err != nil {
			return err
		}
	}
	return nil
}

func (c *serverConn) readPublishedContent(channel uint16) (ContentHeader, []byte, error) {
	headerFrame, err := ReadFrame(c.netConn, c.frameMax)
	if err != nil {
		return ContentHeader{}, nil, err
	}
	if headerFrame.Type != FrameHeader || headerFrame.Channel != channel {
		return ContentHeader{}, nil, fmt.Errorf("amqp: expected content header on channel %d", channel)
	}

	header, err := DecodeContentHeaderFrame(headerFrame)
	if err != nil {
		return ContentHeader{}, nil, err
	}

	const MAX_BODY_SIZE = 256 * 1024 * 1024
	if header.BodySize > MAX_BODY_SIZE {
		return ContentHeader{}, nil, fmt.Errorf("amqp: body size %d exceeds maximum %d", header.BodySize, MAX_BODY_SIZE)
	}
	if header.BodySize > uint64(maxAllocSize) {
		return ContentHeader{}, nil, fmt.Errorf("amqp: body size %d exceeds supported allocation size", header.BodySize)
	}

	bodyCap := int(header.BodySize)
	if c.frameMax > 0 && header.BodySize > uint64(c.frameMax) {
		bodyCap = int(c.frameMax)
	}
	body := make([]byte, 0, bodyCap)
	for uint64(len(body)) < header.BodySize {
		frame, err := ReadFrame(c.netConn, c.frameMax)
		if err != nil {
			return ContentHeader{}, nil, err
		}
		if frame.Type == FrameHeartbeat {
			continue
		}
		if frame.Type != FrameBody || frame.Channel != channel {
			return ContentHeader{}, nil, fmt.Errorf("amqp: expected body frame on channel %d", channel)
		}
		remaining := header.BodySize - uint64(len(body))
		if uint64(len(frame.Payload)) > remaining {
			return ContentHeader{}, nil, fmt.Errorf("amqp: body frame size %d exceeds remaining body bytes %d", len(frame.Payload), remaining)
		}
		body = append(body, frame.Payload...)
	}

	return header, body, nil
}
