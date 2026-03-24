package amqp

import (
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"erionn-mq/internal/amqpcore"
	"erionn-mq/internal/store"
)

var errConnectionClosed = errors.New("amqp: connection closed")

type Server struct {
	Addr   string
	broker *amqpcore.Broker

	nextConnID  atomic.Uint64
	nextQueueID atomic.Uint64

	mu          sync.Mutex
	connections map[uint64]*serverConn
}

type serverConn struct {
	server   *Server
	broker   *amqpcore.Broker
	netConn  net.Conn
	amqpConn *amqpcore.Connection

	writeMu  sync.Mutex
	mu       sync.Mutex
	channels map[uint16]*channelState
	frameMax uint32

	closed atomic.Bool
	done   chan struct{}
}

type channelState struct {
	channel *amqpcore.Channel

	mu              sync.Mutex
	nextDeliveryTag uint64
	inFlight        map[uint64]deliveryRef
	consumers       map[string]*consumerState
}

type deliveryRef struct {
	queueName string
	storeTag  uint64
}

type consumerState struct {
	tag       string
	queueName string
	autoAck   bool
	stop      chan struct{}
	stopped   atomic.Bool
}

func NewServer(addr string, broker *amqpcore.Broker) *Server {
	if addr == "" {
		addr = DefaultAddr
	}
	if broker == nil {
		broker = amqpcore.NewBroker(func() store.MessageStore {
			return store.NewMemoryMessageStore()
		})
	}
	return &Server{
		Addr:        addr,
		broker:      broker,
		connections: make(map[uint64]*serverConn),
	}
}

func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return err
	}
	return s.Serve(ln)
}

func (s *Server) Serve(ln net.Listener) error {
	defer ln.Close()
	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(netConn net.Conn) {
	conn := s.newConn(netConn)
	defer conn.close()

	if err := conn.serve(); err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, errConnectionClosed) {
		_ = conn.netConn.Close()
	}
}

func (s *Server) newConn(netConn net.Conn) *serverConn {
	id := s.nextConnID.Add(1)
	conn := &serverConn{
		server:  s,
		broker:  s.broker,
		netConn: netConn,
		amqpConn: &amqpcore.Connection{
			ID:   id,
			Conn: netConn,
		},
		channels: make(map[uint16]*channelState),
		frameMax: defaultFrameMax,
		done:     make(chan struct{}),
	}

	s.mu.Lock()
	s.connections[id] = conn
	s.mu.Unlock()

	return conn
}

func (c *serverConn) serve() error {
	if err := ReadProtocolHeader(c.netConn); err != nil {
		return err
	}

	if err := c.sendMethod(0, c.startMethod()); err != nil {
		return err
	}

	for {
		frame, err := ReadFrame(c.netConn)
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
				"basic.nack":                   false,
				"connection.blocked":           false,
				"consumer_cancel_notify":       false,
				"exchange_exchange_bindings":   false,
				"publisher_confirms":           false,
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

func (c *serverConn) handleChannelOpen(id uint16) error {
	if id == 0 {
		return fmt.Errorf("amqp: channel 0 is reserved for connection methods")
	}

	c.mu.Lock()
	if _, exists := c.channels[id]; !exists {
		c.channels[id] = &channelState{
			channel: &amqpcore.Channel{
				ID:         id,
				Connection: c.amqpConn,
				Consumers:  make(map[string]*amqpcore.ConsumerSubscription),
			},
			nextDeliveryTag: 1,
			inFlight:        make(map[uint64]deliveryRef),
			consumers:       make(map[string]*consumerState),
		}
	}
	c.mu.Unlock()

	return c.sendMethod(id, ChannelOpenOk{})
}

func (c *serverConn) handleChannelClose(id uint16) error {
	ch, err := c.removeChannel(id)
	if err != nil {
		return err
	}
	ch.stopAllConsumers()
	return c.sendMethod(id, ChannelCloseOk{})
}

func (c *serverConn) handleExchangeDeclare(channel uint16, m ExchangeDeclare) error {
	if _, err := c.requireChannel(channel); err != nil {
		return err
	}

	kind, err := exchangeTypeFromString(m.Type)
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

	var q *amqpcore.Queue
	var err error
	if m.Passive {
		q, err = c.broker.GetQueue(queueName)
	} else {
		q, err = c.broker.DeclareQueue(queueName, m.Durable, m.Exclusive, m.AutoDelete, map[string]any(m.Arguments))
		if err == nil {
			err = c.broker.BindQueue("", queueName, queueName, nil)
		}
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
	if err := c.broker.BindQueue(m.Exchange, m.Queue, m.RoutingKey, map[string]any(m.Arguments)); err != nil {
		return err
	}
	if m.NoWait {
		return nil
	}
	return c.sendMethod(channel, QueueBindOk{})
}

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

	msg := amqpcore.Message{
		ContentType:   header.Properties.ContentType,
		CorrelationID: header.Properties.CorrelationID,
		ReplyTo:       header.Properties.ReplyTo,
		DeliveryMode:  header.Properties.DeliveryMode,
		Body:          body,
	}
	if len(header.Properties.Headers) > 0 {
		msg.Headers = map[string]any(header.Properties.Headers)
	}

	return c.broker.Publish(m.Exchange, m.RoutingKey, msg)
}

func (c *serverConn) handleBasicQos(channel uint16, m BasicQos) error {
	ch, err := c.requireChannel(channel)
	if err != nil {
		return err
	}

	ch.mu.Lock()
	ch.channel.PrefetchCount = int(m.PrefetchCount)
	ch.mu.Unlock()

	return c.sendMethod(channel, BasicQosOk{})
}

func (c *serverConn) handleBasicConsume(channel uint16, m BasicConsume) error {
	ch, err := c.requireChannel(channel)
	if err != nil {
		return err
	}
	if _, err := c.broker.GetQueue(m.Queue); err != nil {
		return err
	}

	tag := m.ConsumerTag
	if tag == "" {
		tag = fmt.Sprintf("ctag-%d-%d", channel, time.Now().UnixNano())
	}

	consumer := &consumerState{
		tag:       tag,
		queueName: m.Queue,
		autoAck:   m.NoAck,
		stop:      make(chan struct{}),
	}

	if err := ch.addConsumer(consumer); err != nil {
		return err
	}

	go c.consumeLoop(channel, ch, consumer)

	if m.NoWait {
		return nil
	}
	return c.sendMethod(channel, BasicConsumeOk{ConsumerTag: tag})
}

func (c *serverConn) handleBasicCancel(channel uint16, m BasicCancel) error {
	ch, err := c.requireChannel(channel)
	if err != nil {
		return err
	}

	consumer, ok := ch.removeConsumer(m.ConsumerTag)
	if !ok {
		return fmt.Errorf("amqp: consumer %q not found", m.ConsumerTag)
	}
	consumer.stopConsuming()

	if m.NoWait {
		return nil
	}
	return c.sendMethod(channel, BasicCancelOk{ConsumerTag: m.ConsumerTag})
}

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
	}
	return nil
}

func (c *serverConn) readPublishedContent(channel uint16) (ContentHeader, []byte, error) {
	headerFrame, err := ReadFrame(c.netConn)
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

	body := make([]byte, 0, header.BodySize)
	for uint64(len(body)) < header.BodySize {
		frame, err := ReadFrame(c.netConn)
		if err != nil {
			return ContentHeader{}, nil, err
		}
		if frame.Type == FrameHeartbeat {
			continue
		}
		if frame.Type != FrameBody || frame.Channel != channel {
			return ContentHeader{}, nil, fmt.Errorf("amqp: expected body frame on channel %d", channel)
		}
		body = append(body, frame.Payload...)
	}

	return header, body, nil
}

func (c *serverConn) sendMethod(channel uint16, method Method) error {
	frame, err := EncodeMethodFrame(channel, method)
	if err != nil {
		return err
	}
	return c.writeFrames(frame)
}

func (c *serverConn) sendDelivery(channel uint16, consumerTag string, deliveryTag uint64, msg amqpcore.Message) error {
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

		if !ch.canDispatch() {
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

		deliveryTag, reserved := ch.reserveDelivery(consumer.queueName, msg.DeliveryTag, consumer.autoAck)

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

		if consumer.autoAck {
			_ = queue.Ack(msg.DeliveryTag)
		}
	}
}

func (s *Server) consumerCount(queueName string) uint32 {
	s.mu.Lock()
	conns := make([]*serverConn, 0, len(s.connections))
	for _, conn := range s.connections {
		conns = append(conns, conn)
	}
	s.mu.Unlock()

	var count uint32
	for _, conn := range conns {
		conn.mu.Lock()
		channels := make([]*channelState, 0, len(conn.channels))
		for _, ch := range conn.channels {
			channels = append(channels, ch)
		}
		conn.mu.Unlock()

		for _, ch := range channels {
			count += ch.consumerCount(queueName)
		}
	}
	return count
}

func (ch *channelState) addConsumer(consumer *consumerState) error {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	if _, exists := ch.consumers[consumer.tag]; exists {
		return fmt.Errorf("amqp: consumer %q already exists", consumer.tag)
	}
	ch.consumers[consumer.tag] = consumer
	ch.channel.Consumers[consumer.tag] = &amqpcore.ConsumerSubscription{
		Tag:     consumer.tag,
		Queue:   consumer.queueName,
		Channel: ch.channel,
		AutoAck: consumer.autoAck,
	}
	return nil
}

func (ch *channelState) removeConsumer(tag string) (*consumerState, bool) {
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
	ch.consumers = make(map[string]*consumerState)
	ch.channel.Consumers = make(map[string]*amqpcore.ConsumerSubscription)
	ch.mu.Unlock()

	for _, consumer := range consumers {
		consumer.stopConsuming()
	}
}

func (ch *channelState) canDispatch() bool {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	if ch.channel.PrefetchCount <= 0 {
		return true
	}
	return len(ch.inFlight) < ch.channel.PrefetchCount
}

func (ch *channelState) reserveDelivery(queueName string, storeTag uint64, autoAck bool) (uint64, bool) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	tag := ch.nextDeliveryTag
	ch.nextDeliveryTag++
	if !autoAck {
		ch.inFlight[tag] = deliveryRef{queueName: queueName, storeTag: storeTag}
	}
	return tag, !autoAck
}

func (ch *channelState) releaseDelivery(deliveryTag uint64) {
	ch.mu.Lock()
	defer ch.mu.Unlock()
	delete(ch.inFlight, deliveryTag)
}

func (ch *channelState) ackRefs(deliveryTag uint64, multiple bool) ([]deliveryRef, error) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	if multiple {
		refs := make([]deliveryRef, 0)
		for tag, ref := range ch.inFlight {
			if tag <= deliveryTag {
				refs = append(refs, ref)
				delete(ch.inFlight, tag)
			}
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
	return []deliveryRef{ref}, nil
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

func exchangeTypeFromString(kind string) (amqpcore.ExchangeType, error) {
	switch kind {
	case string(amqpcore.ExchangeDirect):
		return amqpcore.ExchangeDirect, nil
	case string(amqpcore.ExchangeTopic):
		return amqpcore.ExchangeTopic, nil
	case string(amqpcore.ExchangeFanout):
		return amqpcore.ExchangeFanout, nil
	case string(amqpcore.ExchangeHeaders):
		return "", fmt.Errorf("amqp: exchange type %q is not supported yet", kind)
	default:
		return "", fmt.Errorf("amqp: unsupported exchange type %q", kind)
	}
}
