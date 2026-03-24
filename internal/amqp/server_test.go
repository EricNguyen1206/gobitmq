package amqp

import (
	"fmt"
	"net"
	"testing"
	"time"

	"erionn-mq/internal/amqpcore"
	"erionn-mq/internal/store"
)

func TestServer_EndToEndPublishConsumeAck(t *testing.T) {
	client := newTestClient(t)
	consumerTag := client.consume(t, 1, "test-q")

	client.publish(t, 1, "test-ex", "test-key", BasicProperties{ContentType: "text/plain"}, []byte("hello"))

	deliver, header, body := client.readDelivery(t, 1)
	if deliver.ConsumerTag != consumerTag {
		t.Fatalf("unexpected consumer tag: got=%q want=%q", deliver.ConsumerTag, consumerTag)
	}
	if deliver.Exchange != "test-ex" || deliver.RoutingKey != "test-key" {
		t.Fatalf("unexpected delivery routing: %+v", deliver)
	}
	if header.Properties.ContentType != "text/plain" {
		t.Fatalf("unexpected content type: %q", header.Properties.ContentType)
	}
	if string(body) != "hello" {
		t.Fatalf("unexpected body: %q", string(body))
	}

	client.sendMethod(t, 1, BasicAck{DeliveryTag: deliver.DeliveryTag})
	client.sendMethod(t, 1, BasicCancel{ConsumerTag: consumerTag})
	if _, ok := client.readMethod(t).(BasicCancelOk); !ok {
		t.Fatal("expected BasicCancelOk")
	}

	client.closeChannelAndConnection(t, 1)
}

func TestServer_PublisherConfirms_AckOnPublish(t *testing.T) {
	client := newTestClient(t)
	client.sendMethod(t, 1, ConfirmSelect{})
	if _, ok := client.readMethod(t).(ConfirmSelectOk); !ok {
		t.Fatal("expected ConfirmSelectOk")
	}

	client.publish(t, 1, "test-ex", "test-key", BasicProperties{}, []byte("hello"))
	ack, ok := client.readMethod(t).(BasicAck)
	if !ok {
		t.Fatal("expected BasicAck for publish confirm")
	}
	if ack.DeliveryTag != 1 {
		t.Fatalf("unexpected confirm delivery tag: got=%d want=1", ack.DeliveryTag)
	}

	client.closeChannelAndConnection(t, 1)
}

func TestServer_BasicNack_Requeue_Redelivers(t *testing.T) {
	client := newTestClient(t)
	consumerTag := client.consume(t, 1, "test-q")

	client.publish(t, 1, "test-ex", "test-key", BasicProperties{}, []byte("hello"))

	first, _, body := client.readDelivery(t, 1)
	if string(body) != "hello" {
		t.Fatalf("unexpected body: %q", string(body))
	}
	client.sendMethod(t, 1, BasicNack{DeliveryTag: first.DeliveryTag, Requeue: true})

	redelivery, _, redeliveredBody := client.readDelivery(t, 1)
	if redelivery.ConsumerTag != consumerTag {
		t.Fatalf("unexpected consumer tag: got=%q want=%q", redelivery.ConsumerTag, consumerTag)
	}
	if string(redeliveredBody) != "hello" {
		t.Fatalf("unexpected redelivered body: %q", string(redeliveredBody))
	}
	if !redelivery.Redelivered {
		t.Fatal("expected redelivery flag to be set")
	}

	client.sendMethod(t, 1, BasicAck{DeliveryTag: redelivery.DeliveryTag})
	client.cancelConsumer(t, 1, consumerTag)
	client.closeChannelAndConnection(t, 1)
}

func TestServer_BasicReject_DeadLetters(t *testing.T) {
	client := newTestClient(t)
	client.declareExchange(t, 1, "dlx", "direct")
	client.declareQueue(t, 1, "source-q", Table{
		"x-dead-letter-exchange":    "dlx",
		"x-dead-letter-routing-key": "dead",
	})
	client.bindQueue(t, 1, "source-q", "test-ex", "source")
	client.declareQueue(t, 1, "dead-q", nil)
	client.bindQueue(t, 1, "dead-q", "dlx", "dead")

	sourceTag := client.consume(t, 1, "source-q")
	deadTag := client.consume(t, 1, "dead-q")

	client.publish(t, 1, "test-ex", "source", BasicProperties{}, []byte("hello"))

	rejected, _, body := client.readDelivery(t, 1)
	if rejected.ConsumerTag != sourceTag {
		t.Fatalf("unexpected source consumer tag: got=%q want=%q", rejected.ConsumerTag, sourceTag)
	}
	if string(body) != "hello" {
		t.Fatalf("unexpected rejected body: %q", string(body))
	}
	client.sendMethod(t, 1, BasicReject{DeliveryTag: rejected.DeliveryTag, Requeue: false})

	deadLetter, _, deadBody := client.readDelivery(t, 1)
	if deadLetter.ConsumerTag != deadTag {
		t.Fatalf("unexpected dead-letter consumer tag: got=%q want=%q", deadLetter.ConsumerTag, deadTag)
	}
	if string(deadBody) != "hello" {
		t.Fatalf("unexpected dead-letter body: %q", string(deadBody))
	}

	client.sendMethod(t, 1, BasicAck{DeliveryTag: deadLetter.DeliveryTag})
	client.cancelConsumer(t, 1, sourceTag)
	client.cancelConsumer(t, 1, deadTag)
	client.closeChannelAndConnection(t, 1)
}

func TestServer_BasicQos_BlocksUntilAck(t *testing.T) {
	client := newTestClient(t)
	client.sendMethod(t, 1, BasicQos{PrefetchCount: 1})
	if _, ok := client.readMethod(t).(BasicQosOk); !ok {
		t.Fatal("expected BasicQosOk")
	}

	consumerTag := client.consume(t, 1, "test-q")
	client.publish(t, 1, "test-ex", "test-key", BasicProperties{}, []byte("first"))
	client.publish(t, 1, "test-ex", "test-key", BasicProperties{}, []byte("second"))

	first, _, body := client.readDelivery(t, 1)
	if string(body) != "first" {
		t.Fatalf("unexpected first delivery body: %q", string(body))
	}
	const noDeliveryProbeTimeout = 1 * time.Second
	client.conn.SetReadDeadline(time.Now().Add(noDeliveryProbeTimeout))
	_, err := ReadFrame(client.conn, defaultFrameMax)
	if err == nil {
		t.Fatal("expected no second delivery before ack")
	}
	if netErr, ok := err.(net.Error); !ok || !netErr.Timeout() {
		t.Fatalf("expected timeout before ack, got %v", err)
	}
	client.conn.SetReadDeadline(time.Time{})

	client.sendMethod(t, 1, BasicAck{DeliveryTag: first.DeliveryTag})
	second, _, secondBody := client.readDelivery(t, 1)
	if string(secondBody) != "second" {
		t.Fatalf("unexpected second delivery body: %q", string(secondBody))
	}

	client.sendMethod(t, 1, BasicAck{DeliveryTag: second.DeliveryTag})
	client.sendMethod(t, 1, BasicCancel{ConsumerTag: consumerTag})
	if _, ok := client.readMethod(t).(BasicCancelOk); !ok {
		t.Fatal("expected BasicCancelOk")
	}

	client.closeChannelAndConnection(t, 1)
}

func TestServer_BasicQos_GlobalFalseAppliesPerConsumer(t *testing.T) {
	client := newTestClient(t)
	client.sendMethod(t, 1, BasicQos{PrefetchCount: 1, Global: false})
	if _, ok := client.readMethod(t).(BasicQosOk); !ok {
		t.Fatal("expected BasicQosOk")
	}

	client.declareQueue(t, 1, "alt-q", nil)
	client.bindQueue(t, 1, "alt-q", "test-ex", "alt-key")

	firstTag := client.consume(t, 1, "test-q")
	secondTag := client.consume(t, 1, "alt-q")

	client.publish(t, 1, "test-ex", "test-key", BasicProperties{}, []byte("first"))
	client.publish(t, 1, "test-ex", "alt-key", BasicProperties{}, []byte("second"))

	seen := make(map[string]string, 2)
	deliveryTags := make([]uint64, 0, 2)
	for range 2 {
		deliver, _, body := client.readDelivery(t, 1)
		seen[deliver.ConsumerTag] = string(body)
		deliveryTags = append(deliveryTags, deliver.DeliveryTag)
	}

	if seen[firstTag] != "first" {
		t.Fatalf("unexpected delivery for first consumer: %v", seen)
	}
	if seen[secondTag] != "second" {
		t.Fatalf("unexpected delivery for second consumer: %v", seen)
	}

	for _, deliveryTag := range deliveryTags {
		client.sendMethod(t, 1, BasicAck{DeliveryTag: deliveryTag})
	}

	client.cancelConsumer(t, 1, firstTag)
	client.cancelConsumer(t, 1, secondTag)
	client.closeChannelAndConnection(t, 1)
}

func TestServer_BasicQos_GlobalTrueLimitsWholeChannel(t *testing.T) {
	client := newTestClient(t)
	client.sendMethod(t, 1, BasicQos{PrefetchCount: 1, Global: true})
	if _, ok := client.readMethod(t).(BasicQosOk); !ok {
		t.Fatal("expected BasicQosOk")
	}

	client.declareQueue(t, 1, "alt-q", nil)
	client.bindQueue(t, 1, "alt-q", "test-ex", "alt-key")

	firstTag := client.consume(t, 1, "test-q")
	secondTag := client.consume(t, 1, "alt-q")

	client.publish(t, 1, "test-ex", "test-key", BasicProperties{}, []byte("first"))
	client.publish(t, 1, "test-ex", "alt-key", BasicProperties{}, []byte("second"))

	first, _, firstBody := client.readDelivery(t, 1)
	client.expectNoFrame(t, 1*time.Second)

	client.sendMethod(t, 1, BasicAck{DeliveryTag: first.DeliveryTag})
	second, _, secondBody := client.readDelivery(t, 1)

	seen := map[string]string{
		first.ConsumerTag:  string(firstBody),
		second.ConsumerTag: string(secondBody),
	}
	if seen[firstTag] != "first" {
		t.Fatalf("unexpected first consumer deliveries: %v", seen)
	}
	if seen[secondTag] != "second" {
		t.Fatalf("unexpected second consumer deliveries: %v", seen)
	}

	client.sendMethod(t, 1, BasicAck{DeliveryTag: second.DeliveryTag})
	client.cancelConsumer(t, 1, firstTag)
	client.cancelConsumer(t, 1, secondTag)
	client.closeChannelAndConnection(t, 1)
}

func TestChannelState_StopAllConsumers_RequeuesInFlightMessages(t *testing.T) {
	broker := amqpcore.NewBroker(func() store.MessageStore {
		return store.NewMemoryMessageStore()
	})
	q, err := broker.DeclareQueue("jobs", false, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := q.Enqueue(amqpcore.Message{Body: []byte("hello")}); err != nil {
		t.Fatal(err)
	}

	msg, ok, err := q.Dequeue()
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected dequeued message")
	}

	ch := &channelState{
		broker: broker,
		channel: &amqpcore.Channel{
			Consumers: make(map[string]*amqpcore.ConsumerSubscription),
		},
		inFlight: map[uint64]deliveryRef{
			1: {queueName: "jobs", storeTag: msg.DeliveryTag},
		},
		consumers: map[string]*consumerState{
			"ctag-1": {tag: "ctag-1", queueName: "jobs", stop: make(chan struct{})},
		},
	}

	ch.stopAllConsumers()

	if q.Len() != 1 {
		t.Fatalf("expected queue length 1 after requeue, got %d", q.Len())
	}
	requeued, ok, err := q.Dequeue()
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected requeued message")
	}
	if !requeued.Redelivered {
		t.Fatal("expected requeued message to be marked redelivered")
	}
}

func TestServer_HandleChannelClose_RequeuesInFlightMessages(t *testing.T) {
	broker := amqpcore.NewBroker(func() store.MessageStore {
		return store.NewMemoryMessageStore()
	})
	q, msg := dequeueUnackedMessage(t, broker, "jobs")

	conn := &serverConn{
		broker:   broker,
		netConn:  discardConn{},
		channels: make(map[uint16]*channelState),
		done:     make(chan struct{}),
	}
	conn.channels[1] = &channelState{
		broker: broker,
		channel: &amqpcore.Channel{
			Consumers: make(map[string]*amqpcore.ConsumerSubscription),
		},
		inFlight: map[uint64]deliveryRef{
			1: {queueName: "jobs", storeTag: msg.DeliveryTag},
		},
		consumers: map[string]*consumerState{
			"ctag-1": {tag: "ctag-1", queueName: "jobs", stop: make(chan struct{})},
		},
	}

	if err := conn.handleChannelClose(1); err != nil {
		t.Fatal(err)
	}
	if q.Len() != 1 {
		t.Fatalf("expected queue length 1 after channel close, got %d", q.Len())
	}
}

func TestServerConn_Close_RequeuesInFlightMessages(t *testing.T) {
	broker := amqpcore.NewBroker(func() store.MessageStore {
		return store.NewMemoryMessageStore()
	})
	q, msg := dequeueUnackedMessage(t, broker, "jobs")

	conn := &serverConn{
		server:   &Server{connections: make(map[uint64]*serverConn)},
		broker:   broker,
		netConn:  discardConn{},
		amqpConn: &amqpcore.Connection{ID: 1},
		channels: make(map[uint16]*channelState),
		done:     make(chan struct{}),
	}
	conn.channels[1] = &channelState{
		broker: broker,
		channel: &amqpcore.Channel{
			Consumers: make(map[string]*amqpcore.ConsumerSubscription),
		},
		inFlight: map[uint64]deliveryRef{
			1: {queueName: "jobs", storeTag: msg.DeliveryTag},
		},
		consumers: map[string]*consumerState{
			"ctag-1": {tag: "ctag-1", queueName: "jobs", stop: make(chan struct{})},
		},
	}

	conn.close()
	if q.Len() != 1 {
		t.Fatalf("expected queue length 1 after connection close, got %d", q.Len())
	}
}

func TestChannelState_AckRefs_MultipleZeroAcknowledgesAll(t *testing.T) {
	ch := &channelState{
		inFlight: map[uint64]deliveryRef{
			1: {queueName: "q1", storeTag: 11},
			2: {queueName: "q2", storeTag: 22},
			3: {queueName: "q3", storeTag: 33},
		},
	}

	refs, err := ch.ackRefs(0, true)
	if err != nil {
		t.Fatalf("ackRefs(0, true): %v", err)
	}
	if len(refs) != 3 {
		t.Fatalf("expected 3 refs, got %d", len(refs))
	}
	if len(ch.inFlight) != 0 {
		t.Fatalf("expected inFlight to be empty, got %d entries", len(ch.inFlight))
	}
}

func TestChannelState_AckRefs_MultipleZeroOnEmptyIsNoOp(t *testing.T) {
	ch := &channelState{inFlight: make(map[uint64]deliveryRef)}

	refs, err := ch.ackRefs(0, true)
	if err != nil {
		t.Fatalf("ackRefs(0, true): %v", err)
	}
	if len(refs) != 0 {
		t.Fatalf("expected no refs, got %d", len(refs))
	}
}

type testClient struct {
	t    *testing.T
	conn net.Conn
}

type discardConn struct{}

func (discardConn) Read(_ []byte) (int, error)       { return 0, net.ErrClosed }
func (discardConn) Write(p []byte) (int, error)      { return len(p), nil }
func (discardConn) Close() error                     { return nil }
func (discardConn) LocalAddr() net.Addr              { return dummyAddr("local") }
func (discardConn) RemoteAddr() net.Addr             { return dummyAddr("remote") }
func (discardConn) SetDeadline(time.Time) error      { return nil }
func (discardConn) SetReadDeadline(time.Time) error  { return nil }
func (discardConn) SetWriteDeadline(time.Time) error { return nil }

type dummyAddr string

func (a dummyAddr) Network() string { return string(a) }
func (a dummyAddr) String() string  { return string(a) }

func dequeueUnackedMessage(t *testing.T, broker *amqpcore.Broker, queueName string) (*amqpcore.Queue, amqpcore.Message) {
	t.Helper()

	q, err := broker.DeclareQueue(queueName, false, false, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := q.Enqueue(amqpcore.Message{Body: []byte("hello")}); err != nil {
		t.Fatal(err)
	}
	msg, ok, err := q.Dequeue()
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected dequeued message")
	}
	return q, msg
}

func newTestClient(t *testing.T) *testClient {
	t.Helper()

	broker := amqpcore.NewBroker(func() store.MessageStore {
		return store.NewMemoryMessageStore()
	})
	server := NewServer("127.0.0.1:0", broker)

	ln, err := net.Listen("tcp", server.Addr)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		_ = server.Serve(ln)
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	client := &testClient{t: t, conn: conn}
	client.handshake(t)
	client.openChannel(t, 1)
	client.declareTopology(t)
	return client
}

func (c *testClient) handshake(t *testing.T) {
	t.Helper()

	if err := WriteProtocolHeader(c.conn); err != nil {
		t.Fatalf("WriteProtocolHeader: %v", err)
	}

	if _, ok := c.readMethod(t).(ConnectionStart); !ok {
		t.Fatal("expected ConnectionStart")
	}

	c.sendMethod(t, 0, ConnectionStartOk{
		ClientProperties: Table{"product": "amqplib-test"},
		Mechanism:        "PLAIN",
		Response:         []byte("\x00guest\x00guest"),
		Locale:           defaultLocale,
	})

	tune, ok := c.readMethod(t).(ConnectionTune)
	if !ok {
		t.Fatal("expected ConnectionTune")
	}
	c.sendMethod(t, 0, ConnectionTuneOk{
		ChannelMax: tune.ChannelMax,
		FrameMax:   tune.FrameMax,
		Heartbeat:  tune.Heartbeat,
	})
	c.sendMethod(t, 0, ConnectionOpen{VirtualHost: "/"})
	if _, ok := c.readMethod(t).(ConnectionOpenOk); !ok {
		t.Fatal("expected ConnectionOpenOk")
	}
}

func (c *testClient) openChannel(t *testing.T, channel uint16) {
	t.Helper()
	c.sendMethod(t, channel, ChannelOpen{})
	if _, ok := c.readMethod(t).(ChannelOpenOk); !ok {
		t.Fatal("expected ChannelOpenOk")
	}
}

func (c *testClient) declareTopology(t *testing.T) {
	t.Helper()

	c.declareExchange(t, 1, "test-ex", "direct")
	c.declareQueue(t, 1, "test-q", nil)
	c.bindQueue(t, 1, "test-q", "test-ex", "test-key")
}

func (c *testClient) consume(t *testing.T, channel uint16, queue string) string {
	t.Helper()
	c.sendMethod(t, channel, BasicConsume{Queue: queue})
	consumeOk, ok := c.readMethod(t).(BasicConsumeOk)
	if !ok {
		t.Fatal("expected BasicConsumeOk")
	}
	if consumeOk.ConsumerTag == "" {
		t.Fatal("expected generated consumer tag")
	}
	return consumeOk.ConsumerTag
}

func (c *testClient) declareExchange(t *testing.T, channel uint16, exchange, kind string) {
	t.Helper()
	c.sendMethod(t, channel, ExchangeDeclare{Exchange: exchange, Type: kind})
	if _, ok := c.readMethod(t).(ExchangeDeclareOk); !ok {
		t.Fatal("expected ExchangeDeclareOk")
	}
}

func (c *testClient) declareQueue(t *testing.T, channel uint16, queue string, args Table) {
	t.Helper()
	c.sendMethod(t, channel, QueueDeclare{Queue: queue, Arguments: args})
	if _, ok := c.readMethod(t).(QueueDeclareOk); !ok {
		t.Fatal("expected QueueDeclareOk")
	}
}

func (c *testClient) bindQueue(t *testing.T, channel uint16, queue, exchange, routingKey string) {
	t.Helper()
	c.sendMethod(t, channel, QueueBind{Queue: queue, Exchange: exchange, RoutingKey: routingKey})
	if _, ok := c.readMethod(t).(QueueBindOk); !ok {
		t.Fatal("expected QueueBindOk")
	}
}

func (c *testClient) cancelConsumer(t *testing.T, channel uint16, consumerTag string) {
	t.Helper()
	c.sendMethod(t, channel, BasicCancel{ConsumerTag: consumerTag})
	if _, ok := c.readMethod(t).(BasicCancelOk); !ok {
		t.Fatal("expected BasicCancelOk")
	}
}

func (c *testClient) publish(t *testing.T, channel uint16, exchange, routingKey string, props BasicProperties, body []byte) {
	t.Helper()

	c.sendMethod(t, channel, BasicPublish{Exchange: exchange, RoutingKey: routingKey})
	headerFrame, err := EncodeContentHeaderFrame(channel, ContentHeader{
		ClassID:    classBasic,
		BodySize:   uint64(len(body)),
		Properties: props,
	})
	if err != nil {
		t.Fatalf("EncodeContentHeaderFrame: %v", err)
	}
	if err := WriteFrame(c.conn, headerFrame); err != nil {
		t.Fatalf("WriteFrame(header): %v", err)
	}
	if err := WriteFrame(c.conn, Frame{Type: FrameBody, Channel: channel, Payload: append([]byte(nil), body...)}); err != nil {
		t.Fatalf("WriteFrame(body): %v", err)
	}
}

func (c *testClient) readDelivery(t *testing.T, channel uint16) (BasicDeliver, ContentHeader, []byte) {
	t.Helper()

	method, ok := c.readMethod(t).(BasicDeliver)
	if !ok {
		t.Fatal("expected BasicDeliver")
	}

	frame, err := ReadFrame(c.conn, defaultFrameMax)
	if err != nil {
		t.Fatalf("ReadFrame(header): %v", err)
	}
	if frame.Channel != channel || frame.Type != FrameHeader {
		t.Fatalf("unexpected header frame: %+v", frame)
	}
	header, err := DecodeContentHeaderFrame(frame)
	if err != nil {
		t.Fatalf("DecodeContentHeaderFrame: %v", err)
	}

	body := make([]byte, 0, header.BodySize)
	for uint64(len(body)) < header.BodySize {
		frame, err := ReadFrame(c.conn, defaultFrameMax)
		if err != nil {
			t.Fatalf("ReadFrame(body): %v", err)
		}
		if frame.Channel != channel || frame.Type != FrameBody {
			t.Fatalf("unexpected body frame: %+v", frame)
		}
		body = append(body, frame.Payload...)
	}

	return method, header, body
}

func (c *testClient) expectNoFrame(t *testing.T, timeout time.Duration) {
	t.Helper()
	c.conn.SetReadDeadline(time.Now().Add(timeout))
	_, err := ReadFrame(c.conn, defaultFrameMax)
	if err == nil {
		t.Fatal("expected no frame before timeout")
	}
	if netErr, ok := err.(net.Error); !ok || !netErr.Timeout() {
		t.Fatalf("expected timeout, got %v", err)
	}
	c.conn.SetReadDeadline(time.Time{})
}

func (c *testClient) closeChannelAndConnection(t *testing.T, channel uint16) {
	t.Helper()

	c.sendMethod(t, channel, ChannelClose{ReplyCode: 200, ReplyText: "bye"})
	if _, ok := c.readMethod(t).(ChannelCloseOk); !ok {
		t.Fatal("expected ChannelCloseOk")
	}

	c.sendMethod(t, 0, ConnectionClose{ReplyCode: 200, ReplyText: "bye"})
	if _, ok := c.readMethod(t).(ConnectionCloseOk); !ok {
		t.Fatal("expected ConnectionCloseOk")
	}
}

func (c *testClient) sendMethod(t *testing.T, channel uint16, method Method) {
	t.Helper()
	frame, err := EncodeMethodFrame(channel, method)
	if err != nil {
		t.Fatalf("EncodeMethodFrame(%T): %v", method, err)
	}
	if err := WriteFrame(c.conn, frame); err != nil {
		t.Fatalf("WriteFrame(%T): %v", method, err)
	}
}

func (c *testClient) readMethod(t *testing.T) Method {
	t.Helper()
	frame, err := ReadFrame(c.conn, defaultFrameMax)
	if err != nil {
		t.Fatalf("ReadFrame: %v", err)
	}
	if frame.Type != FrameMethod {
		t.Fatalf("expected method frame, got %+v", frame)
	}
	method, err := DecodeMethodFrame(frame)
	if err != nil {
		t.Fatalf("DecodeMethodFrame: %v", err)
	}
	return method
}

func ExampleServer() {
	broker := amqpcore.NewBroker(func() store.MessageStore {
		return store.NewMemoryMessageStore()
	})
	server := NewServer(DefaultAddr, broker)
	fmt.Println(server.Addr)
	// Output: :5672
}
