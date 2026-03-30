package amqp

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const (
	classConnection uint16 = 10
	classChannel    uint16 = 20
	classExchange   uint16 = 40
	classQueue      uint16 = 50
	classBasic      uint16 = 60
	classConfirm    uint16 = 85
)

type AMQPMethod interface {
	ClassID() uint16
	MethodID() uint16
	marshal(*bytes.Buffer) error
}

type methodKey uint32

func createAMQPMethodKey(classID, methodID uint16) methodKey {
	return methodKey(uint32(classID)<<16 | uint32(methodID))
}

type methodDecoder func(*bytes.Reader) (AMQPMethod, error)

var methodDecoders = map[methodKey]methodDecoder{
	createAMQPMethodKey(classConnection, 10): decodeConnStartRequest,
	createAMQPMethodKey(classConnection, 11): decodeConnStartResponse,
	createAMQPMethodKey(classConnection, 30): decodeConnTuneRequest,
	createAMQPMethodKey(classConnection, 31): decodeConnTuneResponse,
	createAMQPMethodKey(classConnection, 40): decodeConnOpenRequest,
	createAMQPMethodKey(classConnection, 41): decodeConnOpenResponse,
	createAMQPMethodKey(classConnection, 50): decodeConnCloseRequest,
	createAMQPMethodKey(classConnection, 51): decodeConnCloseResponse,
	createAMQPMethodKey(classChannel, 10):    decodeChanOpenRequest,
	createAMQPMethodKey(classChannel, 11):    decodeChanOpenResponse,
	createAMQPMethodKey(classChannel, 40):    decodeChanCloseRequest,
	createAMQPMethodKey(classChannel, 41):    decodeChanCloseResponse,
	createAMQPMethodKey(classExchange, 10):   decodeExchDeclareRequest,
	createAMQPMethodKey(classExchange, 11):   decodeExchDeclareResponse,
	createAMQPMethodKey(classQueue, 10):      decodeQueueDeclareRequest,
	createAMQPMethodKey(classQueue, 11):      decodeQueueDeclareResponse,
	createAMQPMethodKey(classQueue, 20):      decodeQueueBindRequest,
	createAMQPMethodKey(classQueue, 21):      decodeQueueBindResponse,
	createAMQPMethodKey(classBasic, 10):      decodeBasicQosRequest,
	createAMQPMethodKey(classBasic, 11):      decodeBasicQosResponse,
	createAMQPMethodKey(classBasic, 20):      decodeBasicConsumeRequest,
	createAMQPMethodKey(classBasic, 21):      decodeBasicConsumeResponse,
	createAMQPMethodKey(classBasic, 30):      decodeBasicCancelRequest,
	createAMQPMethodKey(classBasic, 31):      decodeBasicCancelResponse,
	createAMQPMethodKey(classBasic, 40):      decodeBasicPublish,
	createAMQPMethodKey(classBasic, 60):      decodeBasicDeliver,
	createAMQPMethodKey(classBasic, 80):      decodeBasicAck,
	createAMQPMethodKey(classBasic, 90):      decodeBasicReject,
	createAMQPMethodKey(classBasic, 120):     decodeBasicNack,
	createAMQPMethodKey(classConfirm, 10):    decodeConfirmSelectRequest,
	createAMQPMethodKey(classConfirm, 11):    decodeConfirmSelectResponse,
}

type ConnStartRequest struct {
	VersionMajor     byte
	VersionMinor     byte
	ServerProperties Table
	Mechanisms       string
	Locales          string
}

type ConnStartResponse struct {
	ClientProperties Table
	Mechanism        string
	Response         []byte
	Locale           string
}

type ConnTuneRequest struct {
	ChannelMax uint16
	FrameMax   uint32
	Heartbeat  uint16
}

type ConnTuneResponse struct {
	ChannelMax uint16
	FrameMax   uint32
	Heartbeat  uint16
}

type ConnOpenRequest struct {
	VirtualHost  string
	Capabilities string
	Insist       bool
}

type ConnOpenResponse struct {
	KnownHosts string
}

type ConnCloseRequest struct {
	ReplyCode   uint16
	ReplyText   string
	ClassIDRef  uint16
	MethodIDRef uint16
}

type ConnCloseResponse struct{}

type ChanOpenRequest struct {
	OutOfBand string
}

type ChanOpenResponse struct {
	ChannelID string
}

type ChanCloseRequest struct {
	ReplyCode   uint16
	ReplyText   string
	ClassIDRef  uint16
	MethodIDRef uint16
}

type ChanCloseResponse struct{}

type ExchDeclareRequest struct {
	Exchange   string
	Type       string
	Passive    bool
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Arguments  Table
}

type ExchDeclareResponse struct{}

type QueueDeclareRequest struct {
	Queue      string
	Passive    bool
	Durable    bool
	Exclusive  bool
	AutoDelete bool
	NoWait     bool
	Arguments  Table
}

type QueueDeclareResponse struct {
	Queue         string
	MessageCount  uint32
	ConsumerCount uint32
}

type QueueBindRequest struct {
	Queue      string
	Exchange   string
	RoutingKey string
	NoWait     bool
	Arguments  Table
}

type QueueBindResponse struct{}

type BasicQosRequest struct {
	PrefetchSize  uint32
	PrefetchCount uint16
	Global        bool
}

type BasicQosResponse struct{}

type BasicConsumeRequest struct {
	Queue       string
	ConsumerTag string
	NoLocal     bool
	NoAck       bool
	Exclusive   bool
	NoWait      bool
	Arguments   Table
}

type BasicConsumeResponse struct {
	ConsumerTag string
}

type BasicCancelRequest struct {
	ConsumerTag string
	NoWait      bool
}

type BasicCancelResponse struct {
	ConsumerTag string
}

type BasicPublish struct {
	Exchange   string
	RoutingKey string
	Mandatory  bool
	Immediate  bool
}

type BasicDeliver struct {
	ConsumerTag string
	DeliveryTag uint64
	Redelivered bool
	Exchange    string
	RoutingKey  string
}

type BasicAck struct {
	DeliveryTag uint64
	Multiple    bool
}

type BasicReject struct {
	DeliveryTag uint64
	Requeue     bool
}

type BasicNack struct {
	DeliveryTag uint64
	Multiple    bool
	Requeue     bool
}

type ConfirmSelectRequest struct {
	NoWait bool
}

type ConfirmSelectResponse struct{}

func EncodeMethodFrame(channel uint16, method AMQPMethod) (Frame, error) {
	var payload bytes.Buffer
	binary.Write(&payload, binary.BigEndian, method.ClassID())  //nolint:errcheck
	binary.Write(&payload, binary.BigEndian, method.MethodID()) //nolint:errcheck
	if err := method.marshal(&payload); err != nil {
		return Frame{}, err
	}
	return Frame{Type: FrameMethod, Channel: channel, Payload: payload.Bytes()}, nil
}

func DecodeMethodFrame(frame Frame) (AMQPMethod, error) {
	if frame.Type != FrameMethod {
		return nil, fmt.Errorf("amqp: expected method frame, got type %d", frame.Type)
	}

	r := bytes.NewReader(frame.Payload)
	var classID, methodID uint16
	if err := binary.Read(r, binary.BigEndian, &classID); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &methodID); err != nil {
		return nil, err
	}

	dec, ok := methodDecoders[createAMQPMethodKey(classID, methodID)]
	if !ok {
		return nil, fmt.Errorf("amqp: unsupported method %d.%d", classID, methodID)
	}
	return dec(r)
}

/* CLASS CONNECTION */

func (m ConnStartRequest) ClassID() uint16  { return classConnection }
func (m ConnStartRequest) MethodID() uint16 { return 10 }
func (m ConnStartRequest) marshal(w *bytes.Buffer) error {
	w.WriteByte(m.VersionMajor)
	w.WriteByte(m.VersionMinor)
	if err := writeTable(w, m.ServerProperties); err != nil {
		return err
	}
	if err := writeLongstrString(w, m.Mechanisms); err != nil {
		return err
	}
	return writeLongstrString(w, m.Locales)
}

func decodeConnStartRequest(r *bytes.Reader) (AMQPMethod, error) {
	major, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	minor, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	props, err := readTable(r)
	if err != nil {
		return nil, err
	}
	mechs, err := readLongstr(r)
	if err != nil {
		return nil, err
	}
	locales, err := readLongstr(r)
	if err != nil {
		return nil, err
	}
	return ConnStartRequest{
		VersionMajor:     major,
		VersionMinor:     minor,
		ServerProperties: props,
		Mechanisms:       string(mechs),
		Locales:          string(locales),
	}, nil
}

func (m ConnStartResponse) ClassID() uint16  { return classConnection }
func (m ConnStartResponse) MethodID() uint16 { return 11 }
func (m ConnStartResponse) marshal(w *bytes.Buffer) error {
	if err := writeTable(w, m.ClientProperties); err != nil {
		return err
	}
	if err := writeShortstr(w, m.Mechanism); err != nil {
		return err
	}
	if err := writeLongstr(w, m.Response); err != nil {
		return err
	}
	return writeShortstr(w, m.Locale)
}

func decodeConnStartResponse(r *bytes.Reader) (AMQPMethod, error) {
	props, err := readTable(r)
	if err != nil {
		return nil, err
	}
	mechanism, err := readShortstr(r)
	if err != nil {
		return nil, err
	}
	response, err := readLongstr(r)
	if err != nil {
		return nil, err
	}
	locale, err := readShortstr(r)
	if err != nil {
		return nil, err
	}
	return ConnStartResponse{
		ClientProperties: props,
		Mechanism:        mechanism,
		Response:         response,
		Locale:           locale,
	}, nil
}

func (m ConnTuneRequest) ClassID() uint16  { return classConnection }
func (m ConnTuneRequest) MethodID() uint16 { return 30 }
func (m ConnTuneRequest) marshal(w *bytes.Buffer) error {
	return marshalTune(w, m.ChannelMax, m.FrameMax, m.Heartbeat)
}

func decodeConnTuneRequest(r *bytes.Reader) (AMQPMethod, error) {
	channelMax, frameMax, heartbeat, err := decodeTune(r)
	if err != nil {
		return nil, err
	}
	return ConnTuneRequest{ChannelMax: channelMax, FrameMax: frameMax, Heartbeat: heartbeat}, nil
}

func (m ConnTuneResponse) ClassID() uint16  { return classConnection }
func (m ConnTuneResponse) MethodID() uint16 { return 31 }
func (m ConnTuneResponse) marshal(w *bytes.Buffer) error {
	return marshalTune(w, m.ChannelMax, m.FrameMax, m.Heartbeat)
}

func decodeConnTuneResponse(r *bytes.Reader) (AMQPMethod, error) {
	channelMax, frameMax, heartbeat, err := decodeTune(r)
	if err != nil {
		return nil, err
	}
	return ConnTuneResponse{ChannelMax: channelMax, FrameMax: frameMax, Heartbeat: heartbeat}, nil
}

func (m ConnOpenRequest) ClassID() uint16  { return classConnection }
func (m ConnOpenRequest) MethodID() uint16 { return 40 }
func (m ConnOpenRequest) marshal(w *bytes.Buffer) error {
	if err := writeShortstr(w, m.VirtualHost); err != nil {
		return err
	}
	if err := writeShortstr(w, m.Capabilities); err != nil {
		return err
	}
	return writeBit(w, m.Insist)
}

func decodeConnOpenRequest(r *bytes.Reader) (AMQPMethod, error) {
	vhost, err := readShortstr(r)
	if err != nil {
		return nil, err
	}
	caps, err := readShortstr(r)
	if err != nil {
		return nil, err
	}
	bits, err := readBits(r)
	if err != nil {
		return nil, err
	}
	return ConnOpenRequest{VirtualHost: vhost, Capabilities: caps, Insist: bit(bits, 0)}, nil
}

func (m ConnOpenResponse) ClassID() uint16  { return classConnection }
func (m ConnOpenResponse) MethodID() uint16 { return 41 }
func (m ConnOpenResponse) marshal(w *bytes.Buffer) error {
	return writeShortstr(w, m.KnownHosts)
}

func decodeConnOpenResponse(r *bytes.Reader) (AMQPMethod, error) {
	knownHosts, err := readShortstr(r)
	if err != nil {
		return nil, err
	}
	return ConnOpenResponse{KnownHosts: knownHosts}, nil
}

func (m ConnCloseRequest) ClassID() uint16  { return classConnection }
func (m ConnCloseRequest) MethodID() uint16 { return 50 }
func (m ConnCloseRequest) marshal(w *bytes.Buffer) error {
	return marshalClose(w, m.ReplyCode, m.ReplyText, m.ClassIDRef, m.MethodIDRef)
}

func decodeConnCloseRequest(r *bytes.Reader) (AMQPMethod, error) {
	replyCode, replyText, classRef, methodRef, err := decodeClose(r)
	if err != nil {
		return nil, err
	}
	return ConnCloseRequest{
		ReplyCode:   replyCode,
		ReplyText:   replyText,
		ClassIDRef:  classRef,
		MethodIDRef: methodRef,
	}, nil
}

func (m ConnCloseResponse) ClassID() uint16  { return classConnection }
func (m ConnCloseResponse) MethodID() uint16 { return 51 }
func (m ConnCloseResponse) marshal(w *bytes.Buffer) error {
	return nil
}

func decodeConnCloseResponse(*bytes.Reader) (AMQPMethod, error) { return ConnCloseResponse{}, nil }

/* CLASS CHANNEL */

func (m ChanOpenRequest) ClassID() uint16  { return classChannel }
func (m ChanOpenRequest) MethodID() uint16 { return 10 }
func (m ChanOpenRequest) marshal(w *bytes.Buffer) error {
	return writeShortstr(w, m.OutOfBand)
}

func decodeChanOpenRequest(r *bytes.Reader) (AMQPMethod, error) {
	outOfBand, err := readShortstr(r)
	if err != nil {
		return nil, err
	}
	return ChanOpenRequest{OutOfBand: outOfBand}, nil
}

func (m ChanOpenResponse) ClassID() uint16  { return classChannel }
func (m ChanOpenResponse) MethodID() uint16 { return 11 }
func (m ChanOpenResponse) marshal(w *bytes.Buffer) error {
	return writeLongstrString(w, m.ChannelID)
}

func decodeChanOpenResponse(r *bytes.Reader) (AMQPMethod, error) {
	channelID, err := readLongstr(r)
	if err != nil {
		return nil, err
	}
	return ChanOpenResponse{ChannelID: string(channelID)}, nil
}

func (m ChanCloseRequest) ClassID() uint16  { return classChannel }
func (m ChanCloseRequest) MethodID() uint16 { return 40 }
func (m ChanCloseRequest) marshal(w *bytes.Buffer) error {
	return marshalClose(w, m.ReplyCode, m.ReplyText, m.ClassIDRef, m.MethodIDRef)
}

func decodeChanCloseRequest(r *bytes.Reader) (AMQPMethod, error) {
	replyCode, replyText, classRef, methodRef, err := decodeClose(r)
	if err != nil {
		return nil, err
	}
	return ChanCloseRequest{
		ReplyCode:   replyCode,
		ReplyText:   replyText,
		ClassIDRef:  classRef,
		MethodIDRef: methodRef,
	}, nil
}

func (m ChanCloseResponse) ClassID() uint16  { return classChannel }
func (m ChanCloseResponse) MethodID() uint16 { return 41 }
func (m ChanCloseResponse) marshal(w *bytes.Buffer) error {
	return nil
}

func decodeChanCloseResponse(*bytes.Reader) (AMQPMethod, error) { return ChanCloseResponse{}, nil }

func (m ExchDeclareRequest) ClassID() uint16  { return classExchange }
func (m ExchDeclareRequest) MethodID() uint16 { return 10 }
func (m ExchDeclareRequest) marshal(w *bytes.Buffer) error {
	binary.Write(w, binary.BigEndian, uint16(0)) //nolint:errcheck
	if err := writeShortstr(w, m.Exchange); err != nil {
		return err
	}
	if err := writeShortstr(w, m.Type); err != nil {
		return err
	}
	if err := writeBits(w, m.Passive, m.Durable, m.AutoDelete, m.Internal, m.NoWait); err != nil {
		return err
	}
	return writeTable(w, m.Arguments)
}

func decodeExchDeclareRequest(r *bytes.Reader) (AMQPMethod, error) {
	if _, err := readUint16(r); err != nil {
		return nil, err
	}
	exchange, err := readShortstr(r)
	if err != nil {
		return nil, err
	}
	typ, err := readShortstr(r)
	if err != nil {
		return nil, err
	}
	bits, err := readBits(r)
	if err != nil {
		return nil, err
	}
	args, err := readTable(r)
	if err != nil {
		return nil, err
	}
	return ExchDeclareRequest{
		Exchange:   exchange,
		Type:       typ,
		Passive:    bit(bits, 0),
		Durable:    bit(bits, 1),
		AutoDelete: bit(bits, 2),
		Internal:   bit(bits, 3),
		NoWait:     bit(bits, 4),
		Arguments:  args,
	}, nil
}

func (m ExchDeclareResponse) ClassID() uint16  { return classExchange }
func (m ExchDeclareResponse) MethodID() uint16 { return 11 }
func (m ExchDeclareResponse) marshal(w *bytes.Buffer) error {
	return nil
}

func decodeExchDeclareResponse(*bytes.Reader) (AMQPMethod, error) { return ExchDeclareResponse{}, nil }

/* CLASS QUEUE */

func (m QueueDeclareRequest) ClassID() uint16  { return classQueue }
func (m QueueDeclareRequest) MethodID() uint16 { return 10 }
func (m QueueDeclareRequest) marshal(w *bytes.Buffer) error {
	binary.Write(w, binary.BigEndian, uint16(0)) //nolint:errcheck
	if err := writeShortstr(w, m.Queue); err != nil {
		return err
	}
	if err := writeBits(w, m.Passive, m.Durable, m.Exclusive, m.AutoDelete, m.NoWait); err != nil {
		return err
	}
	return writeTable(w, m.Arguments)
}

func decodeQueueDeclareRequest(r *bytes.Reader) (AMQPMethod, error) {
	if _, err := readUint16(r); err != nil {
		return nil, err
	}
	queue, err := readShortstr(r)
	if err != nil {
		return nil, err
	}
	bits, err := readBits(r)
	if err != nil {
		return nil, err
	}
	args, err := readTable(r)
	if err != nil {
		return nil, err
	}
	return QueueDeclareRequest{
		Queue:      queue,
		Passive:    bit(bits, 0),
		Durable:    bit(bits, 1),
		Exclusive:  bit(bits, 2),
		AutoDelete: bit(bits, 3),
		NoWait:     bit(bits, 4),
		Arguments:  args,
	}, nil
}

func (m QueueDeclareResponse) ClassID() uint16  { return classQueue }
func (m QueueDeclareResponse) MethodID() uint16 { return 11 }
func (m QueueDeclareResponse) marshal(w *bytes.Buffer) error {
	if err := writeShortstr(w, m.Queue); err != nil {
		return err
	}
	binary.Write(w, binary.BigEndian, m.MessageCount)  //nolint:errcheck
	binary.Write(w, binary.BigEndian, m.ConsumerCount) //nolint:errcheck
	return nil
}

func decodeQueueDeclareResponse(r *bytes.Reader) (AMQPMethod, error) {
	queue, err := readShortstr(r)
	if err != nil {
		return nil, err
	}
	var m QueueDeclareResponse
	m.Queue = queue
	err = binary.Read(r, binary.BigEndian, &m.MessageCount)
	if err == nil {
		err = binary.Read(r, binary.BigEndian, &m.ConsumerCount)
	}
	return m, err
}

func (m QueueBindRequest) ClassID() uint16  { return classQueue }
func (m QueueBindRequest) MethodID() uint16 { return 20 }
func (m QueueBindRequest) marshal(w *bytes.Buffer) error {
	binary.Write(w, binary.BigEndian, uint16(0)) //nolint:errcheck
	if err := writeShortstr(w, m.Queue); err != nil {
		return err
	}
	if err := writeShortstr(w, m.Exchange); err != nil {
		return err
	}
	if err := writeShortstr(w, m.RoutingKey); err != nil {
		return err
	}
	if err := writeBit(w, m.NoWait); err != nil {
		return err
	}
	return writeTable(w, m.Arguments)
}

func decodeQueueBindRequest(r *bytes.Reader) (AMQPMethod, error) {
	if _, err := readUint16(r); err != nil {
		return nil, err
	}
	queue, err := readShortstr(r)
	if err != nil {
		return nil, err
	}
	exchange, err := readShortstr(r)
	if err != nil {
		return nil, err
	}
	routingKey, err := readShortstr(r)
	if err != nil {
		return nil, err
	}
	bits, err := readBits(r)
	if err != nil {
		return nil, err
	}
	args, err := readTable(r)
	if err != nil {
		return nil, err
	}
	return QueueBindRequest{Queue: queue, Exchange: exchange, RoutingKey: routingKey, NoWait: bit(bits, 0), Arguments: args}, nil
}

func (m QueueBindResponse) ClassID() uint16  { return classQueue }
func (m QueueBindResponse) MethodID() uint16 { return 21 }
func (m QueueBindResponse) marshal(w *bytes.Buffer) error {
	return nil
}

func decodeQueueBindResponse(*bytes.Reader) (AMQPMethod, error) { return QueueBindResponse{}, nil }

/* CLASS BASIC */

func (m BasicQosRequest) ClassID() uint16  { return classBasic }
func (m BasicQosRequest) MethodID() uint16 { return 10 }
func (m BasicQosRequest) marshal(w *bytes.Buffer) error {
	binary.Write(w, binary.BigEndian, m.PrefetchSize)  //nolint:errcheck
	binary.Write(w, binary.BigEndian, m.PrefetchCount) //nolint:errcheck
	return writeBit(w, m.Global)
}

func decodeBasicQosRequest(r *bytes.Reader) (AMQPMethod, error) {
	var m BasicQosRequest
	err := binary.Read(r, binary.BigEndian, &m.PrefetchSize)
	if err == nil {
		err = binary.Read(r, binary.BigEndian, &m.PrefetchCount)
	}
	if err == nil {
		bits, bitErr := readBits(r)
		m.Global = bit(bits, 0)
		err = bitErr
	}
	return m, err
}

func (m BasicQosResponse) ClassID() uint16  { return classBasic }
func (m BasicQosResponse) MethodID() uint16 { return 11 }
func (m BasicQosResponse) marshal(w *bytes.Buffer) error {
	return nil
}

func decodeBasicQosResponse(*bytes.Reader) (AMQPMethod, error) { return BasicQosResponse{}, nil }

func (m BasicConsumeRequest) ClassID() uint16  { return classBasic }
func (m BasicConsumeRequest) MethodID() uint16 { return 20 }
func (m BasicConsumeRequest) marshal(w *bytes.Buffer) error {
	binary.Write(w, binary.BigEndian, uint16(0)) //nolint:errcheck
	if err := writeShortstr(w, m.Queue); err != nil {
		return err
	}
	if err := writeShortstr(w, m.ConsumerTag); err != nil {
		return err
	}
	if err := writeBits(w, m.NoLocal, m.NoAck, m.Exclusive, m.NoWait); err != nil {
		return err
	}
	return writeTable(w, m.Arguments)
}

func decodeBasicConsumeRequest(r *bytes.Reader) (AMQPMethod, error) {
	if _, err := readUint16(r); err != nil {
		return nil, err
	}
	queue, err := readShortstr(r)
	if err != nil {
		return nil, err
	}
	consumerTag, err := readShortstr(r)
	if err != nil {
		return nil, err
	}
	bits, err := readBits(r)
	if err != nil {
		return nil, err
	}
	args, err := readTable(r)
	if err != nil {
		return nil, err
	}
	return BasicConsumeRequest{
		Queue:       queue,
		ConsumerTag: consumerTag,
		NoLocal:     bit(bits, 0),
		NoAck:       bit(bits, 1),
		Exclusive:   bit(bits, 2),
		NoWait:      bit(bits, 3),
		Arguments:   args,
	}, nil
}

func (m BasicConsumeResponse) ClassID() uint16  { return classBasic }
func (m BasicConsumeResponse) MethodID() uint16 { return 21 }
func (m BasicConsumeResponse) marshal(w *bytes.Buffer) error {
	return writeShortstr(w, m.ConsumerTag)
}

func decodeBasicConsumeResponse(r *bytes.Reader) (AMQPMethod, error) {
	consumerTag, err := readShortstr(r)
	if err != nil {
		return nil, err
	}
	return BasicConsumeResponse{ConsumerTag: consumerTag}, nil
}

func (m BasicCancelRequest) ClassID() uint16  { return classBasic }
func (m BasicCancelRequest) MethodID() uint16 { return 30 }
func (m BasicCancelRequest) marshal(w *bytes.Buffer) error {
	if err := writeShortstr(w, m.ConsumerTag); err != nil {
		return err
	}
	return writeBit(w, m.NoWait)
}

func decodeBasicCancelRequest(r *bytes.Reader) (AMQPMethod, error) {
	consumerTag, err := readShortstr(r)
	if err != nil {
		return nil, err
	}
	bits, err := readBits(r)
	if err != nil {
		return nil, err
	}
	return BasicCancelRequest{ConsumerTag: consumerTag, NoWait: bit(bits, 0)}, nil
}

func (m BasicCancelResponse) ClassID() uint16  { return classBasic }
func (m BasicCancelResponse) MethodID() uint16 { return 31 }
func (m BasicCancelResponse) marshal(w *bytes.Buffer) error {
	return writeShortstr(w, m.ConsumerTag)
}

func decodeBasicCancelResponse(r *bytes.Reader) (AMQPMethod, error) {
	consumerTag, err := readShortstr(r)
	if err != nil {
		return nil, err
	}
	return BasicCancelResponse{ConsumerTag: consumerTag}, nil
}


func (m BasicPublish) ClassID() uint16  { return classBasic }
func (m BasicPublish) MethodID() uint16 { return 40 }
func (m BasicPublish) marshal(w *bytes.Buffer) error {
	binary.Write(w, binary.BigEndian, uint16(0)) //nolint:errcheck
	if err := writeShortstr(w, m.Exchange); err != nil {
		return err
	}
	if err := writeShortstr(w, m.RoutingKey); err != nil {
		return err
	}
	return writeBits(w, m.Mandatory, m.Immediate)
}

func decodeBasicPublish(r *bytes.Reader) (AMQPMethod, error) {
	if _, err := readUint16(r); err != nil {
		return nil, err
	}
	exchange, err := readShortstr(r)
	if err != nil {
		return nil, err
	}
	routingKey, err := readShortstr(r)
	if err != nil {
		return nil, err
	}
	bits, err := readBits(r)
	if err != nil {
		return nil, err
	}
	return BasicPublish{Exchange: exchange, RoutingKey: routingKey, Mandatory: bit(bits, 0), Immediate: bit(bits, 1)}, nil
}

func (m BasicDeliver) ClassID() uint16  { return classBasic }
func (m BasicDeliver) MethodID() uint16 { return 60 }
func (m BasicDeliver) marshal(w *bytes.Buffer) error {
	if err := writeShortstr(w, m.ConsumerTag); err != nil {
		return err
	}
	binary.Write(w, binary.BigEndian, m.DeliveryTag) //nolint:errcheck
	if err := writeBit(w, m.Redelivered); err != nil {
		return err
	}
	if err := writeShortstr(w, m.Exchange); err != nil {
		return err
	}
	return writeShortstr(w, m.RoutingKey)
}

func decodeBasicDeliver(r *bytes.Reader) (AMQPMethod, error) {
	consumerTag, err := readShortstr(r)
	if err != nil {
		return nil, err
	}
	var deliveryTag uint64
	if err := binary.Read(r, binary.BigEndian, &deliveryTag); err != nil {
		return nil, err
	}
	bits, err := readBits(r)
	if err != nil {
		return nil, err
	}
	exchange, err := readShortstr(r)
	if err != nil {
		return nil, err
	}
	routingKey, err := readShortstr(r)
	if err != nil {
		return nil, err
	}
	return BasicDeliver{ConsumerTag: consumerTag, DeliveryTag: deliveryTag, Redelivered: bit(bits, 0), Exchange: exchange, RoutingKey: routingKey}, nil
}

func (m BasicAck) ClassID() uint16  { return classBasic }
func (m BasicAck) MethodID() uint16 { return 80 }
func (m BasicAck) marshal(w *bytes.Buffer) error {
	binary.Write(w, binary.BigEndian, m.DeliveryTag) //nolint:errcheck
	return writeBit(w, m.Multiple)
}

func decodeBasicAck(r *bytes.Reader) (AMQPMethod, error) {
	var m BasicAck
	err := binary.Read(r, binary.BigEndian, &m.DeliveryTag)
	if err == nil {
		bits, bitErr := readBits(r)
		m.Multiple = bit(bits, 0)
		err = bitErr
	}
	return m, err
}

func (m BasicReject) ClassID() uint16  { return classBasic }
func (m BasicReject) MethodID() uint16 { return 90 }
func (m BasicReject) marshal(w *bytes.Buffer) error {
	binary.Write(w, binary.BigEndian, m.DeliveryTag) //nolint:errcheck
	return writeBit(w, m.Requeue)
}

func decodeBasicReject(r *bytes.Reader) (AMQPMethod, error) {
	var m BasicReject
	err := binary.Read(r, binary.BigEndian, &m.DeliveryTag)
	if err == nil {
		bits, bitErr := readBits(r)
		m.Requeue = bit(bits, 0)
		err = bitErr
	}
	return m, err
}

func (m BasicNack) ClassID() uint16  { return classBasic }
func (m BasicNack) MethodID() uint16 { return 120 }
func (m BasicNack) marshal(w *bytes.Buffer) error {
	binary.Write(w, binary.BigEndian, m.DeliveryTag) //nolint:errcheck
	return writeBits(w, m.Multiple, m.Requeue)
}

func decodeBasicNack(r *bytes.Reader) (AMQPMethod, error) {
	var m BasicNack
	err := binary.Read(r, binary.BigEndian, &m.DeliveryTag)
	if err == nil {
		bits, bitErr := readBits(r)
		m.Multiple = bit(bits, 0)
		m.Requeue = bit(bits, 1)
		err = bitErr
	}
	return m, err
}

/* CLASS CONFIRM */

func (m ConfirmSelectRequest) ClassID() uint16  { return classConfirm }
func (m ConfirmSelectRequest) MethodID() uint16 { return 10 }
func (m ConfirmSelectRequest) marshal(w *bytes.Buffer) error {
	return writeBit(w, m.NoWait)
}

func decodeConfirmSelectRequest(r *bytes.Reader) (AMQPMethod, error) {
	bits, err := readBits(r)
	if err != nil {
		return nil, err
	}
	return ConfirmSelectRequest{NoWait: bit(bits, 0)}, nil
}

func (m ConfirmSelectResponse) ClassID() uint16               { return classConfirm }
func (m ConfirmSelectResponse) MethodID() uint16              { return 11 }
func (m ConfirmSelectResponse) marshal(w *bytes.Buffer) error { return nil }

func decodeConfirmSelectResponse(*bytes.Reader) (AMQPMethod, error) { return ConfirmSelectResponse{}, nil }

func marshalClose(w *bytes.Buffer, replyCode uint16, replyText string, classRef, methodRef uint16) error {
	binary.Write(w, binary.BigEndian, replyCode) //nolint:errcheck
	if err := writeShortstr(w, replyText); err != nil {
		return err
	}
	binary.Write(w, binary.BigEndian, classRef)  //nolint:errcheck
	binary.Write(w, binary.BigEndian, methodRef) //nolint:errcheck
	return nil
}

func decodeClose(r *bytes.Reader) (replyCode uint16, replyText string, classRef, methodRef uint16, err error) {
	if err = binary.Read(r, binary.BigEndian, &replyCode); err != nil {
		return
	}
	if replyText, err = readShortstr(r); err != nil {
		return
	}
	if err = binary.Read(r, binary.BigEndian, &classRef); err != nil {
		return
	}
	err = binary.Read(r, binary.BigEndian, &methodRef)
	return
}

func marshalTune(w *bytes.Buffer, channelMax uint16, frameMax uint32, heartbeat uint16) error {
	binary.Write(w, binary.BigEndian, channelMax) //nolint:errcheck
	binary.Write(w, binary.BigEndian, frameMax)   //nolint:errcheck
	binary.Write(w, binary.BigEndian, heartbeat)  //nolint:errcheck
	return nil
}

func decodeTune(r *bytes.Reader) (channelMax uint16, frameMax uint32, heartbeat uint16, err error) {
	if err = binary.Read(r, binary.BigEndian, &channelMax); err != nil {
		return
	}
	if err = binary.Read(r, binary.BigEndian, &frameMax); err != nil {
		return
	}
	err = binary.Read(r, binary.BigEndian, &heartbeat)
	return
}

func writeBit(w *bytes.Buffer, value bool) error {
	return writeBits(w, value)
}

func writeBits(w *bytes.Buffer, values ...bool) error {
	var b byte
	for i, value := range values {
		if value {
			b |= 1 << i
		}
	}
	return w.WriteByte(b)
}

func readBits(r *bytes.Reader) (byte, error) {
	return r.ReadByte()
}

func bit(bits byte, index uint) bool {
	return bits&(1<<index) != 0
}

func readUint16(r *bytes.Reader) (uint16, error) {
	var v uint16
	err := binary.Read(r, binary.BigEndian, &v)
	return v, err
}
