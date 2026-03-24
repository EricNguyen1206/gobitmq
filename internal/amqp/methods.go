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

type Method interface {
	ClassID() uint16
	MethodID() uint16
	marshal(*bytes.Buffer) error
}

type ConnectionStart struct {
	VersionMajor     byte
	VersionMinor     byte
	ServerProperties Table
	Mechanisms       string
	Locales          string
}

type ConnectionStartOk struct {
	ClientProperties Table
	Mechanism        string
	Response         []byte
	Locale           string
}

type ConnectionTune struct {
	ChannelMax uint16
	FrameMax   uint32
	Heartbeat  uint16
}

type ConnectionTuneOk struct {
	ChannelMax uint16
	FrameMax   uint32
	Heartbeat  uint16
}

type ConnectionOpen struct {
	VirtualHost  string
	Capabilities string
	Insist       bool
}

type ConnectionOpenOk struct {
	KnownHosts string
}

type ConnectionClose struct {
	ReplyCode   uint16
	ReplyText   string
	ClassIDRef  uint16
	MethodIDRef uint16
}

type ConnectionCloseOk struct{}

type ChannelOpen struct {
	OutOfBand string
}

type ChannelOpenOk struct {
	ChannelID string
}

type ChannelClose struct {
	ReplyCode   uint16
	ReplyText   string
	ClassIDRef  uint16
	MethodIDRef uint16
}

type ChannelCloseOk struct{}

type ExchangeDeclare struct {
	Exchange   string
	Type       string
	Passive    bool
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Arguments  Table
}

type ExchangeDeclareOk struct{}

type QueueDeclare struct {
	Queue      string
	Passive    bool
	Durable    bool
	Exclusive  bool
	AutoDelete bool
	NoWait     bool
	Arguments  Table
}

type QueueDeclareOk struct {
	Queue         string
	MessageCount  uint32
	ConsumerCount uint32
}

type QueueBind struct {
	Queue      string
	Exchange   string
	RoutingKey string
	NoWait     bool
	Arguments  Table
}

type QueueBindOk struct{}

type BasicQos struct {
	PrefetchSize  uint32
	PrefetchCount uint16
	Global        bool
}

type BasicQosOk struct{}

type BasicConsume struct {
	Queue       string
	ConsumerTag string
	NoLocal     bool
	NoAck       bool
	Exclusive   bool
	NoWait      bool
	Arguments   Table
}

type BasicConsumeOk struct {
	ConsumerTag string
}

type BasicCancel struct {
	ConsumerTag string
	NoWait      bool
}

type BasicCancelOk struct {
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

type ConfirmSelect struct {
	NoWait bool
}

type ConfirmSelectOk struct{}

func EncodeMethodFrame(channel uint16, method Method) (Frame, error) {
	var payload bytes.Buffer
	binary.Write(&payload, binary.BigEndian, method.ClassID())  //nolint:errcheck
	binary.Write(&payload, binary.BigEndian, method.MethodID()) //nolint:errcheck
	if err := method.marshal(&payload); err != nil {
		return Frame{}, err
	}
	return Frame{Type: FrameMethod, Channel: channel, Payload: payload.Bytes()}, nil
}

func DecodeMethodFrame(frame Frame) (Method, error) {
	if frame.Type != FrameMethod {
		return nil, fmt.Errorf("amqp: expected method frame, got type %d", frame.Type)
	}

	r := bytes.NewReader(frame.Payload)
	var classID uint16
	var methodID uint16
	if err := binary.Read(r, binary.BigEndian, &classID); err != nil {
		return nil, err
	}
	if err := binary.Read(r, binary.BigEndian, &methodID); err != nil {
		return nil, err
	}

	switch classID {
	case classConnection:
		switch methodID {
		case 10:
			return decodeConnectionStart(r)
		case 11:
			return decodeConnectionStartOk(r)
		case 30:
			return decodeConnectionTune(r)
		case 31:
			return decodeConnectionTuneOk(r)
		case 40:
			return decodeConnectionOpen(r)
		case 41:
			return decodeConnectionOpenOk(r)
		case 50:
			return decodeConnectionClose(r)
		case 51:
			return ConnectionCloseOk{}, nil
		}
	case classChannel:
		switch methodID {
		case 10:
			return decodeChannelOpen(r)
		case 11:
			return decodeChannelOpenOk(r)
		case 40:
			return decodeChannelClose(r)
		case 41:
			return ChannelCloseOk{}, nil
		}
	case classExchange:
		switch methodID {
		case 10:
			return decodeExchangeDeclare(r)
		case 11:
			return ExchangeDeclareOk{}, nil
		}
	case classQueue:
		switch methodID {
		case 10:
			return decodeQueueDeclare(r)
		case 11:
			return decodeQueueDeclareOk(r)
		case 20:
			return decodeQueueBind(r)
		case 21:
			return QueueBindOk{}, nil
		}
	case classBasic:
		switch methodID {
		case 10:
			return decodeBasicQos(r)
		case 11:
			return BasicQosOk{}, nil
		case 20:
			return decodeBasicConsume(r)
		case 21:
			return decodeBasicConsumeOk(r)
		case 30:
			return decodeBasicCancel(r)
		case 31:
			return decodeBasicCancelOk(r)
		case 40:
			return decodeBasicPublish(r)
		case 60:
			return decodeBasicDeliver(r)
		case 80:
			return decodeBasicAck(r)
		case 90:
			return decodeBasicReject(r)
		case 120:
			return decodeBasicNack(r)
		}
	case classConfirm:
		switch methodID {
		case 10:
			return decodeConfirmSelect(r)
		case 11:
			return ConfirmSelectOk{}, nil
		}
	}

	return nil, fmt.Errorf("amqp: unsupported method %d.%d", classID, methodID)
}

func (m ConnectionStart) ClassID() uint16  { return classConnection }
func (m ConnectionStart) MethodID() uint16 { return 10 }
func (m ConnectionStart) marshal(w *bytes.Buffer) error {
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

func decodeConnectionStart(r *bytes.Reader) (Method, error) {
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
	return ConnectionStart{
		VersionMajor:     major,
		VersionMinor:     minor,
		ServerProperties: props,
		Mechanisms:       string(mechs),
		Locales:          string(locales),
	}, nil
}

func (m ConnectionStartOk) ClassID() uint16  { return classConnection }
func (m ConnectionStartOk) MethodID() uint16 { return 11 }
func (m ConnectionStartOk) marshal(w *bytes.Buffer) error {
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

func decodeConnectionStartOk(r *bytes.Reader) (Method, error) {
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
	return ConnectionStartOk{
		ClientProperties: props,
		Mechanism:        mechanism,
		Response:         response,
		Locale:           locale,
	}, nil
}

func (m ConnectionTune) ClassID() uint16  { return classConnection }
func (m ConnectionTune) MethodID() uint16 { return 30 }
func (m ConnectionTune) marshal(w *bytes.Buffer) error {
	binary.Write(w, binary.BigEndian, m.ChannelMax) //nolint:errcheck
	binary.Write(w, binary.BigEndian, m.FrameMax)   //nolint:errcheck
	binary.Write(w, binary.BigEndian, m.Heartbeat)  //nolint:errcheck
	return nil
}

func decodeConnectionTune(r *bytes.Reader) (Method, error) {
	var m ConnectionTune
	err := binary.Read(r, binary.BigEndian, &m.ChannelMax)
	if err == nil {
		err = binary.Read(r, binary.BigEndian, &m.FrameMax)
	}
	if err == nil {
		err = binary.Read(r, binary.BigEndian, &m.Heartbeat)
	}
	return m, err
}

func (m ConnectionTuneOk) ClassID() uint16  { return classConnection }
func (m ConnectionTuneOk) MethodID() uint16 { return 31 }
func (m ConnectionTuneOk) marshal(w *bytes.Buffer) error {
	binary.Write(w, binary.BigEndian, m.ChannelMax) //nolint:errcheck
	binary.Write(w, binary.BigEndian, m.FrameMax)   //nolint:errcheck
	binary.Write(w, binary.BigEndian, m.Heartbeat)  //nolint:errcheck
	return nil
}

func decodeConnectionTuneOk(r *bytes.Reader) (Method, error) {
	var m ConnectionTuneOk
	err := binary.Read(r, binary.BigEndian, &m.ChannelMax)
	if err == nil {
		err = binary.Read(r, binary.BigEndian, &m.FrameMax)
	}
	if err == nil {
		err = binary.Read(r, binary.BigEndian, &m.Heartbeat)
	}
	return m, err
}

func (m ConnectionOpen) ClassID() uint16  { return classConnection }
func (m ConnectionOpen) MethodID() uint16 { return 40 }
func (m ConnectionOpen) marshal(w *bytes.Buffer) error {
	if err := writeShortstr(w, m.VirtualHost); err != nil {
		return err
	}
	if err := writeShortstr(w, m.Capabilities); err != nil {
		return err
	}
	return writeBit(w, m.Insist)
}

func decodeConnectionOpen(r *bytes.Reader) (Method, error) {
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
	return ConnectionOpen{VirtualHost: vhost, Capabilities: caps, Insist: bit(bits, 0)}, nil
}

func (m ConnectionOpenOk) ClassID() uint16  { return classConnection }
func (m ConnectionOpenOk) MethodID() uint16 { return 41 }
func (m ConnectionOpenOk) marshal(w *bytes.Buffer) error {
	return writeShortstr(w, m.KnownHosts)
}

func decodeConnectionOpenOk(r *bytes.Reader) (Method, error) {
	knownHosts, err := readShortstr(r)
	if err != nil {
		return nil, err
	}
	return ConnectionOpenOk{KnownHosts: knownHosts}, nil
}

func (m ConnectionClose) ClassID() uint16  { return classConnection }
func (m ConnectionClose) MethodID() uint16 { return 50 }
func (m ConnectionClose) marshal(w *bytes.Buffer) error {
	binary.Write(w, binary.BigEndian, m.ReplyCode) //nolint:errcheck
	if err := writeShortstr(w, m.ReplyText); err != nil {
		return err
	}
	binary.Write(w, binary.BigEndian, m.ClassIDRef)  //nolint:errcheck
	binary.Write(w, binary.BigEndian, m.MethodIDRef) //nolint:errcheck
	return nil
}

func decodeConnectionClose(r *bytes.Reader) (Method, error) {
	var m ConnectionClose
	err := binary.Read(r, binary.BigEndian, &m.ReplyCode)
	if err != nil {
		return nil, err
	}
	m.ReplyText, err = readShortstr(r)
	if err == nil {
		err = binary.Read(r, binary.BigEndian, &m.ClassIDRef)
	}
	if err == nil {
		err = binary.Read(r, binary.BigEndian, &m.MethodIDRef)
	}
	return m, err
}

func (m ConnectionCloseOk) ClassID() uint16  { return classConnection }
func (m ConnectionCloseOk) MethodID() uint16 { return 51 }
func (m ConnectionCloseOk) marshal(w *bytes.Buffer) error {
	return nil
}

func (m ChannelOpen) ClassID() uint16  { return classChannel }
func (m ChannelOpen) MethodID() uint16 { return 10 }
func (m ChannelOpen) marshal(w *bytes.Buffer) error {
	return writeShortstr(w, m.OutOfBand)
}

func decodeChannelOpen(r *bytes.Reader) (Method, error) {
	outOfBand, err := readShortstr(r)
	if err != nil {
		return nil, err
	}
	return ChannelOpen{OutOfBand: outOfBand}, nil
}

func (m ChannelOpenOk) ClassID() uint16  { return classChannel }
func (m ChannelOpenOk) MethodID() uint16 { return 11 }
func (m ChannelOpenOk) marshal(w *bytes.Buffer) error {
	return writeLongstrString(w, m.ChannelID)
}

func decodeChannelOpenOk(r *bytes.Reader) (Method, error) {
	channelID, err := readLongstr(r)
	if err != nil {
		return nil, err
	}
	return ChannelOpenOk{ChannelID: string(channelID)}, nil
}

func (m ChannelClose) ClassID() uint16  { return classChannel }
func (m ChannelClose) MethodID() uint16 { return 40 }
func (m ChannelClose) marshal(w *bytes.Buffer) error {
	binary.Write(w, binary.BigEndian, m.ReplyCode) //nolint:errcheck
	if err := writeShortstr(w, m.ReplyText); err != nil {
		return err
	}
	binary.Write(w, binary.BigEndian, m.ClassIDRef)  //nolint:errcheck
	binary.Write(w, binary.BigEndian, m.MethodIDRef) //nolint:errcheck
	return nil
}

func decodeChannelClose(r *bytes.Reader) (Method, error) {
	var m ChannelClose
	err := binary.Read(r, binary.BigEndian, &m.ReplyCode)
	if err != nil {
		return nil, err
	}
	m.ReplyText, err = readShortstr(r)
	if err == nil {
		err = binary.Read(r, binary.BigEndian, &m.ClassIDRef)
	}
	if err == nil {
		err = binary.Read(r, binary.BigEndian, &m.MethodIDRef)
	}
	return m, err
}

func (m ChannelCloseOk) ClassID() uint16  { return classChannel }
func (m ChannelCloseOk) MethodID() uint16 { return 41 }
func (m ChannelCloseOk) marshal(w *bytes.Buffer) error {
	return nil
}

func (m ExchangeDeclare) ClassID() uint16  { return classExchange }
func (m ExchangeDeclare) MethodID() uint16 { return 10 }
func (m ExchangeDeclare) marshal(w *bytes.Buffer) error {
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

func decodeExchangeDeclare(r *bytes.Reader) (Method, error) {
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
	return ExchangeDeclare{
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

func (m ExchangeDeclareOk) ClassID() uint16  { return classExchange }
func (m ExchangeDeclareOk) MethodID() uint16 { return 11 }
func (m ExchangeDeclareOk) marshal(w *bytes.Buffer) error {
	return nil
}

func (m QueueDeclare) ClassID() uint16  { return classQueue }
func (m QueueDeclare) MethodID() uint16 { return 10 }
func (m QueueDeclare) marshal(w *bytes.Buffer) error {
	binary.Write(w, binary.BigEndian, uint16(0)) //nolint:errcheck
	if err := writeShortstr(w, m.Queue); err != nil {
		return err
	}
	if err := writeBits(w, m.Passive, m.Durable, m.Exclusive, m.AutoDelete, m.NoWait); err != nil {
		return err
	}
	return writeTable(w, m.Arguments)
}

func decodeQueueDeclare(r *bytes.Reader) (Method, error) {
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
	return QueueDeclare{
		Queue:      queue,
		Passive:    bit(bits, 0),
		Durable:    bit(bits, 1),
		Exclusive:  bit(bits, 2),
		AutoDelete: bit(bits, 3),
		NoWait:     bit(bits, 4),
		Arguments:  args,
	}, nil
}

func (m QueueDeclareOk) ClassID() uint16  { return classQueue }
func (m QueueDeclareOk) MethodID() uint16 { return 11 }
func (m QueueDeclareOk) marshal(w *bytes.Buffer) error {
	if err := writeShortstr(w, m.Queue); err != nil {
		return err
	}
	binary.Write(w, binary.BigEndian, m.MessageCount)  //nolint:errcheck
	binary.Write(w, binary.BigEndian, m.ConsumerCount) //nolint:errcheck
	return nil
}

func decodeQueueDeclareOk(r *bytes.Reader) (Method, error) {
	queue, err := readShortstr(r)
	if err != nil {
		return nil, err
	}
	var m QueueDeclareOk
	m.Queue = queue
	err = binary.Read(r, binary.BigEndian, &m.MessageCount)
	if err == nil {
		err = binary.Read(r, binary.BigEndian, &m.ConsumerCount)
	}
	return m, err
}

func (m QueueBind) ClassID() uint16  { return classQueue }
func (m QueueBind) MethodID() uint16 { return 20 }
func (m QueueBind) marshal(w *bytes.Buffer) error {
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

func decodeQueueBind(r *bytes.Reader) (Method, error) {
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
	return QueueBind{Queue: queue, Exchange: exchange, RoutingKey: routingKey, NoWait: bit(bits, 0), Arguments: args}, nil
}

func (m QueueBindOk) ClassID() uint16  { return classQueue }
func (m QueueBindOk) MethodID() uint16 { return 21 }
func (m QueueBindOk) marshal(w *bytes.Buffer) error {
	return nil
}

func (m BasicQos) ClassID() uint16  { return classBasic }
func (m BasicQos) MethodID() uint16 { return 10 }
func (m BasicQos) marshal(w *bytes.Buffer) error {
	binary.Write(w, binary.BigEndian, m.PrefetchSize)  //nolint:errcheck
	binary.Write(w, binary.BigEndian, m.PrefetchCount) //nolint:errcheck
	return writeBit(w, m.Global)
}

func decodeBasicQos(r *bytes.Reader) (Method, error) {
	var m BasicQos
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

func (m BasicQosOk) ClassID() uint16  { return classBasic }
func (m BasicQosOk) MethodID() uint16 { return 11 }
func (m BasicQosOk) marshal(w *bytes.Buffer) error {
	return nil
}

func (m BasicConsume) ClassID() uint16  { return classBasic }
func (m BasicConsume) MethodID() uint16 { return 20 }
func (m BasicConsume) marshal(w *bytes.Buffer) error {
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

func decodeBasicConsume(r *bytes.Reader) (Method, error) {
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
	return BasicConsume{
		Queue:       queue,
		ConsumerTag: consumerTag,
		NoLocal:     bit(bits, 0),
		NoAck:       bit(bits, 1),
		Exclusive:   bit(bits, 2),
		NoWait:      bit(bits, 3),
		Arguments:   args,
	}, nil
}

func (m BasicConsumeOk) ClassID() uint16  { return classBasic }
func (m BasicConsumeOk) MethodID() uint16 { return 21 }
func (m BasicConsumeOk) marshal(w *bytes.Buffer) error {
	return writeShortstr(w, m.ConsumerTag)
}

func decodeBasicConsumeOk(r *bytes.Reader) (Method, error) {
	consumerTag, err := readShortstr(r)
	if err != nil {
		return nil, err
	}
	return BasicConsumeOk{ConsumerTag: consumerTag}, nil
}

func (m BasicCancel) ClassID() uint16  { return classBasic }
func (m BasicCancel) MethodID() uint16 { return 30 }
func (m BasicCancel) marshal(w *bytes.Buffer) error {
	if err := writeShortstr(w, m.ConsumerTag); err != nil {
		return err
	}
	return writeBit(w, m.NoWait)
}

func decodeBasicCancel(r *bytes.Reader) (Method, error) {
	consumerTag, err := readShortstr(r)
	if err != nil {
		return nil, err
	}
	bits, err := readBits(r)
	if err != nil {
		return nil, err
	}
	return BasicCancel{ConsumerTag: consumerTag, NoWait: bit(bits, 0)}, nil
}

func (m BasicCancelOk) ClassID() uint16  { return classBasic }
func (m BasicCancelOk) MethodID() uint16 { return 31 }
func (m BasicCancelOk) marshal(w *bytes.Buffer) error {
	return writeShortstr(w, m.ConsumerTag)
}

func decodeBasicCancelOk(r *bytes.Reader) (Method, error) {
	consumerTag, err := readShortstr(r)
	if err != nil {
		return nil, err
	}
	return BasicCancelOk{ConsumerTag: consumerTag}, nil
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

func decodeBasicPublish(r *bytes.Reader) (Method, error) {
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

func decodeBasicDeliver(r *bytes.Reader) (Method, error) {
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

func decodeBasicAck(r *bytes.Reader) (Method, error) {
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

func decodeBasicReject(r *bytes.Reader) (Method, error) {
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

func decodeBasicNack(r *bytes.Reader) (Method, error) {
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

func (m ConfirmSelect) ClassID() uint16  { return classConfirm }
func (m ConfirmSelect) MethodID() uint16 { return 10 }
func (m ConfirmSelect) marshal(w *bytes.Buffer) error {
	return writeBit(w, m.NoWait)
}

func decodeConfirmSelect(r *bytes.Reader) (Method, error) {
	bits, err := readBits(r)
	if err != nil {
		return nil, err
	}
	return ConfirmSelect{NoWait: bit(bits, 0)}, nil
}

func (m ConfirmSelectOk) ClassID() uint16  { return classConfirm }
func (m ConfirmSelectOk) MethodID() uint16 { return 11 }
func (m ConfirmSelectOk) marshal(w *bytes.Buffer) error {
	return nil
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
