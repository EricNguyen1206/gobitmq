package amqp

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"sort"
	"time"
)

const (
	FrameMethod    byte = 1
	FrameHeader    byte = 2
	FrameBody      byte = 3
	FrameHeartbeat byte = 8
	FrameEnd       byte = 0xCE

	DefaultAddr     = ":5672"
	defaultFrameMax = 131072
	defaultLocale   = "en_US"
)

var protocolHeader = []byte{'A', 'M', 'Q', 'P', 0, 0, 9, 1}

type Frame struct {
	Type    byte
	Channel uint16
	Payload []byte
}

type Table map[string]any

type BasicProperties struct {
	ContentType     string
	ContentEncoding string
	Headers         Table
	DeliveryMode    uint8
	Priority        uint8
	CorrelationID   string
	ReplyTo         string
	Expiration      string
	MessageID       string
	Timestamp       uint64
	Type            string
	UserID          string
	AppID           string
	ClusterID       string
}

type ContentHeader struct {
	ClassID    uint16
	Weight     uint16
	BodySize   uint64
	Properties BasicProperties
}

func ReadProtocolHeader(r io.Reader) error {
	head := make([]byte, len(protocolHeader))
	if _, err := io.ReadFull(r, head); err != nil {
		return err
	}
	if !bytes.Equal(head, protocolHeader) {
		return fmt.Errorf("amqp: invalid protocol header %q", head)
	}
	return nil
}

func WriteProtocolHeader(w io.Writer) error {
	_, err := w.Write(protocolHeader)
	return err
}

func ReadFrame(r io.Reader) (Frame, error) {
	var header [7]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return Frame{}, err
	}

	size := binary.BigEndian.Uint32(header[3:])
	payload := make([]byte, size)
	if _, err := io.ReadFull(r, payload); err != nil {
		return Frame{}, err
	}

	var end [1]byte
	if _, err := io.ReadFull(r, end[:]); err != nil {
		return Frame{}, err
	}
	if end[0] != FrameEnd {
		return Frame{}, fmt.Errorf("amqp: invalid frame terminator %#x", end[0])
	}

	return Frame{
		Type:    header[0],
		Channel: binary.BigEndian.Uint16(header[1:3]),
		Payload: payload,
	}, nil
}

func WriteFrame(w io.Writer, frame Frame) error {
	if uint64(len(frame.Payload)) > math.MaxUint32 {
		return fmt.Errorf("amqp: frame payload too large: %d", len(frame.Payload))
	}

	var header [7]byte
	header[0] = frame.Type
	binary.BigEndian.PutUint16(header[1:3], frame.Channel)
	binary.BigEndian.PutUint32(header[3:], uint32(len(frame.Payload)))

	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	if _, err := w.Write(frame.Payload); err != nil {
		return err
	}
	_, err := w.Write([]byte{FrameEnd})
	return err
}

func EncodeContentHeaderFrame(channel uint16, header ContentHeader) (Frame, error) {
	var payload bytes.Buffer
	binary.Write(&payload, binary.BigEndian, header.ClassID)  //nolint:errcheck
	binary.Write(&payload, binary.BigEndian, header.Weight)   //nolint:errcheck
	binary.Write(&payload, binary.BigEndian, header.BodySize) //nolint:errcheck

	flags, err := encodePropertyFlags(header.Properties)
	if err != nil {
		return Frame{}, err
	}
	binary.Write(&payload, binary.BigEndian, flags) //nolint:errcheck
	if err := writeProperties(&payload, header.Properties); err != nil {
		return Frame{}, err
	}

	return Frame{Type: FrameHeader, Channel: channel, Payload: payload.Bytes()}, nil
}

func DecodeContentHeaderFrame(frame Frame) (ContentHeader, error) {
	if frame.Type != FrameHeader {
		return ContentHeader{}, fmt.Errorf("amqp: expected header frame, got type %d", frame.Type)
	}

	r := bytes.NewReader(frame.Payload)
	var header ContentHeader
	if err := binary.Read(r, binary.BigEndian, &header.ClassID); err != nil {
		return ContentHeader{}, err
	}
	if err := binary.Read(r, binary.BigEndian, &header.Weight); err != nil {
		return ContentHeader{}, err
	}
	if err := binary.Read(r, binary.BigEndian, &header.BodySize); err != nil {
		return ContentHeader{}, err
	}

	flags, err := readPropertyFlags(r)
	if err != nil {
		return ContentHeader{}, err
	}
	props, err := readProperties(r, flags)
	if err != nil {
		return ContentHeader{}, err
	}
	header.Properties = props

	return header, nil
}

func writeShortstr(w *bytes.Buffer, s string) error {
	if len(s) > math.MaxUint8 {
		return fmt.Errorf("amqp: shortstr too long: %d", len(s))
	}
	w.WriteByte(byte(len(s)))
	_, err := w.WriteString(s)
	return err
}

func readShortstr(r *bytes.Reader) (string, error) {
	n, err := r.ReadByte()
	if err != nil {
		return "", err
	}
	b := make([]byte, int(n))
	if _, err := io.ReadFull(r, b); err != nil {
		return "", err
	}
	return string(b), nil
}

func writeLongstr(w *bytes.Buffer, data []byte) error {
	if uint64(len(data)) > math.MaxUint32 {
		return fmt.Errorf("amqp: longstr too long: %d", len(data))
	}
	binary.Write(w, binary.BigEndian, uint32(len(data))) //nolint:errcheck
	_, err := w.Write(data)
	return err
}

func writeLongstrString(w *bytes.Buffer, s string) error {
	return writeLongstr(w, []byte(s))
}

func readLongstr(r *bytes.Reader) ([]byte, error) {
	var n uint32
	if err := binary.Read(r, binary.BigEndian, &n); err != nil {
		return nil, err
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(r, b); err != nil {
		return nil, err
	}
	return b, nil
}

func writeTable(w *bytes.Buffer, table Table) error {
	var payload bytes.Buffer
	keys := make([]string, 0, len(table))
	for key := range table {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		if err := writeShortstr(&payload, key); err != nil {
			return err
		}
		if err := writeFieldValue(&payload, table[key]); err != nil {
			return fmt.Errorf("amqp: write field %q: %w", key, err)
		}
	}

	binary.Write(w, binary.BigEndian, uint32(payload.Len())) //nolint:errcheck
	_, err := w.Write(payload.Bytes())
	return err
}

func readTable(r *bytes.Reader) (Table, error) {
	var size uint32
	if err := binary.Read(r, binary.BigEndian, &size); err != nil {
		return nil, err
	}

	data := make([]byte, size)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}

	table := make(Table)
	inner := bytes.NewReader(data)
	for inner.Len() > 0 {
		key, err := readShortstr(inner)
		if err != nil {
			return nil, err
		}
		value, err := readFieldValue(inner)
		if err != nil {
			return nil, fmt.Errorf("amqp: read field %q: %w", key, err)
		}
		table[key] = value
	}

	return table, nil
}

func writeFieldValue(w *bytes.Buffer, value any) error {
	switch v := value.(type) {
	case nil:
		w.WriteByte('V')
		return nil
	case bool:
		w.WriteByte('t')
		if v {
			w.WriteByte(1)
		} else {
			w.WriteByte(0)
		}
		return nil
	case string:
		w.WriteByte('S')
		return writeLongstrString(w, v)
	case []byte:
		w.WriteByte('x')
		return writeLongstr(w, v)
	case Table:
		w.WriteByte('F')
		return writeTable(w, v)
	case map[string]any:
		w.WriteByte('F')
		return writeTable(w, Table(v))
	case []any:
		w.WriteByte('A')
		return writeArray(w, v)
	case int8:
		w.WriteByte('b')
		return binary.Write(w, binary.BigEndian, v)
	case uint8:
		w.WriteByte('B')
		return binary.Write(w, binary.BigEndian, v)
	case int16:
		w.WriteByte('s')
		return binary.Write(w, binary.BigEndian, v)
	case int32:
		w.WriteByte('I')
		return binary.Write(w, binary.BigEndian, v)
	case int:
		w.WriteByte('I')
		return binary.Write(w, binary.BigEndian, int32(v))
	case int64:
		w.WriteByte('l')
		return binary.Write(w, binary.BigEndian, v)
	case float32:
		w.WriteByte('f')
		return binary.Write(w, binary.BigEndian, v)
	case float64:
		w.WriteByte('d')
		return binary.Write(w, binary.BigEndian, v)
	case time.Time:
		w.WriteByte('T')
		return binary.Write(w, binary.BigEndian, uint64(v.Unix()))
	default:
		return fmt.Errorf("unsupported field value type %T", value)
	}
}

func readFieldValue(r *bytes.Reader) (any, error) {
	kind, err := r.ReadByte()
	if err != nil {
		return nil, err
	}

	switch kind {
	case 'V':
		return nil, nil
	case 't':
		b, err := r.ReadByte()
		if err != nil {
			return nil, err
		}
		return b != 0, nil
	case 'b':
		var v int8
		return v, binary.Read(r, binary.BigEndian, &v)
	case 'B':
		var v uint8
		return v, binary.Read(r, binary.BigEndian, &v)
	case 's':
		var v int16
		return v, binary.Read(r, binary.BigEndian, &v)
	case 'I':
		var v int32
		return v, binary.Read(r, binary.BigEndian, &v)
	case 'l':
		var v int64
		return v, binary.Read(r, binary.BigEndian, &v)
	case 'f':
		var v float32
		return v, binary.Read(r, binary.BigEndian, &v)
	case 'd':
		var v float64
		return v, binary.Read(r, binary.BigEndian, &v)
	case 'S':
		b, err := readLongstr(r)
		return string(b), err
	case 'x':
		return readLongstr(r)
	case 'A':
		return readArray(r)
	case 'T':
		var v uint64
		return v, binary.Read(r, binary.BigEndian, &v)
	case 'F':
		return readTable(r)
	default:
		return nil, fmt.Errorf("unsupported field kind %q", kind)
	}
}

func writeArray(w *bytes.Buffer, values []any) error {
	var payload bytes.Buffer
	for _, value := range values {
		if err := writeFieldValue(&payload, value); err != nil {
			return err
		}
	}
	binary.Write(w, binary.BigEndian, uint32(payload.Len())) //nolint:errcheck
	_, err := w.Write(payload.Bytes())
	return err
}

func readArray(r *bytes.Reader) ([]any, error) {
	var size uint32
	if err := binary.Read(r, binary.BigEndian, &size); err != nil {
		return nil, err
	}

	data := make([]byte, size)
	if _, err := io.ReadFull(r, data); err != nil {
		return nil, err
	}

	inner := bytes.NewReader(data)
	values := make([]any, 0)
	for inner.Len() > 0 {
		value, err := readFieldValue(inner)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}

func encodePropertyFlags(props BasicProperties) (uint16, error) {
	var flags uint16
	if props.ContentType != "" {
		flags |= 1 << 15
	}
	if props.ContentEncoding != "" {
		flags |= 1 << 14
	}
	if len(props.Headers) > 0 {
		flags |= 1 << 13
	}
	if props.DeliveryMode != 0 {
		flags |= 1 << 12
	}
	if props.Priority != 0 {
		flags |= 1 << 11
	}
	if props.CorrelationID != "" {
		flags |= 1 << 10
	}
	if props.ReplyTo != "" {
		flags |= 1 << 9
	}
	if props.Expiration != "" {
		flags |= 1 << 8
	}
	if props.MessageID != "" {
		flags |= 1 << 7
	}
	if props.Timestamp != 0 {
		flags |= 1 << 6
	}
	if props.Type != "" {
		flags |= 1 << 5
	}
	if props.UserID != "" {
		flags |= 1 << 4
	}
	if props.AppID != "" {
		flags |= 1 << 3
	}
	if props.ClusterID != "" {
		flags |= 1 << 2
	}
	return flags, nil
}

func readPropertyFlags(r *bytes.Reader) (uint16, error) {
	var flags uint16
	if err := binary.Read(r, binary.BigEndian, &flags); err != nil {
		return 0, err
	}
	if flags&1 != 0 {
		return 0, fmt.Errorf("amqp: continuation property flags are not supported")
	}
	return flags, nil
}

func writeProperties(w *bytes.Buffer, props BasicProperties) error {
	if props.ContentType != "" {
		if err := writeShortstr(w, props.ContentType); err != nil {
			return err
		}
	}
	if props.ContentEncoding != "" {
		if err := writeShortstr(w, props.ContentEncoding); err != nil {
			return err
		}
	}
	if len(props.Headers) > 0 {
		if err := writeTable(w, props.Headers); err != nil {
			return err
		}
	}
	if props.DeliveryMode != 0 {
		w.WriteByte(props.DeliveryMode)
	}
	if props.Priority != 0 {
		w.WriteByte(props.Priority)
	}
	if props.CorrelationID != "" {
		if err := writeShortstr(w, props.CorrelationID); err != nil {
			return err
		}
	}
	if props.ReplyTo != "" {
		if err := writeShortstr(w, props.ReplyTo); err != nil {
			return err
		}
	}
	if props.Expiration != "" {
		if err := writeShortstr(w, props.Expiration); err != nil {
			return err
		}
	}
	if props.MessageID != "" {
		if err := writeShortstr(w, props.MessageID); err != nil {
			return err
		}
	}
	if props.Timestamp != 0 {
		binary.Write(w, binary.BigEndian, props.Timestamp) //nolint:errcheck
	}
	if props.Type != "" {
		if err := writeShortstr(w, props.Type); err != nil {
			return err
		}
	}
	if props.UserID != "" {
		if err := writeShortstr(w, props.UserID); err != nil {
			return err
		}
	}
	if props.AppID != "" {
		if err := writeShortstr(w, props.AppID); err != nil {
			return err
		}
	}
	if props.ClusterID != "" {
		if err := writeShortstr(w, props.ClusterID); err != nil {
			return err
		}
	}
	return nil
}

func readProperties(r *bytes.Reader, flags uint16) (BasicProperties, error) {
	var props BasicProperties
	var err error

	if flags&(1<<15) != 0 {
		props.ContentType, err = readShortstr(r)
		if err != nil {
			return BasicProperties{}, err
		}
	}
	if flags&(1<<14) != 0 {
		props.ContentEncoding, err = readShortstr(r)
		if err != nil {
			return BasicProperties{}, err
		}
	}
	if flags&(1<<13) != 0 {
		props.Headers, err = readTable(r)
		if err != nil {
			return BasicProperties{}, err
		}
	}
	if flags&(1<<12) != 0 {
		props.DeliveryMode, err = r.ReadByte()
		if err != nil {
			return BasicProperties{}, err
		}
	}
	if flags&(1<<11) != 0 {
		props.Priority, err = r.ReadByte()
		if err != nil {
			return BasicProperties{}, err
		}
	}
	if flags&(1<<10) != 0 {
		props.CorrelationID, err = readShortstr(r)
		if err != nil {
			return BasicProperties{}, err
		}
	}
	if flags&(1<<9) != 0 {
		props.ReplyTo, err = readShortstr(r)
		if err != nil {
			return BasicProperties{}, err
		}
	}
	if flags&(1<<8) != 0 {
		props.Expiration, err = readShortstr(r)
		if err != nil {
			return BasicProperties{}, err
		}
	}
	if flags&(1<<7) != 0 {
		props.MessageID, err = readShortstr(r)
		if err != nil {
			return BasicProperties{}, err
		}
	}
	if flags&(1<<6) != 0 {
		if err := binary.Read(r, binary.BigEndian, &props.Timestamp); err != nil {
			return BasicProperties{}, err
		}
	}
	if flags&(1<<5) != 0 {
		props.Type, err = readShortstr(r)
		if err != nil {
			return BasicProperties{}, err
		}
	}
	if flags&(1<<4) != 0 {
		props.UserID, err = readShortstr(r)
		if err != nil {
			return BasicProperties{}, err
		}
	}
	if flags&(1<<3) != 0 {
		props.AppID, err = readShortstr(r)
		if err != nil {
			return BasicProperties{}, err
		}
	}
	if flags&(1<<2) != 0 {
		props.ClusterID, err = readShortstr(r)
		if err != nil {
			return BasicProperties{}, err
		}
	}

	return props, nil
}
