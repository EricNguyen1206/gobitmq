package store

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

const (
	walOpEnqueue = "enqueue"
	walOpDequeue = "dequeue"
	walOpAck     = "ack"
	walOpNack    = "nack"
)

type DurableMessageStore struct {
	mu      sync.Mutex
	path    string
	file    *os.File
	encoder *json.Encoder
	state   queueState
}

type walRecord struct {
	Op          string            `json:"op"`
	DeliveryTag uint64            `json:"delivery_tag,omitempty"`
	Requeue     bool              `json:"requeue,omitempty"`
	Message     *persistedMessage `json:"message,omitempty"`
}

type persistedMessage struct {
	DeliveryTag   uint64                    `json:"delivery_tag"`
	Exchange      string                    `json:"exchange,omitempty"`
	RoutingKey    string                    `json:"routing_key,omitempty"`
	ContentType   string                    `json:"content_type,omitempty"`
	CorrelationID string                    `json:"correlation_id,omitempty"`
	ReplyTo       string                    `json:"reply_to,omitempty"`
	DeliveryMode  uint8                     `json:"delivery_mode,omitempty"`
	Headers       map[string]persistedValue `json:"headers,omitempty"`
	Body          []byte                    `json:"body,omitempty"`
	Redelivered   bool                      `json:"redelivered,omitempty"`
}

type persistedValue struct {
	Kind    string                    `json:"kind"`
	Bool    bool                      `json:"bool,omitempty"`
	String  string                    `json:"string,omitempty"`
	Bytes   string                    `json:"bytes,omitempty"`
	Int64   int64                     `json:"int64,omitempty"`
	Uint64  uint64                    `json:"uint64,omitempty"`
	Float64 float64                   `json:"float64,omitempty"`
	Time    int64                     `json:"time,omitempty"`
	Table   map[string]persistedValue `json:"table,omitempty"`
	Array   []persistedValue          `json:"array,omitempty"`
}

func NewDurableMessageStore(path string) (*DurableMessageStore, error) {
	if path == "" {
		return nil, errors.New("store: durable WAL path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}

	s := &DurableMessageStore{
		path:  path,
		file:  file,
		state: newQueueState(),
	}

	if err := s.replay(); err != nil {
		_ = file.Close()
		return nil, err
	}
	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		_ = file.Close()
		return nil, err
	}
	s.encoder = json.NewEncoder(file)
	if err := s.recoverUnackedLocked(); err != nil {
		_ = file.Close()
		return nil, err
	}

	return s, nil
}

func (s *DurableMessageStore) Enqueue(msg Message) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	msg = s.state.enqueue(msg)
	record, err := newEnqueueRecord(msg)
	if err != nil {
		s.state.rollbackEnqueue(msg.DeliveryTag)
		return 0, err
	}
	if err := s.appendRecordLocked(record); err != nil {
		s.state.rollbackEnqueue(msg.DeliveryTag)
		return 0, err
	}
	return msg.DeliveryTag, nil
}

func (s *DurableMessageStore) Dequeue() (Message, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	msg, ok := s.state.dequeue()
	if !ok {
		return Message{}, false, nil
	}
	if err := s.appendRecordLocked(walRecord{Op: walOpDequeue, DeliveryTag: msg.DeliveryTag}); err != nil {
		s.state.restoreDequeue(msg)
		return Message{}, false, err
	}
	return msg, true, nil
}

func (s *DurableMessageStore) Ack(deliveryTag uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	msg, err := s.state.ack(deliveryTag)
	if err != nil {
		return err
	}
	if err := s.appendRecordLocked(walRecord{Op: walOpAck, DeliveryTag: deliveryTag}); err != nil {
		s.state.restoreAck(msg)
		return err
	}
	return nil
}

func (s *DurableMessageStore) Nack(deliveryTag uint64, requeue bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	original, err := s.state.nack(deliveryTag, requeue)
	if err != nil {
		return err
	}
	if err := s.appendRecordLocked(walRecord{Op: walOpNack, DeliveryTag: deliveryTag, Requeue: requeue}); err != nil {
		s.state.restoreNack(original, requeue)
		return err
	}
	return nil
}

func (s *DurableMessageStore) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.len()
}

func (s *DurableMessageStore) Stats() QueueStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.stats()
}

func (s *DurableMessageStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeLocked()
}

func (s *DurableMessageStore) Destroy() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.closeLocked(); err != nil {
		return err
	}
	if err := os.Remove(s.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (s *DurableMessageStore) closeLocked() error {
	if s.file == nil {
		return nil
	}
	err := s.file.Close()
	s.file = nil
	s.encoder = nil
	return err
}

func (s *DurableMessageStore) replay() error {
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return err
	}

	decoder := json.NewDecoder(s.file)
	for {
		var record walRecord
		if err := decoder.Decode(&record); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return fmt.Errorf("store: replay WAL: %w", err)
		}
		if err := s.applyRecordLocked(record); err != nil {
			return fmt.Errorf("store: replay WAL: %w", err)
		}
	}
}

func (s *DurableMessageStore) recoverUnackedLocked() error {
	if len(s.state.unacked) == 0 {
		return nil
	}
	tags := make([]uint64, 0, len(s.state.unacked))
	for tag := range s.state.unacked {
		tags = append(tags, tag)
	}
	sort.Slice(tags, func(i, j int) bool { return tags[i] < tags[j] })
	for i := len(tags) - 1; i >= 0; i-- {
		if _, err := s.state.nack(tags[i], true); err != nil {
			return err
		}
		if err := s.appendRecordLocked(walRecord{Op: walOpNack, DeliveryTag: tags[i], Requeue: true}); err != nil {
			return err
		}
	}
	return nil
}

func (s *DurableMessageStore) applyRecordLocked(record walRecord) error {
	switch record.Op {
	case walOpEnqueue:
		if record.Message == nil {
			return errors.New("enqueue record missing message")
		}
		msg, err := decodePersistedMessage(*record.Message)
		if err != nil {
			return err
		}
		s.state.enqueueRecovered(msg)
		return nil
	case walOpDequeue:
		return s.state.dequeueByTag(record.DeliveryTag)
	case walOpAck:
		_, err := s.state.ack(record.DeliveryTag)
		return err
	case walOpNack:
		_, err := s.state.nack(record.DeliveryTag, record.Requeue)
		return err
	default:
		return fmt.Errorf("unknown WAL op %q", record.Op)
	}
}

func (s *DurableMessageStore) appendRecordLocked(record walRecord) error {
	if s.encoder == nil || s.file == nil {
		return errors.New("store: durable WAL is closed")
	}
	if err := s.encoder.Encode(record); err != nil {
		return err
	}
	return s.file.Sync()
}

func newEnqueueRecord(msg Message) (walRecord, error) {
	persisted, err := encodePersistedMessage(msg)
	if err != nil {
		return walRecord{}, err
	}
	return walRecord{Op: walOpEnqueue, Message: &persisted}, nil
}

func encodePersistedMessage(msg Message) (persistedMessage, error) {
	persisted := persistedMessage{
		DeliveryTag:   msg.DeliveryTag,
		Exchange:      msg.Exchange,
		RoutingKey:    msg.RoutingKey,
		ContentType:   msg.ContentType,
		CorrelationID: msg.CorrelationID,
		ReplyTo:       msg.ReplyTo,
		DeliveryMode:  msg.DeliveryMode,
		Body:          append([]byte(nil), msg.Body...),
		Redelivered:   msg.Redelivered,
	}
	if len(msg.Headers) > 0 {
		headers, err := encodePersistedHeaders(msg.Headers)
		if err != nil {
			return persistedMessage{}, err
		}
		persisted.Headers = headers
	}
	return persisted, nil
}

func decodePersistedMessage(msg persistedMessage) (Message, error) {
	decoded := Message{
		DeliveryTag:   msg.DeliveryTag,
		Exchange:      msg.Exchange,
		RoutingKey:    msg.RoutingKey,
		ContentType:   msg.ContentType,
		CorrelationID: msg.CorrelationID,
		ReplyTo:       msg.ReplyTo,
		DeliveryMode:  msg.DeliveryMode,
		Body:          append([]byte(nil), msg.Body...),
		Redelivered:   msg.Redelivered,
	}
	if len(msg.Headers) > 0 {
		headers, err := decodePersistedHeaders(msg.Headers)
		if err != nil {
			return Message{}, err
		}
		decoded.Headers = headers
	}
	return decoded, nil
}

func encodePersistedHeaders(headers map[string]any) (map[string]persistedValue, error) {
	encoded := make(map[string]persistedValue, len(headers))
	for key, value := range headers {
		persisted, err := encodePersistedValue(value)
		if err != nil {
			return nil, fmt.Errorf("header %q: %w", key, err)
		}
		encoded[key] = persisted
	}
	return encoded, nil
}

func decodePersistedHeaders(headers map[string]persistedValue) (map[string]any, error) {
	decoded := make(map[string]any, len(headers))
	for key, value := range headers {
		decodedValue, err := decodePersistedValue(value)
		if err != nil {
			return nil, fmt.Errorf("header %q: %w", key, err)
		}
		decoded[key] = decodedValue
	}
	return decoded, nil
}

func encodePersistedValue(value any) (persistedValue, error) {
	switch v := value.(type) {
	case nil:
		return persistedValue{Kind: "null"}, nil
	case bool:
		return persistedValue{Kind: "bool", Bool: v}, nil
	case string:
		return persistedValue{Kind: "string", String: v}, nil
	case []byte:
		return persistedValue{Kind: "bytes", Bytes: base64.StdEncoding.EncodeToString(v)}, nil
	case map[string]any:
		table, err := encodePersistedHeaders(v)
		if err != nil {
			return persistedValue{}, err
		}
		return persistedValue{Kind: "table", Table: table}, nil
	case []any:
		array := make([]persistedValue, 0, len(v))
		for _, item := range v {
			persisted, err := encodePersistedValue(item)
			if err != nil {
				return persistedValue{}, err
			}
			array = append(array, persisted)
		}
		return persistedValue{Kind: "array", Array: array}, nil
	case int8:
		return persistedValue{Kind: "int8", Int64: int64(v)}, nil
	case uint8:
		return persistedValue{Kind: "uint8", Uint64: uint64(v)}, nil
	case int16:
		return persistedValue{Kind: "int16", Int64: int64(v)}, nil
	case int32:
		return persistedValue{Kind: "int32", Int64: int64(v)}, nil
	case int:
		return persistedValue{Kind: "int", Int64: int64(v)}, nil
	case int64:
		return persistedValue{Kind: "int64", Int64: v}, nil
	case float32:
		return persistedValue{Kind: "float32", Float64: float64(v)}, nil
	case float64:
		return persistedValue{Kind: "float64", Float64: v}, nil
	case time.Time:
		return persistedValue{Kind: "time", Time: v.UTC().Unix()}, nil
	default:
		return persistedValue{}, fmt.Errorf("unsupported persisted value type %T", value)
	}
}

func decodePersistedValue(value persistedValue) (any, error) {
	switch value.Kind {
	case "null":
		return nil, nil
	case "bool":
		return value.Bool, nil
	case "string":
		return value.String, nil
	case "bytes":
		decoded, err := base64.StdEncoding.DecodeString(value.Bytes)
		if err != nil {
			return nil, err
		}
		return decoded, nil
	case "table":
		return decodePersistedHeaders(value.Table)
	case "array":
		decoded := make([]any, 0, len(value.Array))
		for _, item := range value.Array {
			decodedValue, err := decodePersistedValue(item)
			if err != nil {
				return nil, err
			}
			decoded = append(decoded, decodedValue)
		}
		return decoded, nil
	case "int8":
		return int8(value.Int64), nil
	case "uint8":
		return uint8(value.Uint64), nil
	case "int16":
		return int16(value.Int64), nil
	case "int32":
		return int32(value.Int64), nil
	case "int":
		return int(value.Int64), nil
	case "int64":
		return value.Int64, nil
	case "float32":
		return float32(value.Float64), nil
	case "float64":
		return value.Float64, nil
	case "time":
		return time.Unix(value.Time, 0).UTC(), nil
	default:
		return nil, fmt.Errorf("unsupported persisted value kind %q", value.Kind)
	}
}
