package store

import (
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

func init() {
	for _, v := range []any{
		map[string]any(nil), []any(nil), []byte(nil), time.Time{},
		int8(0), int16(0), int32(0), int64(0), uint8(0), float32(0), float64(0),
	} {
		gob.Register(v)
	}
}

type snapshot struct {
	NextTag uint64
	Ready   []Message
	Unacked map[uint64]Message
}

// DurableMessageStore persists queue state as a gob-encoded snapshot file.
// Each mutation atomically rewrites the file via temp-file + rename.
type DurableMessageStore struct {
	mu    sync.Mutex
	path  string
	state queueState
}

func NewDurableMessageStore(path string) (*DurableMessageStore, error) {
	if path == "" {
		return nil, errors.New("store: snapshot path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	s := &DurableMessageStore{path: path, state: newQueueState()}
	if err := s.load(); err != nil {
		return nil, err
	}
	// Recover any in-flight messages back to the ready queue.
	tags := make([]uint64, 0, len(s.state.unacked))
	for tag := range s.state.unacked {
		tags = append(tags, tag)
	}
	sort.Slice(tags, func(i, j int) bool { return tags[i] < tags[j] })
	for i := len(tags) - 1; i >= 0; i-- {
		s.state.nack(tags[i], true)
	}
	return s, nil
}

func (s *DurableMessageStore) Enqueue(msg Message) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	msg = s.state.enqueue(msg)
	if err := s.saveLocked(); err != nil {
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
	if err := s.saveLocked(); err != nil {
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
	if err := s.saveLocked(); err != nil {
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
	if err := s.saveLocked(); err != nil {
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
	return s.saveLocked()
}

func (s *DurableMessageStore) Destroy() error {
	if err := os.Remove(s.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (s *DurableMessageStore) load() error {
	f, err := os.Open(s.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer f.Close()
	var snap snapshot
	if err := gob.NewDecoder(f).Decode(&snap); err != nil {
		return fmt.Errorf("store: load snapshot: %w", err)
	}
	s.state.nextTag = snap.NextTag
	s.state.ready = snap.Ready
	for tag, msg := range snap.Unacked {
		s.state.unacked[tag] = msg
	}
	return nil
}

func (s *DurableMessageStore) saveLocked() error {
	tmp := s.path + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	snap := snapshot{NextTag: s.state.nextTag, Ready: s.state.ready, Unacked: s.state.unacked}
	if err := gob.NewEncoder(f).Encode(&snap); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, s.path)
}
