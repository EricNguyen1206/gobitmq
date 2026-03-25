package store

import "fmt"

type queueState struct {
	nextTag uint64
	ready   []Message
	unacked map[uint64]Message
}

func newQueueState() queueState {
	return queueState{nextTag: 1, ready: make([]Message, 0), unacked: make(map[uint64]Message)}
}

func (s *queueState) enqueue(msg Message) Message {
	msg = CloneMessage(msg)
	msg.DeliveryTag = s.nextTag
	s.nextTag++
	s.ready = append(s.ready, msg)
	return msg
}

func (s *queueState) rollbackEnqueue(tag uint64) {
	if n := len(s.ready); n > 0 && s.ready[n-1].DeliveryTag == tag {
		s.ready = s.ready[:n-1]
	}
	if tag+1 == s.nextTag {
		s.nextTag--
	}
}

func (s *queueState) enqueueRecovered(msg Message) {
	if msg.DeliveryTag >= s.nextTag {
		s.nextTag = msg.DeliveryTag + 1
	}
	s.ready = append(s.ready, msg)
}

func (s *queueState) dequeue() (Message, bool) {
	if len(s.ready) == 0 {
		return Message{}, false
	}
	msg := s.ready[0]
	s.ready = s.ready[1:]
	s.unacked[msg.DeliveryTag] = msg
	return CloneMessage(msg), true
}

func (s *queueState) restoreDequeue(msg Message) {
	delete(s.unacked, msg.DeliveryTag)
	s.ready = append([]Message{CloneMessage(msg)}, s.ready...)
}

func (s *queueState) dequeueByTag(tag uint64) error {
	for i, msg := range s.ready {
		if msg.DeliveryTag == tag {
			s.ready = append(s.ready[:i], s.ready[i+1:]...)
			s.unacked[tag] = msg
			return nil
		}
	}
	return fmt.Errorf("ready message %d not found", tag)
}

func (s *queueState) ack(tag uint64) (Message, error) {
	msg, ok := s.unacked[tag]
	if !ok {
		return Message{}, fmt.Errorf("unknown delivery tag %d", tag)
	}
	delete(s.unacked, tag)
	return msg, nil
}

func (s *queueState) restoreAck(msg Message) {
	s.unacked[msg.DeliveryTag] = msg
}

func (s *queueState) nack(tag uint64, requeue bool) (Message, error) {
	msg, ok := s.unacked[tag]
	if !ok {
		return Message{}, fmt.Errorf("unknown delivery tag %d", tag)
	}
	delete(s.unacked, tag)
	original := CloneMessage(msg)
	if requeue {
		msg.Redelivered = true
		s.ready = append([]Message{msg}, s.ready...)
	}
	return original, nil
}

func (s *queueState) restoreNack(msg Message, requeue bool) {
	if requeue && len(s.ready) > 0 {
		s.ready = s.ready[1:]
	}
	s.unacked[msg.DeliveryTag] = msg
}

func (s *queueState) len() int { return len(s.ready) }

func (s *queueState) stats() QueueStats {
	return QueueStats{Ready: len(s.ready), Unacked: len(s.unacked)}
}
