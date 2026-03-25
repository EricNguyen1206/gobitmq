package amqpcore

import "sync/atomic"

type brokerMetrics struct {
	publish   atomic.Uint64
	deliver   atomic.Uint64
	ack       atomic.Uint64
	nack      atomic.Uint64
	reject    atomic.Uint64
	redeliver atomic.Uint64
}

type MessageStats struct {
	Publish   uint64 `json:"publish"`
	Deliver   uint64 `json:"deliver"`
	Ack       uint64 `json:"ack"`
	Nack      uint64 `json:"nack"`
	Reject    uint64 `json:"reject"`
	Redeliver uint64 `json:"redeliver"`
}

func (b *Broker) RecordPublish() {
	b.metrics.publish.Add(1)
}

func (b *Broker) RecordDeliver() {
	b.metrics.deliver.Add(1)
}

func (b *Broker) RecordAck() {
	b.metrics.ack.Add(1)
}

func (b *Broker) RecordNack() {
	b.metrics.nack.Add(1)
}

func (b *Broker) RecordReject() {
	b.metrics.reject.Add(1)
}

func (b *Broker) RecordRedeliver() {
	b.metrics.redeliver.Add(1)
}

func (b *Broker) snapshotStats() MessageStats {
	return MessageStats{
		Publish:   b.metrics.publish.Load(),
		Deliver:   b.metrics.deliver.Load(),
		Ack:       b.metrics.ack.Load(),
		Nack:      b.metrics.nack.Load(),
		Reject:    b.metrics.reject.Load(),
		Redeliver: b.metrics.redeliver.Load(),
	}
}
