package core

import (
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"erionn-mq/internal/store"
)

type durableBrokerState struct {
	Exchanges []durableExchangeState `json:"exchanges,omitempty"`
	Queues    []durableQueueState    `json:"queues,omitempty"`
	Bindings  []durableBindingState  `json:"bindings,omitempty"`
}

type durableExchangeState struct {
	Name       string       `json:"name"`
	Type       ExchangeType `json:"type"`
	Durable    bool         `json:"durable"`
	AutoDelete bool         `json:"auto_delete,omitempty"`
	Internal   bool         `json:"internal,omitempty"`
}

type durableQueueState struct {
	Name       string         `json:"name"`
	Durable    bool           `json:"durable"`
	Exclusive  bool           `json:"exclusive,omitempty"`
	AutoDelete bool           `json:"auto_delete,omitempty"`
	Args       map[string]any `json:"args,omitempty"`
}

type durableBindingState struct {
	ExchangeName string         `json:"exchange_name"`
	QueueName    string         `json:"queue_name"`
	RoutingKey   string         `json:"routing_key,omitempty"`
	Args         map[string]any `json:"args,omitempty"`
}

func NewDurableBroker(dataDir string) (*Broker, error) {
	if dataDir == "" {
		return nil, errors.New("core: durable broker data directory is required")
	}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, err
	}

	queueDir := filepath.Join(dataDir, "queues")
	if err := os.MkdirAll(queueDir, 0o755); err != nil {
		return nil, err
	}

	b := NewBrokerWithQueueStoreFactory(func(cfg QueueConfig) (store.MessageStore, error) {
		if !cfg.Durable {
			return store.NewMemoryMessageStore(), nil
		}
		return store.NewDurableMessageStore(filepath.Join(queueDir, durableQueueFileName(cfg.Name)))
	})
	b.metadataPath = filepath.Join(dataDir, "metadata.json")

	if err := b.loadMetadata(); err != nil {
		return nil, err
	}
	return b, nil
}

func (b *Broker) loadMetadata() error {
	if b.metadataPath == "" {
		return nil
	}

	data, err := os.ReadFile(b.metadataPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}

	var state durableBrokerState
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}

	for _, ex := range state.Exchanges {
		if isDefaultExchange(ex.Name) {
			continue
		}
		b.exchanges[ex.Name] = newExchange(ex.Name, ex.Type, ex.Durable, ex.AutoDelete, ex.Internal)
	}

	for _, q := range state.Queues {
		store, err := b.storeFactory(QueueConfig{
			Name:       q.Name,
			Durable:    q.Durable,
			Exclusive:  q.Exclusive,
			AutoDelete: q.AutoDelete,
			Args:       store.CloneTable(q.Args),
		})
		if err != nil {
			return err
		}
		b.queues[q.Name] = newQueue(q.Name, q.Durable, q.Exclusive, q.AutoDelete, q.Args, store)
		if err := b.bindQueueLocked("", q.Name, q.Name, nil); err != nil {
			return err
		}
	}

	for _, bind := range state.Bindings {
		if err := b.bindQueueLocked(bind.ExchangeName, bind.QueueName, bind.RoutingKey, bind.Args); err != nil {
			return err
		}
	}

	return nil
}

func (b *Broker) persistMetadataLocked() error {
	if b.metadataPath == "" {
		return nil
	}

	state := durableBrokerState{
		Exchanges: make([]durableExchangeState, 0),
		Queues:    make([]durableQueueState, 0),
		Bindings:  make([]durableBindingState, 0),
	}

	for _, ex := range b.exchanges {
		if !ex.Durable || isDefaultExchange(ex.Name) {
			continue
		}
		state.Exchanges = append(state.Exchanges, durableExchangeState{
			Name:       ex.Name,
			Type:       ex.Type,
			Durable:    ex.Durable,
			AutoDelete: ex.AutoDelete,
			Internal:   ex.Internal,
		})
	}
	sort.Slice(state.Exchanges, func(i, j int) bool {
		return state.Exchanges[i].Name < state.Exchanges[j].Name
	})

	for _, q := range b.queues {
		if !q.Durable {
			continue
		}
		state.Queues = append(state.Queues, durableQueueState{
			Name:       q.Name,
			Durable:    q.Durable,
			Exclusive:  q.Exclusive,
			AutoDelete: q.AutoDelete,
			Args:       store.CloneTable(q.Args),
		})
	}
	sort.Slice(state.Queues, func(i, j int) bool {
		return state.Queues[i].Name < state.Queues[j].Name
	})

	for _, bind := range b.bindings {
		if bind.ExchangeName == "" {
			continue
		}
		ex := b.exchanges[bind.ExchangeName]
		q := b.queues[bind.QueueName]
		if ex == nil || q == nil || !ex.Durable || !q.Durable {
			continue
		}
		state.Bindings = append(state.Bindings, durableBindingState{
			ExchangeName: bind.ExchangeName,
			QueueName:    bind.QueueName,
			RoutingKey:   bind.RoutingKey,
			Args:         store.CloneTable(bind.Args),
		})
	}
	sort.Slice(state.Bindings, func(i, j int) bool {
		left := state.Bindings[i]
		right := state.Bindings[j]
		if left.ExchangeName != right.ExchangeName {
			return left.ExchangeName < right.ExchangeName
		}
		if left.QueueName != right.QueueName {
			return left.QueueName < right.QueueName
		}
		return left.RoutingKey < right.RoutingKey
	})

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(b.metadataPath, data, 0o644)
}

func durableQueueFileName(queueName string) string {
	sum := sha1.Sum([]byte(queueName))
	return fmt.Sprintf("%s-%s.gob", sanitizeQueueName(queueName), hex.EncodeToString(sum[:]))
}

func sanitizeQueueName(queueName string) string {
	if queueName == "" {
		return "queue"
	}
	runes := make([]rune, 0, len(queueName))
	for _, r := range queueName {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			runes = append(runes, r)
		case r == '-', r == '_', r == '.':
			runes = append(runes, r)
		default:
			runes = append(runes, '-')
		}
	}
	return string(runes)
}

func isDefaultExchange(name string) bool {
	switch name {
	case "", "amq.direct", "amq.topic", "amq.fanout":
		return true
	default:
		return false
	}
}
