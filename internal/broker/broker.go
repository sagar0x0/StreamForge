package broker

import (
	"fmt"
	"sync"

	"github.com/sagar/streamforge/pkg/config"
	"github.com/sagar/streamforge/pkg/types"
	"github.com/sagar/streamforge/internal/storage"
)

type Broker struct {
	mu         sync.RWMutex
	cfg        config.BrokerConfig
	partitions map[string]map[int32]*storage.Partition
	metadata   *MetadataController
}

func NewBroker(cfg config.BrokerConfig) (*Broker, error) {
	return &Broker{
		cfg:        cfg,
		partitions: make(map[string]map[int32]*storage.Partition),
	}, nil
}

func (b *Broker) AppendData(topic string, partition int32, msg types.Message) (int64, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	pMap, ok := b.partitions[topic]
	if !ok {
		return 0, fmt.Errorf("topic not found")
	}
	part, ok := pMap[partition]
	if !ok {
		return 0, fmt.Errorf("partition not found")
	}

	return part.Append(msg.Key, msg.Value, msg.Timestamp)
}

func (b *Broker) FetchData(topic string, partition int32, offset int64) (types.Message, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	pMap, ok := b.partitions[topic]
	if !ok {
		return types.Message{}, fmt.Errorf("topic not found")
	}
	part, ok := pMap[partition]
	if !ok {
		return types.Message{}, fmt.Errorf("partition not found")
	}

	return part.Read(offset)
}
