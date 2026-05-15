package client

import (
	"fmt"
	"sync"
	"time"

	"github.com/sagar/streamforge/pkg/types"
	"github.com/sagar/streamforge/pkg/log"
)

type Consumer interface {
	Poll(timeout time.Duration) ([]types.Message, error)
	CommitOffset(topic string, partition int32, offset types.Offset) error
	Close() error
}

type consumerImpl struct {
	mu              sync.Mutex
	brokerAddrs     []string
	groupID         string
	assignedParts   []int32
	currentOffsets  map[int32]types.Offset
	// gRPC connection omitted
}

func NewConsumer(addrs []string, groupID string) Consumer {
	return &consumerImpl{
		brokerAddrs:    addrs,
		groupID:        groupID,
		currentOffsets: make(map[int32]types.Offset),
	}
}

// Poll fetches new messages from the assigned partitions.
func (c *consumerImpl) Poll(timeout time.Duration) ([]types.Message, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var fetches []types.Message
	if len(c.assignedParts) == 0 {
		return fetches, fmt.Errorf("no partitions assigned")
	}

	// Pseudo logic: Make gRPC FetchRequests for each assigned partition
	for _, part := range c.assignedParts {
		offset := c.currentOffsets[part]
		
		// TODO: Issue gRPC FetchRequest(topic, part, offset)
		_ = offset
		
		// Mock message
		msg := types.Message{
			Offset:    offset + 1,
			Key:       []byte("mock-city"),
			Value:     []byte(`{"amount": 100}`),
			Timestamp: time.Now(),
		}
		
		fetches = append(fetches, msg)
		c.currentOffsets[part] = offset + 1
	}

	return fetches, nil
}

// CommitOffset sends an offset commit RPC to the consumer group coordinator.
func (c *consumerImpl) CommitOffset(topic string, partition int32, offset types.Offset) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// TODO: Send OffsetCommitRequest over gRPC
	log.WithComponent("consumer").Info("Committed offset", "topic", topic, "partition", partition, "offset", offset)
	
	c.currentOffsets[partition] = offset
	return nil
}

func (c *consumerImpl) Close() error {
	return nil
}
