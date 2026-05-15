package client

import (
	"crypto/sha256"
	"encoding/binary"
	"sync"
	"time"

	"github.com/sagar/streamforge/pkg/types"
)

type Producer interface {
	Send(topic string, key []byte, value []byte) (int64, error)
	Close() error
}

type producerImpl struct {
	mu           sync.Mutex
	brokerAddrs  []string
	partitionCnt int32
	roundRobinID int32
	// internal gRPC clients to brokers omitted for brevity
}

func NewProducer(addrs []string, partitionCnt int32) Producer {
	return &producerImpl{
		brokerAddrs:  addrs,
		partitionCnt: partitionCnt,
	}
}

// HashKey provides key-based routing, falling back to round-robin.
func (p *producerImpl) HashKey(key []byte) int32 {
	if len(key) == 0 {
		p.mu.Lock()
		defer p.mu.Unlock()
		part := p.roundRobinID % p.partitionCnt
		p.roundRobinID++
		return part
	}

	h := sha256.Sum256(key)
	val := binary.BigEndian.Uint32(h[:4])
	return int32(val % uint32(p.partitionCnt))
}

// Send determines the partition and dispatches the produce RPC request.
func (p *producerImpl) Send(topic string, key []byte, value []byte) (int64, error) {
	partition := p.HashKey(key)

	// Mock building of the request.
	msg := types.Message{
		Key:       key,
		Value:     value,
		Timestamp: time.Now(),
	}
	_ = msg

	// TODO: Send gRPC ProduceRequest to leader of 'partition'
	_ = partition

	// Mock return offset 10
	return 10, nil
}

func (p *producerImpl) Close() error {
	return nil
}
