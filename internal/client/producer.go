package client

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"github.com/sagar/streamforge/pkg/log"
	pb "github.com/sagar/streamforge/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
	conns        []*grpc.ClientConn
	clients      []pb.BrokerServiceClient
}

func NewProducer(addrs []string, partitionCnt int32) Producer {
	p := &producerImpl{
		brokerAddrs:  addrs,
		partitionCnt: partitionCnt,
	}

	for _, addr := range addrs {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.WithComponent("producer").Error("Failed to connect to broker", "address", addr, "error", err)
			continue
		}
		p.conns = append(p.conns, conn)
		p.clients = append(p.clients, pb.NewBrokerServiceClient(conn))
	}

	return p
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

	if len(p.clients) == 0 {
		return 0, fmt.Errorf("no broker clients available")
	}

	req := &pb.ProduceRequest{
		Topic:     topic,
		Partition: partition,
		Key:       key,
		Value:     value,
	}

	// Route to correct broker (simple deterministic routing for now)
	clientIdx := int(partition) % len(p.clients)
	client := p.clients[clientIdx]

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.Produce(ctx, req)
	if err != nil {
		return 0, err
	}

	if !resp.Success {
		return 0, fmt.Errorf("produce failed on broker")
	}

	return resp.Offset, nil
}

func (p *producerImpl) Close() error {
	for _, conn := range p.conns {
		conn.Close()
	}
	return nil
}
