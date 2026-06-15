package broker

import (
	"fmt"
	"net"
	"sync"

	"github.com/sagar/streamforge/internal/storage"
	"github.com/sagar/streamforge/pkg/config"
	"github.com/sagar/streamforge/pkg/log"
	"github.com/sagar/streamforge/pkg/metrics"
	"github.com/sagar/streamforge/pkg/types"
	pb "github.com/sagar/streamforge/proto"
	"google.golang.org/grpc"
)

type Broker struct {
	pb.UnimplementedBrokerServiceServer
	mu         sync.RWMutex
	cfg        config.BrokerConfig
	partitions map[string]map[int32]*storage.Partition
	metadata   *MetadataController
	offsets    map[string]map[string]map[int32]int64 // groupID -> topic -> partition -> offset

	grpcServer *grpc.Server
	listener   net.Listener
	stopCh     chan struct{}
}

func NewBroker(cfg config.BrokerConfig) (*Broker, error) {
	return &Broker{
		cfg:        cfg,
		partitions: make(map[string]map[int32]*storage.Partition),
		offsets:    make(map[string]map[string]map[int32]int64),
		stopCh:     make(chan struct{}),
	}, nil
}

func (b *Broker) Start(address string) error {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	b.listener = lis
	b.grpcServer = grpc.NewServer()
	pb.RegisterBrokerServiceServer(b.grpcServer, b)

	go func() {
		if err := b.grpcServer.Serve(b.listener); err != nil {
			log.WithComponent("broker").Error("Failed to serve gRPC", "error", err)
		}
	}()
	log.WithComponent("broker").Info("Started broker grpc server", "address", address)
	return nil
}

func (b *Broker) AddPartition(topic string, partition int32, p *storage.Partition) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.partitions[topic]; !ok {
		b.partitions[topic] = make(map[int32]*storage.Partition)
	}
	b.partitions[topic][partition] = p
}

func (b *Broker) Stop() {
	close(b.stopCh)
	if b.grpcServer != nil {
		b.grpcServer.GracefulStop()
	}
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

	offset, err := part.Append(msg.Key, msg.Value, msg.Timestamp)
	if err == nil {
		pStr := fmt.Sprintf("%d", partition)
		metrics.BrokerMessagesProduced.WithLabelValues(topic, pStr).Inc()
		metrics.BrokerPartitionLatestOffset.WithLabelValues(topic, pStr).Set(float64(offset))
	}
	return offset, err
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
