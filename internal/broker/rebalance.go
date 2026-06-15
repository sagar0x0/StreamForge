package broker

import (
	"net"
	"sync"
	"time"

	"github.com/sagar/streamforge/pkg/log"
	pb "github.com/sagar/streamforge/proto"
	"google.golang.org/grpc"
)

type ConsumerGroupMember struct {
	ConsumerID   string
	LastHeartbeat int64
}

type ConsumerGroup struct {
	GroupID      string
	GenerationID int32
	Members      map[string]*ConsumerGroupMember
	Assignments  map[string][]int32 // consumerID -> assigned partitions
	mu           sync.RWMutex
}

type RebalanceCoordinator struct {
	pb.UnimplementedConsumerCoordinatorServer
	groups map[string]*ConsumerGroup
	mu     sync.RWMutex

	grpcServer *grpc.Server
	listener   net.Listener
	stopCh     chan struct{}
}

func NewRebalanceCoordinator() *RebalanceCoordinator {
	return &RebalanceCoordinator{
		groups: make(map[string]*ConsumerGroup),
		stopCh: make(chan struct{}),
	}
}

func (c *RebalanceCoordinator) Start(address string) error {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	c.listener = lis
	c.grpcServer = grpc.NewServer()
	pb.RegisterConsumerCoordinatorServer(c.grpcServer, c)

	go func() {
		if err := c.grpcServer.Serve(c.listener); err != nil {
			log.WithComponent("coordinator").Error("Failed to serve gRPC", "error", err)
		}
	}()
	log.WithComponent("coordinator").Info("Started coordinator grpc server", "address", address)
	
	go c.heartbeatMonitor()
	
	return nil
}

func (c *RebalanceCoordinator) Stop() {
	close(c.stopCh)
	if c.grpcServer != nil {
		c.grpcServer.GracefulStop()
	}
}

func (c *RebalanceCoordinator) heartbeatMonitor() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			c.mu.Lock()
			now := time.Now().UnixMilli()
			for _, group := range c.groups {
				group.mu.Lock()
				changed := false
				for id, member := range group.Members {
					if now-member.LastHeartbeat > 2000 { // 2s timeout
						log.WithComponent("coordinator").Info("Consumer timeout", "consumer", id)
						delete(group.Members, id)
						changed = true
					}
				}
				if changed {
					group.GenerationID++
					c.rebalance(group)
				}
				group.mu.Unlock()
			}
			c.mu.Unlock()
		}
	}
}

// JoinGroupInternal adds a consumer to the group and triggers rebalance logic.
func (c *RebalanceCoordinator) JoinGroupInternal(groupID, consumerID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	group, ok := c.groups[groupID]
	if !ok {
		group = &ConsumerGroup{
			GroupID:     groupID,
			Members:     make(map[string]*ConsumerGroupMember),
			Assignments: make(map[string][]int32),
		}
		c.groups[groupID] = group
	}

	group.mu.Lock()
	defer group.mu.Unlock()

	group.Members[consumerID] = &ConsumerGroupMember{
		ConsumerID:    consumerID,
		LastHeartbeat: time.Now().UnixMilli(),
	}
	
	group.GenerationID++
	log.WithComponent("coordinator").Info("Consumer joined, triggered rebalance", "group", groupID, "consumer", consumerID)
	
	c.rebalance(group)
}

// rebalance assigns partitions to group members
func (c *RebalanceCoordinator) rebalance(group *ConsumerGroup) {
	// Simple mock logical rebalance: evenly divide a fixed pool of partitions (e.g. 4)
	partitions := []int32{0, 1, 2, 3}
	
	if len(group.Members) == 0 {
		return
	}
	
	memberIDs := make([]string, 0, len(group.Members))
	for id := range group.Members {
		memberIDs = append(memberIDs, id)
	}
	
	group.Assignments = make(map[string][]int32)
	for i, part := range partitions {
		targetID := memberIDs[i % len(memberIDs)]
		group.Assignments[targetID] = append(group.Assignments[targetID], part)
	}
	
	log.WithComponent("coordinator").Info("Rebalance complete", "generation", group.GenerationID, "assignments", group.Assignments)
}
