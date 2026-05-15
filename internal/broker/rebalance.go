package broker

import (
	"sync"

	"github.com/sagar/streamforge/pkg/log"
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
	groups map[string]*ConsumerGroup
	mu     sync.RWMutex
}

func NewRebalanceCoordinator() *RebalanceCoordinator {
	return &RebalanceCoordinator{
		groups: make(map[string]*ConsumerGroup),
	}
}

// JoinGroup adds a consumer to the group and triggers rebalance logic.
func (c *RebalanceCoordinator) JoinGroup(groupID, consumerID string) {
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
		ConsumerID: consumerID,
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
