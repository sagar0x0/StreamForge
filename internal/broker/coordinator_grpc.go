package broker

import (
	"context"
	"time"

	pb "github.com/sagar/streamforge/proto"
)

func (c *RebalanceCoordinator) JoinGroup(ctx context.Context, req *pb.JoinGroupRequest) (*pb.JoinGroupResponse, error) {
	c.JoinGroupInternal(req.GroupId, req.ConsumerId) // We will rename the original method to JoinGroupInternal

	c.mu.RLock()
	group := c.groups[req.GroupId]
	c.mu.RUnlock()

	group.mu.RLock()
	defer group.mu.RUnlock()

	members := make([]string, 0, len(group.Members))
	for id := range group.Members {
		members = append(members, id)
	}

	isLeader := len(members) > 0 && members[0] == req.ConsumerId // Simple leader election

	return &pb.JoinGroupResponse{
		ConsumerId:   req.ConsumerId,
		GenerationId: group.GenerationID,
		IsLeader:     isLeader,
		Members:      members,
	}, nil
}

func (c *RebalanceCoordinator) SyncGroup(ctx context.Context, req *pb.SyncGroupRequest) (*pb.SyncGroupResponse, error) {
	c.mu.RLock()
	group, ok := c.groups[req.GroupId]
	c.mu.RUnlock()

	if !ok {
		return &pb.SyncGroupResponse{}, nil
	}

	group.mu.RLock()
	defer group.mu.RUnlock()

	// If leader sends assignments, we could save them. But our mock rebalance actually assigns them server-side.
	// So we just return the server-computed assignments.
	var parts []int32
	if assignment, ok := group.Assignments[req.ConsumerId]; ok {
		parts = assignment
	}

	return &pb.SyncGroupResponse{
		Assignment: &pb.PartitionAssignment{
			Partitions: parts,
		},
	}, nil
}

func (c *RebalanceCoordinator) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	c.mu.RLock()
	group, ok := c.groups[req.GroupId]
	c.mu.RUnlock()

	if !ok {
		return &pb.HeartbeatResponse{RebalanceInProgress: true}, nil
	}

	group.mu.Lock()
	defer group.mu.Unlock()

	if member, ok := group.Members[req.ConsumerId]; ok {
		member.LastHeartbeat = time.Now().UnixMilli()
	}

	rebalanceInProgress := group.GenerationID != req.GenerationId

	return &pb.HeartbeatResponse{
		RebalanceInProgress: rebalanceInProgress,
	}, nil
}

func (c *RebalanceCoordinator) LeaveGroup(ctx context.Context, req *pb.LeaveGroupRequest) (*pb.LeaveGroupResponse, error) {
	c.mu.RLock()
	group, ok := c.groups[req.GroupId]
	c.mu.RUnlock()

	if !ok {
		return &pb.LeaveGroupResponse{Success: true}, nil
	}

	group.mu.Lock()
	defer group.mu.Unlock()

	delete(group.Members, req.ConsumerId)
	group.GenerationID++
	c.rebalance(group)

	return &pb.LeaveGroupResponse{Success: true}, nil
}
