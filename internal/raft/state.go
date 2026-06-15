package raft

import (
	"fmt"
	"sync"

	"github.com/sagar/streamforge/pkg/metrics"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

type NodeState struct {
	mu            sync.RWMutex
	NodeID        int32
	CurrentTerm   int64
	VotedFor      int32
	State         State
	CommitIndex   int64
	LastApplied   int64
	
	NextIndex     map[int32]int64
	MatchIndex    map[int32]int64
}

func NewNodeState(nodeID int32) *NodeState {
	return &NodeState{
		NodeID:     nodeID,
		State:      Follower,
		NextIndex:  make(map[int32]int64),
		MatchIndex: make(map[int32]int64),
		VotedFor:   -1,
	}
}

func (s *NodeState) Transition(newState State) {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	// Reset all state metrics for this node
	nodeStr := fmt.Sprintf("%d", s.NodeID)
	metrics.RaftState.WithLabelValues(nodeStr, "0").Set(0)
	metrics.RaftState.WithLabelValues(nodeStr, "1").Set(0)
	metrics.RaftState.WithLabelValues(nodeStr, "2").Set(0)
	
	s.State = newState
	metrics.RaftState.WithLabelValues(nodeStr, fmt.Sprintf("%d", int(newState))).Set(1)
}
