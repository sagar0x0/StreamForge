package raft

import "sync"

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

type NodeState struct {
	mu            sync.RWMutex
	CurrentTerm   int64
	VotedFor      int32
	State         State
	CommitIndex   int64
	LastApplied   int64
	
	NextIndex     map[int32]int64
	MatchIndex    map[int32]int64
}

func NewNodeState() *NodeState {
	return &NodeState{
		State:      Follower,
		NextIndex:  make(map[int32]int64),
		MatchIndex: make(map[int32]int64),
		VotedFor:   -1,
	}
}

func (s *NodeState) Transition(newState State) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.State = newState
}
