package raft

import (
	"context"
	"fmt"
	"sync"

	pb "github.com/sagar/streamforge/proto"
)

func (n *Node) RequestVote(ctx context.Context, req *pb.RequestVoteRequest) (*pb.RequestVoteResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := &pb.RequestVoteResponse{
		Term:        n.state.CurrentTerm,
		VoteGranted: false,
	}

	if req.Term < n.state.CurrentTerm {
		return resp, nil
	}

	if req.Term > n.state.CurrentTerm {
		n.state.CurrentTerm = req.Term
		n.state.Transition(Follower)
		n.state.VotedFor = -1
	}

	lastLogTerm := n.log.LastTerm()
	lastLogIndex := n.log.LastIndex()

	logOk := (req.LastLogTerm > lastLogTerm) ||
		(req.LastLogTerm == lastLogTerm && req.LastLogIndex >= lastLogIndex)

	if (n.state.VotedFor == -1 || n.state.VotedFor == req.CandidateId) && logOk {
		n.state.VotedFor = req.CandidateId
		resp.VoteGranted = true
		n.resetElectionTimer()
	}

	resp.Term = n.state.CurrentTerm
	return resp, nil
}

func (n *Node) AppendEntries(ctx context.Context, req *pb.AppendEntriesRequest) (*pb.AppendEntriesResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	resp := &pb.AppendEntriesResponse{
		Term:    n.state.CurrentTerm,
		Success: false,
	}

	if req.Term < n.state.CurrentTerm {
		return resp, nil
	}

	if req.Term >= n.state.CurrentTerm {
		n.state.CurrentTerm = req.Term
		n.state.Transition(Follower)
		n.state.VotedFor = -1
		n.resetElectionTimer()
	}

	if n.log.Term(req.PrevLogIndex) != req.PrevLogTerm {
		return resp, nil
	}

	if len(req.Entries) > 0 {
		n.log.Truncate(req.PrevLogIndex + 1)
		
		entries := make([]LogEntry, len(req.Entries))
		for i, e := range req.Entries {
			entries[i] = LogEntry{Term: e.Term, Index: e.Index, Command: e.Command}
		}
		n.log.Append(entries)
	}

	if req.LeaderCommit > n.state.CommitIndex {
		lastNewIndex := req.PrevLogIndex + int64(len(req.Entries))
		if req.LeaderCommit < lastNewIndex {
			n.state.CommitIndex = req.LeaderCommit
		} else {
			n.state.CommitIndex = lastNewIndex
		}
		n.applyLogs()
	}

	resp.Success = true
	return resp, nil
}

func (n *Node) startElection() {
	n.mu.Lock()
	n.state.Transition(Candidate)
	n.state.CurrentTerm++
	n.state.VotedFor = n.config.ID
	term := n.state.CurrentTerm
	lastLogIndex := n.log.LastIndex()
	lastLogTerm := n.log.LastTerm()
	n.mu.Unlock()

	fmt.Printf("[RAFT] transitioning to CANDIDATE, term %d\n", term)

	req := &pb.RequestVoteRequest{
		Term:         term,
		CandidateId:  n.config.ID,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	votes := 1
	var voteMu sync.Mutex

	for peerID, client := range n.peers {
		go func(id int32, c pb.RaftServiceClient) {
			ctx, cancel := context.WithTimeout(context.Background(), n.config.HeartbeatTick)
			defer cancel()

			resp, err := c.RequestVote(ctx, req)
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if resp.Term > n.state.CurrentTerm {
				n.state.CurrentTerm = resp.Term
				n.state.Transition(Follower)
				n.state.VotedFor = -1
				n.resetElectionTimer()
				return
			}

			if n.state.State == Candidate && resp.VoteGranted {
				voteMu.Lock()
				votes++
				if votes > (len(n.peers)+1)/2 {
					n.becomeLeader()
				}
				voteMu.Unlock()
			}
		}(peerID, client)
	}
}

func (n *Node) becomeLeader() {
	if n.state.State == Leader {
		return
	}
	n.state.Transition(Leader)
	fmt.Println("[RAFT] received majority votes → LEADER")
	fmt.Println("[BROKER] assuming leadership: partition 0, partition 1")
	
	for id := range n.peers {
		n.state.NextIndex[id] = n.log.LastIndex() + 1
		n.state.MatchIndex[id] = 0
	}
	go n.sendHeartbeats()
}

func (n *Node) sendHeartbeats() {
	n.mu.Lock()
	if n.state.State != Leader {
		n.mu.Unlock()
		return
	}
	term := n.state.CurrentTerm
	leaderID := n.config.ID
	commitIndex := n.state.CommitIndex
	n.mu.Unlock()

	for peerID, client := range n.peers {
		go func(id int32, c pb.RaftServiceClient) {
			n.mu.Lock()
			nextIdx := n.state.NextIndex[id]
			prevLogIndex := nextIdx - 1
			prevLogTerm := n.log.Term(prevLogIndex)
			
			var pbEntries []*pb.LogEntry
			entries := n.log.EntriesFrom(nextIdx)
			for _, e := range entries {
				pbEntries = append(pbEntries, &pb.LogEntry{Term: e.Term, Index: e.Index, Command: e.Command})
			}
			n.mu.Unlock()

			req := &pb.AppendEntriesRequest{
				Term:         term,
				LeaderId:     leaderID,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				LeaderCommit: commitIndex,
				Entries:      pbEntries,
			}

			ctx, cancel := context.WithTimeout(context.Background(), n.config.HeartbeatTick)
			defer cancel()

			resp, err := c.AppendEntries(ctx, req)
			if err != nil {
				return
			}

			n.mu.Lock()
			defer n.mu.Unlock()

			if resp.Term > n.state.CurrentTerm {
				n.state.CurrentTerm = resp.Term
				n.state.Transition(Follower)
				n.state.VotedFor = -1
				n.resetElectionTimer()
				return
			}

			if resp.Success {
				if len(entries) > 0 {
					n.state.NextIndex[id] = entries[len(entries)-1].Index + 1
					n.state.MatchIndex[id] = entries[len(entries)-1].Index
					n.updateCommitIndex()
				}
			} else {
				n.state.NextIndex[id]--
			}
		}(peerID, client)
	}
}

func (n *Node) updateCommitIndex() {
	for i := n.log.LastIndex(); i > n.state.CommitIndex; i-- {
		matches := 1
		for id := range n.peers {
			if n.state.MatchIndex[id] >= i {
				matches++
			}
		}
		if matches > (len(n.peers)+1)/2 && n.log.Term(i) == n.state.CurrentTerm {
			n.state.CommitIndex = i
			n.applyLogs()
			break
		}
	}
}

func (n *Node) applyLogs() {
	if n.state.CommitIndex > n.state.LastApplied {
		var pbEntries []pb.LogEntry
		entries := n.log.EntriesFrom(n.state.LastApplied + 1)
		
		limit := n.state.CommitIndex - n.state.LastApplied
		if limit > int64(len(entries)) {
			limit = int64(len(entries))
		}
		
		for _, e := range entries[:limit] {
			pbEntries = append(pbEntries, pb.LogEntry{Term: e.Term, Index: e.Index, Command: e.Command})
		}
		
		n.state.LastApplied = n.state.CommitIndex
		if n.applyCh != nil && len(pbEntries) > 0 {
			n.applyCh <- pbEntries
		}
	}
}

func (n *Node) Propose(command []byte) (int64, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	
	if n.state.State != Leader {
		return 0, nil // Should return error or redirect, returning 0 for now
	}
	
	entry := LogEntry{
		Term:    n.state.CurrentTerm,
		Index:   n.log.LastIndex() + 1,
		Command: command,
	}
	n.log.Append([]LogEntry{entry})
	go n.sendHeartbeats()
	return entry.Index, nil
}

// Needed by protobuf interface
func (n *Node) InstallSnapshot(ctx context.Context, req *pb.InstallSnapshotRequest) (*pb.InstallSnapshotResponse, error) {
	return &pb.InstallSnapshotResponse{Term: n.state.CurrentTerm}, nil
}
