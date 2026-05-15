package speculative

import (
	"sync"
	"time"

	"github.com/sagar/streamforge/pkg/types"
	"github.com/sagar/streamforge/pkg/log"
)

type SpeculativeManager struct {
	mu            sync.Mutex
	activeTaskIDs map[int32]bool
}

func NewSpeculativeManager() *SpeculativeManager {
	return &SpeculativeManager{
		activeTaskIDs: make(map[int32]bool),
	}
}

func (m *SpeculativeManager) Launch(partitionID int32, lastCheckpoint time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.activeTaskIDs[partitionID] {
		return // already launched
	}
	m.activeTaskIDs[partitionID] = true

	// In real implementation, this would spin up a new processor worker or send gRPC request to spare node
	log.WithPartition(int(partitionID)).Info("Speculative task launch initiated starting from checkpoint")
}

func (m *SpeculativeManager) Cancel(partitionID int32) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.activeTaskIDs, partitionID)
}

type ResultArbitrator struct {
	mu         sync.Mutex
	tombstones map[types.WindowID]map[int32]bool
}

func NewResultArbitrator() *ResultArbitrator {
	return &ResultArbitrator{
		tombstones: make(map[types.WindowID]map[int32]bool),
	}
}

func (a *ResultArbitrator) Submit(windowID types.WindowID, partitionID int32, result types.AggregatedResult) bool {
	a.mu.Lock()
	defer a.mu.Unlock()

	if _, ok := a.tombstones[windowID]; !ok {
		a.tombstones[windowID] = make(map[int32]bool)
	}

	if a.tombstones[windowID][partitionID] {
		// A result has already been submitted for this window partition. Discard.
		log.Logger.Info("Speculative or original task discarded due to tombstone", "window", windowID, "partition", partitionID)
		return false
	}

	// First write wins -> Set tombstone
	a.tombstones[windowID][partitionID] = true
	return true
}
