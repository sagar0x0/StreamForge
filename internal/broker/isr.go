package broker

import (
	"sync"
	"time"

	"github.com/sagar/streamforge/pkg/log"
)

// ISRManager tracks In-Sync Replicas per partition.
type ISRManager struct {
	mu       sync.RWMutex
	isrSets  map[int32]map[int32]bool // partition -> set of broker IDs in ISR
	lastSeen map[int32]map[int32]time.Time // partition -> broker -> last caught-up time
	lagThreshold time.Duration
}

func NewISRManager(lagThreshold time.Duration) *ISRManager {
	return &ISRManager{
		isrSets:      make(map[int32]map[int32]bool),
		lastSeen:     make(map[int32]map[int32]time.Time),
		lagThreshold: lagThreshold,
	}
}

// AddReplica marks a broker as an ISR member for a partition.
func (m *ISRManager) AddReplica(partition int32, brokerID int32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.isrSets[partition]; !ok {
		m.isrSets[partition] = make(map[int32]bool)
		m.lastSeen[partition] = make(map[int32]time.Time)
	}
	m.isrSets[partition][brokerID] = true
	m.lastSeen[partition][brokerID] = time.Now()
}

// UpdateReplicaProgress is called when a follower confirms replication.
func (m *ISRManager) UpdateReplicaProgress(partition int32, brokerID int32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.lastSeen[partition]; !ok {
		m.lastSeen[partition] = make(map[int32]time.Time)
	}
	m.lastSeen[partition][brokerID] = time.Now()

	// Re-add to ISR if it had been removed
	if _, ok := m.isrSets[partition]; !ok {
		m.isrSets[partition] = make(map[int32]bool)
	}
	m.isrSets[partition][brokerID] = true
}

// CheckISR removes brokers that have fallen behind the lag threshold.
func (m *ISRManager) CheckISR() {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	for partition, brokers := range m.lastSeen {
		for brokerID, lastTime := range brokers {
			if now.Sub(lastTime) > m.lagThreshold {
				delete(m.isrSets[partition], brokerID)
				log.WithComponent("isr").Info("Removed broker from ISR due to lag",
					"partition", partition, "broker", brokerID)
			}
		}
	}
}

// GetISR returns the current in-sync replica set for a partition.
func (m *ISRManager) GetISR(partition int32) []int32 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var result []int32
	if brokers, ok := m.isrSets[partition]; ok {
		for id := range brokers {
			result = append(result, id)
		}
	}
	return result
}
