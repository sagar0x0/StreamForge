package broker

import "sync"

// MetadataController tracks topic/partition layout and leader assignments.
type MetadataController struct {
	mu              sync.RWMutex
	partitionLeaders map[string]map[int32]int32 // topic -> partition -> leader broker ID
}

func NewMetadataController() *MetadataController {
	return &MetadataController{
		partitionLeaders: make(map[string]map[int32]int32),
	}
}

// SetLeader records which broker is the leader for a given topic-partition.
func (m *MetadataController) SetLeader(topic string, partition int32, brokerID int32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.partitionLeaders[topic]; !ok {
		m.partitionLeaders[topic] = make(map[int32]int32)
	}
	m.partitionLeaders[topic][partition] = brokerID
}

// GetLeader returns the leader broker ID for a given topic-partition.
func (m *MetadataController) GetLeader(topic string, partition int32) (int32, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if tp, ok := m.partitionLeaders[topic]; ok {
		if leader, ok := tp[partition]; ok {
			return leader, true
		}
	}
	return 0, false
}

// GetPartitionLeaders returns the full leader map for a topic.
func (m *MetadataController) GetPartitionLeaders(topic string) map[int32]int32 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make(map[int32]int32)
	if tp, ok := m.partitionLeaders[topic]; ok {
		for k, v := range tp {
			result[k] = v
		}
	}
	return result
}
