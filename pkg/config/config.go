package config

import "time"

type BrokerConfig struct {
	DataDir          string
	NodeID           int32
	Port             int
	RaftPort         int
	Peers            []string // addresses of other raft nodes
	SegmentSizeBytes int64
	RetentionHours   int
}

func DefaultBrokerConfig() BrokerConfig {
	return BrokerConfig{
		DataDir:          "/tmp/streamforge/broker",
		NodeID:           1,
		Port:             9092,
		RaftPort:         9093,
		SegmentSizeBytes: 1024 * 1024 * 1024, // 1GB
		RetentionHours:   168,                 // 7 days
	}
}

type ProcessorConfig struct {
	BrokerAddrs          []string
	GroupID              string
	WindowSize           time.Duration
	CheckpointInterval   time.Duration
	EnableSpeculation    bool
	SpeculationThreshold float64 // e.g. 0.5 means 50% of median progress
}

func DefaultProcessorConfig() ProcessorConfig {
	return ProcessorConfig{
		BrokerAddrs:          []string{"localhost:9092"},
		GroupID:              "default-group",
		WindowSize:           10 * time.Second,
		CheckpointInterval:   30 * time.Second,
		EnableSpeculation:    true,
		SpeculationThreshold: 0.5,
	}
}
