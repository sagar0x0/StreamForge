package types

import "time"

type Offset int64
type WindowID int64
type ConsumerGroupID string

type TopicPartition struct {
	Topic     string
	Partition int32
}

type Message struct {
	Offset    Offset
	Key       []byte
	Value     []byte
	Timestamp time.Time
}

type AggregatedResult struct {
	WindowID    WindowID
	WindowStart time.Time
	WindowEnd   time.Time
	Key         string
	Count       int64
	Sum         float64
	Avg         float64
	Min         float64
	Max         float64
}
