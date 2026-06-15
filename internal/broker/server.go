package broker

import (
	"context"
	"time"

	"github.com/sagar/streamforge/pkg/types"
	pb "github.com/sagar/streamforge/proto"
)

func (b *Broker) Produce(ctx context.Context, req *pb.ProduceRequest) (*pb.ProduceResponse, error) {
	msg := types.Message{
		Key:       req.Key,
		Value:     req.Value,
		Timestamp: time.Now(),
	}

	offset, err := b.AppendData(req.Topic, req.Partition, msg)
	if err != nil {
		return &pb.ProduceResponse{Success: false}, err
	}

	return &pb.ProduceResponse{
		Success: true,
		Offset:  offset,
	}, nil
}

func (b *Broker) Fetch(ctx context.Context, req *pb.FetchRequest) (*pb.FetchResponse, error) {
	msg, err := b.FetchData(req.Topic, req.Partition, req.Offset)
	if err != nil {
		return &pb.FetchResponse{}, err
	}

	return &pb.FetchResponse{
		Key:        msg.Key,
		Value:      msg.Value,
		Offset:     int64(msg.Offset),
		NextOffset: int64(msg.Offset) + 1,
	}, nil
}

func (b *Broker) Metadata(ctx context.Context, req *pb.MetadataRequest) (*pb.MetadataResponse, error) {
	// Simple mock returning self as leader for partitions 0-3
	leaders := make(map[int32]int32)
	for i := int32(0); i < 4; i++ {
		leaders[i] = 1 // Assuming this broker is ID 1
	}
	
	return &pb.MetadataResponse{
		PartitionLeaders: leaders,
	}, nil
}

func (b *Broker) OffsetCommit(ctx context.Context, req *pb.OffsetCommitRequest) (*pb.OffsetCommitResponse, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.offsets[req.GroupId]; !ok {
		b.offsets[req.GroupId] = make(map[string]map[int32]int64)
	}
	if _, ok := b.offsets[req.GroupId][req.Topic]; !ok {
		b.offsets[req.GroupId][req.Topic] = make(map[int32]int64)
	}

	b.offsets[req.GroupId][req.Topic][req.Partition] = req.Offset

	return &pb.OffsetCommitResponse{Success: true}, nil
}

func (b *Broker) OffsetFetch(ctx context.Context, req *pb.OffsetFetchRequest) (*pb.OffsetFetchResponse, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var offset int64 = 0
	if groupData, ok := b.offsets[req.GroupId]; ok {
		if topicData, ok := groupData[req.Topic]; ok {
			if o, ok := topicData[req.Partition]; ok {
				offset = o
			}
		}
	}

	return &pb.OffsetFetchResponse{Offset: offset}, nil
}
