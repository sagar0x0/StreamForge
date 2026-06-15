package main

import (
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/sagar/streamforge/internal/broker"
	"github.com/sagar/streamforge/internal/raft"
	"github.com/sagar/streamforge/internal/storage"
	"github.com/sagar/streamforge/pkg/config"
	"github.com/sagar/streamforge/pkg/log"
)

func main() {
	nodeIDStr := os.Getenv("NODE_ID")
	peersStr := os.Getenv("PEERS")

	nodeID, err := strconv.Atoi(nodeIDStr)
	if err != nil {
		log.Logger.Error("Invalid or missing NODE_ID", "err", err)
		os.Exit(1)
	}

	// Example PEERS format: broker-1:9093,broker-2:9093,broker-3:9093
	peerAddrs := strings.Split(peersStr, ",")
	peers := make(map[int32]string)
	for i, addr := range peerAddrs {
		peers[int32(i+1)] = addr
	}

	dataDir := fmt.Sprintf("/tmp/streamforge-data/broker-%d", nodeID)
	os.MkdirAll(dataDir, 0755)

	brk, err := broker.NewBroker(config.BrokerConfig{})
	if err != nil {
		log.Logger.Error("Failed to create broker", "err", err)
		os.Exit(1)
	}

	// Add mock partitions for 'orders' topic
	numPartitions := 4
	for i := 0; i < numPartitions; i++ {
		pDir := filepath.Join(dataDir, fmt.Sprintf("partition-%d", i))
		p, err := storage.OpenPartition(pDir, 64*1024*1024)
		if err != nil {
			log.Logger.Error("Failed to open partition", "err", err)
			os.Exit(1)
		}
		brk.AddPartition("orders", int32(i), p)
	}

	// Port 9092 for client gRPC requests
	go func() {
		if err := brk.Start("0.0.0.0:9092"); err != nil {
			log.Logger.Error("Broker failed to start", "err", err)
		}
	}()



	// Real Raft startup logic
	raftConfig := raft.Config{
		ID:            int32(nodeID),
		Address:       "0.0.0.0:9093", // Run raft on a separate port 9093 internally
		Peers:         peers,
		ElectionMin:   150 * time.Millisecond,
		ElectionMax:   300 * time.Millisecond,
		HeartbeatTick: 50 * time.Millisecond,
	}

	// The NewNode signature is NewNode(cfg Config, applyCh chan<- []pb.LogEntry)
	// We can pass nil.
	raftNode := raft.NewNode(raftConfig, nil)
	
	if err := raftNode.Start(); err != nil {
		log.Logger.Error("Failed to start raft node", "error", err)
		os.Exit(1)
	}

	log.Logger.Info("Broker node started", "nodeID", nodeID)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Logger.Info("Shutting down broker...")
	raftNode.Stop()
	brk.Stop()
}
