package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sagar/streamforge/internal/processor"
	"github.com/sagar/streamforge/internal/speculative"
	"github.com/sagar/streamforge/pkg/log"
	"github.com/sagar/streamforge/pkg/metrics"
	"github.com/sagar/streamforge/pkg/types"
)

func main() {
	brokersStr := os.Getenv("BROKERS")
	groupID := os.Getenv("GROUP_ID")
	enableSpeculation := os.Getenv("ENABLE_SPECULATION") == "true"

	_ = groupID // Group ID used when joining cluster
	_ = brokersStr

	metrics.StartMetricsServer(":2113")

	log.Logger.Info("Starting processor node", "brokers", brokersStr, "groupID", groupID, "speculation", enableSpeculation)

	var detector *speculative.StragglerDetector
	var arbitrator *speculative.ResultArbitrator
	var specMgr *speculative.SpeculativeManager

	if enableSpeculation {
		specMgr = speculative.NewSpeculativeManager()
		detector = speculative.NewStragglerDetector(0.5, specMgr)
		arbitrator = speculative.NewResultArbitrator()
	}

	winOp := processor.NewTumblingWindow(10 * time.Second)
	agg := processor.NewAggregator()
	store := processor.NewStateStore()
	
	engine := processor.NewEngine(winOp, agg, store, detector, arbitrator)

	numPartitions := 4
	streams := make([]chan types.Message, numPartitions)
	for i := 0; i < numPartitions; i++ {
		streams[i] = make(chan types.Message, 100)
		go engine.ProcessStream(int32(i), streams[i])
		
		// Push mock data to keep the engine processing and updating partition lag metrics
		go func(stream chan types.Message) {
			for {
				time.Sleep(10 * time.Millisecond)
				stream <- types.Message{Timestamp: time.Now(), Key: []byte("k"), Value: []byte("v")}
			}
		}(streams[i])
	}

	// Start checkpoint coordinator to generate realistic checkpoint metrics
	chkptCoord := processor.NewCheckpointCoordinator(2 * time.Second)
	chkptCoord.Start(func(id int64) {
		// Mock injection
	})

	log.Logger.Info("Processor engines and checkpoint coordinator started")



	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Logger.Info("Shutting down processor...")
	engine.Stop()
}
