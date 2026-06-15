package processor

import (
	"sync"
	"time"

	"github.com/sagar/streamforge/internal/speculative"
	"github.com/sagar/streamforge/pkg/log"
	"github.com/sagar/streamforge/pkg/types"
)

type Engine struct {
	mu               sync.RWMutex
	windowOperator   *WindowOperator
	aggregator       *Aggregator
	stateStore       *StateStore
	stragglerDetector *speculative.StragglerDetector
	arbitrator       *speculative.ResultArbitrator
	
	stop chan struct{}
}

func NewEngine(
	winOp *WindowOperator, 
	agg *Aggregator, 
	store *StateStore,
	detector *speculative.StragglerDetector,
	arbitrator *speculative.ResultArbitrator) *Engine {
	
	return &Engine{
		windowOperator:   winOp,
		aggregator:       agg,
		stateStore:       store,
		stragglerDetector: detector,
		arbitrator:       arbitrator,
		stop:             make(chan struct{}),
	}
}

// ProcessStream runs the core processing loop locally
func (e *Engine) ProcessStream(partitionID int32, stream <-chan types.Message) {
	log.WithPartition(int(partitionID)).Info("Starting stream processing loop")
	
	var processedCount int64

	for {
		select {
		case <-e.stop:
			return
		case msg := <-stream:
			processedCount++
			
			// 1. Determine Window
			windows := e.windowOperator.AssignWindows(msg.Timestamp)
			
			for _, winID := range windows {
				keyStr := string(msg.Key)
				
				// 2. State Lookup
				state := e.stateStore.Get(winID, keyStr)
				
				// 3. Apply Stateful Aggregation
				updatedState := e.aggregator.Apply(msg, state)
				e.stateStore.Put(winID, keyStr, updatedState)
			}
			
			// Periodically report progress to straggler detector
			if processedCount%100 == 0 {
				if e.stragglerDetector != nil {
					// Mock progress metric (e.g., 0.0 to 1.0)
					progress := float64(processedCount % 1000) / 1000.0
					e.stragglerDetector.UpdateProgress(partitionID, progress, time.Now())
				}

			}
		}
	}
}

func (e *Engine) Stop() {
	close(e.stop)
}
