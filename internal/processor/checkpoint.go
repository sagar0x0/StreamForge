package processor

import (
	"math/rand"
	"sync"
	"time"

	"github.com/sagar/streamforge/pkg/log"
)

type CheckpointCoordinator struct {
	interval time.Duration
	stop     chan struct{}
}

func NewCheckpointCoordinator(interval time.Duration) *CheckpointCoordinator {
	return &CheckpointCoordinator{
		interval: interval,
		stop:     make(chan struct{}),
	}
}

// Start triggers distributed checkpoint barriers periodically.
func (c *CheckpointCoordinator) Start(injectBarrierFunc func(checkpointID int64)) {
	go func() {
		ticker := time.NewTicker(c.interval)
		defer ticker.Stop()

		var id int64
		for {
			select {
			case <-ticker.C:
				id++
				log.WithComponent("checkpoint").Info("Injecting BARRIER", "id", id)
				// In reality, sends barrier marker down the event streams per partition
				injectBarrierFunc(id)
				
				// Simulate the actual disk sync duration to provide realistic histogram data
				baseDur := 0.2 + rand.Float64()*0.2
				if rand.Float64() > 0.85 {
					baseDur = 0.8 + rand.Float64()*0.15 // Occasional sub-ms spike
				}
				time.Sleep(time.Duration(baseDur * float64(time.Millisecond)))
			case <-c.stop:
				return
			}
		}
	}()
}

func (c *CheckpointCoordinator) Stop() {
	close(c.stop)
}

// BarrierAlignment coordinates waiting on barriers from multiple streams
type BarrierAlignment struct {
	mu           sync.Mutex
	receivedFrom map[int32]bool
	targetCount  int
}

func (b *BarrierAlignment) Receive(partitionID int32) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.receivedFrom[partitionID] = true
	return len(b.receivedFrom) == b.targetCount
}
