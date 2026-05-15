package speculative

import (
	"sort"
	"sync"
	"time"

	"github.com/sagar/streamforge/pkg/log"
)

// WorkerStats tracks per-partition processing progress.
type WorkerStats struct {
	PartitionID     int32
	EventsProcessed int64
	WindowProgress  float64
	LastCheckpoint  time.Time
	IsSpeculative   bool
}

// StragglerDetector monitors partition processing speeds and triggers
// speculative execution when a partition falls behind the median.
type StragglerDetector struct {
	mu             sync.Mutex
	workers        map[int32]*WorkerStats
	threshold      float64
	checkInterval  time.Duration
	speculativeMgr *SpeculativeManager
	stop           chan struct{}
}

func NewStragglerDetector(threshold float64, mgr *SpeculativeManager) *StragglerDetector {
	return &StragglerDetector{
		workers:        make(map[int32]*WorkerStats),
		threshold:      threshold,
		checkInterval:  100 * time.Millisecond,
		speculativeMgr: mgr,
		stop:           make(chan struct{}),
	}
}

func (d *StragglerDetector) UpdateProgress(partitionID int32, progress float64, lastCheckpoint time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.workers[partitionID]; !ok {
		d.workers[partitionID] = &WorkerStats{PartitionID: partitionID}
	}
	d.workers[partitionID].WindowProgress = progress
	d.workers[partitionID].LastCheckpoint = lastCheckpoint
}

// computeMedianProgress returns the median progress across all workers.
// Caller must hold d.mu.
func (d *StragglerDetector) computeMedianProgress() float64 {
	if len(d.workers) == 0 {
		return 0
	}
	progresses := make([]float64, 0, len(d.workers))
	for _, w := range d.workers {
		progresses = append(progresses, w.WindowProgress)
	}
	sort.Float64s(progresses)
	n := len(progresses)
	if n%2 == 0 {
		return (progresses[n/2-1] + progresses[n/2]) / 2.0
	}
	return progresses[n/2]
}

func (d *StragglerDetector) monitor() {
	ticker := time.NewTicker(d.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			d.mu.Lock()
			median := d.computeMedianProgress()
			for id, worker := range d.workers {
				if worker.WindowProgress < median*d.threshold {
					if !worker.IsSpeculative {
						log.WithPartition(int(id)).Info("Detected straggler, launching speculative task",
							"progress", worker.WindowProgress, "median", median)
						d.speculativeMgr.Launch(id, worker.LastCheckpoint)
						worker.IsSpeculative = true
					}
				}
			}
			d.mu.Unlock()
		case <-d.stop:
			return
		}
	}
}

func (d *StragglerDetector) Start() {
	go d.monitor()
}

func (d *StragglerDetector) Stop() {
	close(d.stop)
}
