package speculative

import (
	"testing"
	"time"

	"github.com/sagar/streamforge/pkg/types"
)

func BenchmarkStragglerDetector_UpdateProgress(b *testing.B) {
	mgr := NewSpeculativeManager()
	det := NewStragglerDetector(0.5, mgr)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		det.UpdateProgress(int32(i%8), float64(i%100)/100.0, time.Now())
	}
}

func BenchmarkStragglerDetector_ComputeMedian(b *testing.B) {
	mgr := NewSpeculativeManager()
	det := NewStragglerDetector(0.5, mgr)

	// Pre-populate 8 partitions
	for i := 0; i < 8; i++ {
		det.UpdateProgress(int32(i), float64(i+1)/10.0, time.Now())
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		det.mu.Lock()
		det.computeMedianProgress()
		det.mu.Unlock()
	}
}

func BenchmarkResultArbitrator_Submit(b *testing.B) {
	arb := NewResultArbitrator()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		winID := types.WindowID(int64(i))
		arb.Submit(winID, 0, types.AggregatedResult{WindowID: winID, Key: "NYC"})
	}
}
