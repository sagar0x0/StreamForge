package speculative

import (
	"testing"
	"time"

	"github.com/sagar/streamforge/pkg/types"
)

func TestStragglerDetectorIdentifiesSlowPartition(t *testing.T) {
	mgr := NewSpeculativeManager()
	detector := NewStragglerDetector(0.5, mgr)

	// Partition 0, 1, 2 are fast. Partition 3 is slow.
	detector.UpdateProgress(0, 0.9, time.Now())
	detector.UpdateProgress(1, 0.85, time.Now())
	detector.UpdateProgress(2, 0.88, time.Now())
	detector.UpdateProgress(3, 0.2, time.Now()) // straggler

	median := detector.computeMedianProgress()
	// Sorted: [0.2, 0.85, 0.88, 0.9] → median = (0.85+0.88)/2 = 0.865
	if median < 0.85 || median > 0.90 {
		t.Fatalf("expected median ~0.865, got %f", median)
	}

	// Partition 3's progress (0.2) < median*0.5 (0.4325) → straggler
	if detector.workers[3].WindowProgress >= median*0.5 {
		t.Fatal("partition 3 should be identified as straggler")
	}
}

func TestResultArbitratorFirstWriteWins(t *testing.T) {
	arb := NewResultArbitrator()

	winID := types.WindowID(1000)
	result := types.AggregatedResult{
		WindowID: winID,
		Key:      "NYC",
		Count:    100,
	}

	// First submission wins
	accepted := arb.Submit(winID, 0, result)
	if !accepted {
		t.Fatal("first submission should be accepted")
	}

	// Second submission (duplicate from speculative task) should be rejected
	accepted2 := arb.Submit(winID, 0, result)
	if accepted2 {
		t.Fatal("duplicate submission should be rejected due to tombstone")
	}

	// Different partition should still be accepted
	accepted3 := arb.Submit(winID, 1, result)
	if !accepted3 {
		t.Fatal("submission for different partition should be accepted")
	}
}

func TestSpeculativeManagerLaunchAndCancel(t *testing.T) {
	mgr := NewSpeculativeManager()

	mgr.Launch(3, time.Now())
	if !mgr.activeTaskIDs[3] {
		t.Fatal("partition 3 should have an active speculative task")
	}

	// Launching again should be a no-op
	mgr.Launch(3, time.Now())

	mgr.Cancel(3)
	if mgr.activeTaskIDs[3] {
		t.Fatal("partition 3 should no longer have an active task after cancel")
	}
}
