package processor

import (
	"fmt"
	"testing"
	"time"

	"github.com/sagar/streamforge/pkg/types"
)

func TestTumblingWindowAssignment(t *testing.T) {
	w := NewTumblingWindow(10 * time.Second)

	// Event at t=15s should go into window starting at t=10s
	ts := time.Unix(15, 0)
	windows := w.AssignWindows(ts)
	if len(windows) != 1 {
		t.Fatalf("expected 1 window, got %d", len(windows))
	}

	expected := types.WindowID(time.Unix(10, 0).UnixNano())
	if windows[0] != expected {
		t.Fatalf("expected window ID %d, got %d", expected, windows[0])
	}

	// Event at t=25s should go to window starting at t=20s
	ts2 := time.Unix(25, 0)
	windows2 := w.AssignWindows(ts2)
	expected2 := types.WindowID(time.Unix(20, 0).UnixNano())
	if windows2[0] != expected2 {
		t.Fatalf("expected window ID %d, got %d", expected2, windows2[0])
	}
}

func TestAggregatorApplyAndEmit(t *testing.T) {
	agg := NewAggregator()
	winID := types.WindowID(1000)

	state := &WindowState{Key: "NYC", WindowID: winID}

	// Apply 3 events
	amounts := []float64{250.0, 120.0, 890.0}
	for _, amount := range amounts {
		msg := types.Message{
			Value:     []byte(fmt.Sprintf(`{"amount": %f}`, amount)),
			Timestamp: time.Now(),
		}
		state = agg.Apply(msg, state)
	}

	if state.Count != 3 {
		t.Fatalf("expected count 3, got %d", state.Count)
	}

	expectedSum := 1260.0
	if state.Sum != expectedSum {
		t.Fatalf("expected sum %f, got %f", expectedSum, state.Sum)
	}

	if state.Min != 120.0 {
		t.Fatalf("expected min 120.0, got %f", state.Min)
	}

	if state.Max != 890.0 {
		t.Fatalf("expected max 890.0, got %f", state.Max)
	}

	// Test emit
	result := agg.Emit(state)
	expectedAvg := 420.0
	if result.Avg != expectedAvg {
		t.Fatalf("expected avg %f, got %f", expectedAvg, result.Avg)
	}
}

func TestStateStorePutGet(t *testing.T) {
	store := NewStateStore()
	winID := types.WindowID(1000)

	state := &WindowState{
		Key:      "NYC",
		WindowID: winID,
		Count:    5,
		Sum:      500.0,
	}
	store.Put(winID, "NYC", state)

	got := store.Get(winID, "NYC")
	if got.Count != 5 || got.Sum != 500.0 {
		t.Fatalf("state mismatch: got count=%d sum=%f", got.Count, got.Sum)
	}

	// Modify returned state should not affect stored state (should be a copy)
	got.Count = 99
	stored := store.Get(winID, "NYC")
	if stored.Count == 99 {
		t.Fatal("store returned reference instead of copy")
	}

	// Missing key should return empty state
	missing := store.Get(winID, "UNKNOWN")
	if missing.Count != 0 {
		t.Fatal("expected empty state for unknown key")
	}
}

func TestEngineProcessStream(t *testing.T) {
	winOp := NewTumblingWindow(10 * time.Second)
	agg := NewAggregator()
	store := NewStateStore()

	engine := NewEngine(winOp, agg, store, nil, nil)
	defer engine.Stop()

	stream := make(chan types.Message, 100)

	go engine.ProcessStream(0, stream)

	// Send 5 events for "NYC"
	now := time.Now().Truncate(10 * time.Second).Add(5 * time.Second) // mid-window
	for i := 0; i < 5; i++ {
		stream <- types.Message{
			Key:       []byte("NYC"),
			Value:     []byte(`{"amount": 100}`),
			Timestamp: now,
		}
	}

	// Give the goroutine time to process
	time.Sleep(200 * time.Millisecond)

	winID := types.WindowID(now.Truncate(10 * time.Second).UnixNano())
	state := store.Get(winID, "NYC")
	if state.Count != 5 {
		t.Fatalf("expected count 5, got %d", state.Count)
	}
	if state.Sum != 500.0 {
		t.Fatalf("expected sum 500.0, got %f", state.Sum)
	}
}
