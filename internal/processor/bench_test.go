package processor

import (
	"fmt"
	"testing"
	"time"

	"github.com/sagar/streamforge/pkg/types"
)

func BenchmarkAggregatorApply(b *testing.B) {
	agg := NewAggregator()
	winID := types.WindowID(1000)
	state := &WindowState{Key: "NYC", WindowID: winID}

	msg := types.Message{
		Value:     []byte(`{"amount": 250.50}`),
		Timestamp: time.Now(),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		state = agg.Apply(msg, state)
	}
}

func BenchmarkWindowAssign_Tumbling(b *testing.B) {
	w := NewTumblingWindow(10 * time.Second)
	ts := time.Now()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w.AssignWindows(ts)
	}
}

func BenchmarkStateStore_PutGet(b *testing.B) {
	store := NewStateStore()
	winID := types.WindowID(1000)

	for i := 0; i < 100; i++ {
		store.Put(winID, fmt.Sprintf("key-%d", i), &WindowState{
			Key: fmt.Sprintf("key-%d", i), Count: int64(i), Sum: float64(i),
		})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i%100)
		store.Get(winID, key)
	}
}

func BenchmarkEngineProcessEvent(b *testing.B) {
	winOp := NewTumblingWindow(10 * time.Second)
	agg := NewAggregator()
	store := NewStateStore()

	now := time.Now().Truncate(10 * time.Second).Add(5 * time.Second)
	msg := types.Message{
		Key:       []byte("NYC"),
		Value:     []byte(`{"amount": 100}`),
		Timestamp: now,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		windows := winOp.AssignWindows(msg.Timestamp)
		for _, wid := range windows {
			state := store.Get(wid, string(msg.Key))
			updated := agg.Apply(msg, state)
			store.Put(wid, string(msg.Key), updated)
		}
	}
}
