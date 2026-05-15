package processor

import (
	"encoding/json"
	"math"

	"github.com/sagar/streamforge/pkg/types"
)

type Payload struct {
	Amount float64 `json:"amount"`
}

type Aggregator struct {}

func NewAggregator() *Aggregator {
	return &Aggregator{}
}

func (a *Aggregator) Apply(msg types.Message, currentState *WindowState) *WindowState {
	var p Payload
	_ = json.Unmarshal(msg.Value, &p) // ignore err for mock

	if currentState.Count == 0 {
		currentState.Min = math.MaxFloat64
		currentState.Max = -math.MaxFloat64
	}

	currentState.Count++
	currentState.Sum += p.Amount
	
	if p.Amount < currentState.Min {
		currentState.Min = p.Amount
	}
	if p.Amount > currentState.Max {
		currentState.Max = p.Amount
	}

	currentState.LastUpdate = msg.Timestamp
	return currentState
}

func (a *Aggregator) Emit(state *WindowState) types.AggregatedResult {
	avg := 0.0
	if state.Count > 0 {
		avg = state.Sum / float64(state.Count)
	}

	return types.AggregatedResult{
		WindowID: state.WindowID,
		Key:      state.Key,
		Count:    state.Count,
		Sum:      state.Sum,
		Avg:      avg,
		Min:      state.Min,
		Max:      state.Max,
	}
}
