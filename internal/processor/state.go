package processor

import (
	"sync"
	"time"

	"github.com/sagar/streamforge/pkg/types"
)

type WindowState struct {
	Key        string
	WindowID   types.WindowID
	Count      int64
	Sum        float64
	Min        float64
	Max        float64
	LastUpdate time.Time
}

type StateStore struct {
	mu    sync.RWMutex
	store map[types.WindowID]map[string]*WindowState
}

func NewStateStore() *StateStore {
	return &StateStore{
		store: make(map[types.WindowID]map[string]*WindowState),
	}
}

func (s *StateStore) Get(winID types.WindowID, key string) *WindowState {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if _, ok := s.store[winID]; !ok {
		return &WindowState{Key: key, WindowID: winID}
	}
	if state, ok := s.store[winID][key]; ok {
		// return copy
		return &WindowState{
			Key:        state.Key,
			WindowID:   state.WindowID,
			Count:      state.Count,
			Sum:        state.Sum,
			Min:        state.Min,
			Max:        state.Max,
			LastUpdate: state.LastUpdate,
		}
	}
	return &WindowState{Key: key, WindowID: winID}
}

func (s *StateStore) Put(winID types.WindowID, key string, state *WindowState) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.store[winID]; !ok {
		s.store[winID] = make(map[string]*WindowState)
	}
	s.store[winID][key] = state
}

func (s *StateStore) Snapshot() []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()
	// Mock serialization for Chandy-Lamport barrier triggers
	return []byte("serialized_state_data")
}
