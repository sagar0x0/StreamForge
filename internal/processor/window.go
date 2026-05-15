package processor

import (
	"time"

	"github.com/sagar/streamforge/pkg/types"
)

type WindowType int

const (
	Tumbling WindowType = iota
	Sliding
)

type WindowOperator struct {
	Type          WindowType
	Size          time.Duration
	SlideInterval time.Duration
}

func NewTumblingWindow(size time.Duration) *WindowOperator {
	return &WindowOperator{
		Type: Tumbling,
		Size: size,
	}
}

func NewSlidingWindow(size, slide time.Duration) *WindowOperator {
	return &WindowOperator{
		Type:          Sliding,
		Size:          size,
		SlideInterval: slide,
	}
}

// AssignWindows returns the WindowIDs this timestamp belongs to.
func (w *WindowOperator) AssignWindows(ts time.Time) []types.WindowID {
	if w.Type == Tumbling {
		// e.g. size=10s, ts=15s -> returns window ID representing [10s, 20s]
		winStart := ts.Truncate(w.Size)
		return []types.WindowID{types.WindowID(winStart.UnixNano())}
	}

	if w.Type == Sliding {
		var windows []types.WindowID
		// calculate overlaps based on SlideInterval and Size
		// simplified logic for mock
		winStart := ts.Truncate(w.SlideInterval)
		windows = append(windows, types.WindowID(winStart.UnixNano()))
		return windows
	}

	return nil
}
