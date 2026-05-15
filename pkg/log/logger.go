package log

import (
	"log/slog"
	"os"
)

var Logger *slog.Logger

func init() {
	opts := &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}
	handler := slog.NewTextHandler(os.Stdout, opts)
	Logger = slog.New(handler)
}

// WithComponent creates a logger with the given component field attached
func WithComponent(name string) *slog.Logger {
	return Logger.With(slog.String("component", name))
}

// WithPartition creates a logger with the given partition field attached
func WithPartition(id int) *slog.Logger {
	return Logger.With(slog.Int("partition", id))
}
