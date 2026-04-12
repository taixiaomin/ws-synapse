package core

import (
	"log/slog"
	"os"
)

// defaultLogger returns a Logger backed by the standard library's log/slog.
func defaultLogger() Logger {
	return NewSlogAdapter(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))
}

// slogAdapter adapts *slog.Logger to the Logger interface.
type slogAdapter struct {
	logger *slog.Logger
}

func (s *slogAdapter) Info(msg string, keysAndValues ...any) {
	s.logger.Info(msg, keysAndValues...)
}

func (s *slogAdapter) Warn(msg string, keysAndValues ...any) {
	s.logger.Warn(msg, keysAndValues...)
}

func (s *slogAdapter) Error(msg string, keysAndValues ...any) {
	s.logger.Error(msg, keysAndValues...)
}

// NewSlogAdapter wraps an existing *slog.Logger as a Logger.
// If l is nil, creates a new Logger with default settings.
func NewSlogAdapter(l *slog.Logger) Logger {
	if l == nil {
		l = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		}))
	}
	return &slogAdapter{logger: l}
}

// NopLogger is a Logger that discards all output. Useful for tests.
type NopLogger struct{}

func (NopLogger) Info(string, ...any) {}
func (NopLogger) Warn(string, ...any) {}
func (NopLogger) Error(string, ...any) {}

