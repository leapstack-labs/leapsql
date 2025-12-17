// Package testutil provides test utilities for structured logging.
package testutil

import (
	"log/slog"
	"testing"
)

// NewTestLogger returns a logger that writes to t.Log().
// Logs only appear on test failure or when running with -v.
func NewTestLogger(t testing.TB) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
}

type testWriter struct {
	t testing.TB
}

func (w testWriter) Write(p []byte) (n int, err error) {
	w.t.Helper()
	w.t.Log(string(p))
	return len(p), nil
}
