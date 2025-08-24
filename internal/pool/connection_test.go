package pool

import (
	"context"
	"testing"
	"time"
)

func TestShutdownContext(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	const shutdownTimeout = 2 * time.Second
	shutdownCtx := shutdownContext(ctx, shutdownTimeout)
	cancel()
	select {
	case <-time.After(shutdownTimeout / 2):
		t.Log("did not cancel at the same time as parent ctx")
	case <-shutdownCtx.Done():
		t.Error("should not cancel for", shutdownTimeout)
	}
	select {
	case <-time.After(shutdownTimeout):
		t.Error("failed to cancel after timeout")
	case <-shutdownCtx.Done():
		t.Log("canceled successfully after timeout")
	}
}
