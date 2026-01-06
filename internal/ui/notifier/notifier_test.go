package notifier

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNotifier_Subscribe_Unsubscribe(t *testing.T) {
	n := New()

	// Subscribe creates a channel
	ch := n.Subscribe()
	require.NotNil(t, ch)

	// Verify listener is added
	n.mu.RLock()
	assert.Len(t, n.listeners, 1)
	n.mu.RUnlock()

	// Unsubscribe removes the channel
	n.Unsubscribe(ch)

	n.mu.RLock()
	assert.Len(t, n.listeners, 0)
	n.mu.RUnlock()
}

func TestNotifier_Broadcast(t *testing.T) {
	n := New()

	// Create multiple subscribers
	ch1 := n.Subscribe()
	ch2 := n.Subscribe()
	defer n.Unsubscribe(ch1)
	defer n.Unsubscribe(ch2)

	// Broadcast should notify both
	n.Broadcast()

	// Both channels should receive
	select {
	case <-ch1:
		// OK
	case <-time.After(100 * time.Millisecond):
		t.Error("ch1 did not receive broadcast")
	}

	select {
	case <-ch2:
		// OK
	case <-time.After(100 * time.Millisecond):
		t.Error("ch2 did not receive broadcast")
	}
}

func TestNotifier_Broadcast_NonBlocking(t *testing.T) {
	n := New()

	ch := n.Subscribe()
	defer n.Unsubscribe(ch)

	// Fill the channel buffer
	ch <- struct{}{}

	// Broadcast should not block even if channel is full
	done := make(chan bool)
	go func() {
		n.Broadcast()
		done <- true
	}()

	select {
	case <-done:
		// OK - broadcast completed
	case <-time.After(100 * time.Millisecond):
		t.Error("Broadcast blocked on full channel")
	}
}

func TestNotifier_Concurrent(t *testing.T) {
	n := New()

	var wg sync.WaitGroup
	const numGoroutines = 10

	// Concurrent subscribe/unsubscribe/broadcast
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ch := n.Subscribe()
			n.Broadcast()
			n.Unsubscribe(ch)
		}()
	}

	wg.Wait()

	// All listeners should be cleaned up
	n.mu.RLock()
	assert.Len(t, n.listeners, 0)
	n.mu.RUnlock()
}
