// Package notifier provides a simple broadcast mechanism for SSE updates.
package notifier

import "sync"

// Notifier broadcasts update signals to all subscribed listeners.
// It uses a simple ping mechanism - listeners receive an empty struct
// when updates are available and should re-query the store.
type Notifier struct {
	mu        sync.RWMutex
	listeners map[chan struct{}]struct{}
}

// New creates a new Notifier instance.
func New() *Notifier {
	return &Notifier{
		listeners: make(map[chan struct{}]struct{}),
	}
}

// Subscribe returns a channel that receives pings when updates are available.
// The caller must call Unsubscribe when done to prevent goroutine leaks.
func (n *Notifier) Subscribe() chan struct{} {
	ch := make(chan struct{}, 1)
	n.mu.Lock()
	n.listeners[ch] = struct{}{}
	n.mu.Unlock()
	return ch
}

// Unsubscribe removes a listener channel and closes it.
func (n *Notifier) Unsubscribe(ch chan struct{}) {
	n.mu.Lock()
	delete(n.listeners, ch)
	n.mu.Unlock()
	close(ch)
}

// Broadcast sends a ping to all listeners.
// Non-blocking: if a listener's channel is full, the ping is skipped.
func (n *Notifier) Broadcast() {
	n.mu.RLock()
	defer n.mu.RUnlock()

	for ch := range n.listeners {
		select {
		case ch <- struct{}{}:
		default:
			// Channel full, skip (listener will catch up on next broadcast)
		}
	}
}
