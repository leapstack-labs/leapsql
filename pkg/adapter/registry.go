package adapter

import (
	"fmt"
	"log/slog"
	"sort"
	"sync"

	"github.com/leapstack-labs/leapsql/pkg/core"
)

var (
	registryMu sync.RWMutex
	registry   = make(map[string]func(*slog.Logger) Adapter)
)

// Register adds an adapter factory to the registry.
// Called by adapter implementations in their init() functions.
func Register(name string, factory func(*slog.Logger) Adapter) {
	registryMu.Lock()
	defer registryMu.Unlock()
	registry[name] = factory
}

// Get retrieves an adapter factory by name.
func Get(name string) (func(*slog.Logger) Adapter, bool) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	f, ok := registry[name]
	return f, ok
}

// NewAdapter creates a new adapter instance based on config type.
// The logger parameter is passed to the adapter constructor (nil uses discard logger).
func NewAdapter(cfg core.AdapterConfig, logger *slog.Logger) (Adapter, error) {
	if cfg.Type == "" {
		return nil, fmt.Errorf("adapter type not specified")
	}

	factory, ok := Get(cfg.Type)
	if !ok {
		return nil, &UnknownAdapterError{
			Type:      cfg.Type,
			Available: ListAdapters(),
		}
	}
	return factory(logger), nil
}

// ListAdapters returns all registered adapter names (sorted).
func ListAdapters() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()
	names := make([]string, 0, len(registry))
	for name := range registry {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// IsRegistered checks if an adapter type is registered.
func IsRegistered(name string) bool {
	registryMu.RLock()
	defer registryMu.RUnlock()
	_, ok := registry[name]
	return ok
}

// UnknownAdapterError is returned when an unknown adapter type is requested.
type UnknownAdapterError struct {
	Type      string
	Available []string
}

func (e *UnknownAdapterError) Error() string {
	return fmt.Sprintf("unknown adapter type %q\nAvailable adapters: %v\nHint: Check your target.type in leapsql.yaml", e.Type, e.Available)
}
