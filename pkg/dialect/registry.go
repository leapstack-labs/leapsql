package dialect

import (
	"sort"
	"strings"
	"sync"
)

// Dialect registry
var (
	dialectsMu sync.RWMutex
	dialects   = make(map[string]*Dialect)
)

// Get returns a dialect by name.
func Get(name string) (*Dialect, bool) {
	dialectsMu.RLock()
	defer dialectsMu.RUnlock()
	d, ok := dialects[strings.ToLower(name)]
	return d, ok
}

// Register registers a dialect in the global registry.
// Called by dialect implementations in their init() functions.
func Register(d *Dialect) {
	dialectsMu.Lock()
	defer dialectsMu.Unlock()
	dialects[strings.ToLower(d.Name)] = d
}

// List returns all registered dialect names (sorted).
func List() []string {
	dialectsMu.RLock()
	defer dialectsMu.RUnlock()
	names := make([]string, 0, len(dialects))
	for name := range dialects {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// defaultDialect stores the default dialect (set by first registration or explicitly)
var defaultDialect *Dialect

// Default returns the default dialect.
// Returns nil if no dialects have been registered.
func Default() *Dialect {
	dialectsMu.RLock()
	defer dialectsMu.RUnlock()
	return defaultDialect
}

// SetDefault sets the default dialect.
func SetDefault(d *Dialect) {
	dialectsMu.Lock()
	defer dialectsMu.Unlock()
	defaultDialect = d
}
