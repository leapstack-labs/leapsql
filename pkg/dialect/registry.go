package dialect

import (
	"sort"
	"strings"
	"sync"

	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/token"
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

// recordClause registers a token as a clause keyword.
// Called automatically by Builder.ClauseHandler().
// Delegates to core.RecordClause for the global registry.
func recordClause(t token.TokenType, name string) {
	core.RecordClause(t, name)
}

// IsKnownClause returns true if ANY registered dialect uses this token as a clause.
// Returns the clause name for error messages.
// This is a convenience wrapper around core.IsKnownClause.
func IsKnownClause(t token.TokenType) (string, bool) {
	return core.IsKnownClause(t)
}

// AllKnownClauses returns all registered clause tokens.
// Useful for debugging and testing.
// This is a convenience wrapper around core.AllKnownClauses.
func AllKnownClauses() map[token.TokenType]string {
	return core.AllKnownClauses()
}
