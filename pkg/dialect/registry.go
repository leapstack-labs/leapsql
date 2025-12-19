package dialect

import (
	"errors"
	"sort"
	"strings"
	"sync"

	"github.com/leapstack-labs/leapsql/pkg/token"
)

// Dialect registry
var (
	dialectsMu sync.RWMutex
	dialects   = make(map[string]*Dialect)
)

// Global clause registry - tracks ALL tokens that act as clauses in ANY registered dialect.
// Used purely for generating helpful error messages.
var (
	knownClauses = make(map[token.TokenType]string)
	clausesMu    sync.RWMutex
)

// ErrDialectRequired is returned when a dialect is required but not provided.
var ErrDialectRequired = errors.New("dialect is required")

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
func recordClause(t token.TokenType, name string) {
	clausesMu.Lock()
	defer clausesMu.Unlock()
	knownClauses[t] = name
}

// IsKnownClause returns true if ANY registered dialect uses this token as a clause.
// Returns the clause name for error messages.
func IsKnownClause(t token.TokenType) (string, bool) {
	clausesMu.RLock()
	defer clausesMu.RUnlock()
	name, ok := knownClauses[t]
	return name, ok
}

// AllKnownClauses returns all registered clause tokens.
// Useful for debugging and testing.
func AllKnownClauses() map[token.TokenType]string {
	clausesMu.RLock()
	defer clausesMu.RUnlock()
	result := make(map[token.TokenType]string, len(knownClauses))
	for k, v := range knownClauses {
		result[k] = v
	}
	return result
}
