package core

import (
	"sync"

	"github.com/leapstack-labs/leapsql/pkg/token"
)

// Global clause registry - tracks ALL tokens that act as clauses in ANY registered dialect.
// Used purely for generating helpful error messages.
var (
	knownClauses = make(map[token.TokenType]string)
	clausesMu    sync.RWMutex
)

// RecordClause registers a token as a clause keyword.
// Called automatically by dialect.Builder.ClauseHandler().
func RecordClause(t token.TokenType, name string) {
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
