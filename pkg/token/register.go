package token

import (
	"maps"
	"strings"
	"sync"
	"sync/atomic"
)

var (
	// nextTokenID tracks the next available dynamic token ID.
	// Dynamic tokens start after maxBuiltin (999).
	nextTokenID = int32(maxBuiltin)

	// dynamicTokens maps registered dynamic tokens to their names (ID -> Name).
	// Names are stored in uppercase for display purposes.
	dynamicTokens = make(map[TokenType]string)

	// dynamicKeywords maps registered dynamic keyword names to their token types (Name -> ID).
	// Names are normalized to lowercase for case-insensitive lookup.
	dynamicKeywords = make(map[string]TokenType)

	// registryMu protects the registry maps during concurrent access.
	registryMu sync.RWMutex
)

// Register registers a dynamic token with the given name.
// If a token with the same name already exists, returns the existing ID.
// This allows multiple dialects to register the same keyword (e.g., ILIKE)
// and share the same token ID.
//
// The name is normalized to lowercase for lookup but stored in uppercase for display.
// Thread-safe: Uses mutex to protect concurrent registration.
func Register(name string) TokenType {
	lowerName := strings.ToLower(name)

	// Fast path: check if already registered (read lock)
	registryMu.RLock()
	if existing, ok := dynamicKeywords[lowerName]; ok {
		registryMu.RUnlock()
		return existing
	}
	registryMu.RUnlock()

	// Slow path: need to register (write lock)
	registryMu.Lock()
	defer registryMu.Unlock()

	// Double-check after acquiring write lock (another goroutine may have registered)
	if existing, ok := dynamicKeywords[lowerName]; ok {
		return existing
	}

	// Create new token
	id := atomic.AddInt32(&nextTokenID, 1)
	t := TokenType(id)

	// Store uppercase for display, lowercase for lookup
	dynamicTokens[t] = strings.ToUpper(name)
	dynamicKeywords[lowerName] = t

	return t
}

// getDynamicName returns the name of a dynamic token.
func getDynamicName(t TokenType) (string, bool) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	name, ok := dynamicTokens[t]
	return name, ok
}

// LookupDynamicKeyword returns the token type for a dynamic keyword.
// The lookup is case-insensitive.
// Returns IDENT and false if the keyword is not registered.
func LookupDynamicKeyword(name string) (TokenType, bool) {
	registryMu.RLock()
	defer registryMu.RUnlock()
	lowerName := strings.ToLower(name)
	if tok, ok := dynamicKeywords[lowerName]; ok {
		return tok, true
	}
	return IDENT, false
}

// IsDynamic returns true if the token type is a dynamically registered token.
func IsDynamic(t TokenType) bool {
	return t > maxBuiltin
}

// RegisteredTokens returns a copy of all registered dynamic tokens.
func RegisteredTokens() map[TokenType]string {
	registryMu.RLock()
	defer registryMu.RUnlock()
	result := make(map[TokenType]string, len(dynamicTokens))
	maps.Copy(result, dynamicTokens)
	return result
}
