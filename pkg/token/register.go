package token

import "sync/atomic"

// nextTokenID tracks the next available dynamic token ID.
// Dynamic tokens start after maxBuiltin (999).
var nextTokenID = int32(maxBuiltin)

// dynamicTokens maps registered dynamic tokens to their names.
// Protected by atomic operations for the ID counter,
// sync is handled at the dialect registration level.
var dynamicTokens = make(map[TokenType]string)

// dynamicKeywords maps registered dynamic keyword names to their token types.
var dynamicKeywords = make(map[string]TokenType)

// Register registers a new dynamic token with the given name.
// This is used by dialects to register dialect-specific keywords
// like QUALIFY, ILIKE, etc.
//
// Thread-safe: Uses atomic increment for ID generation.
// Registration typically happens at init() time, so concurrent
// registration of the same keyword should be avoided.
func Register(name string) TokenType {
	id := atomic.AddInt32(&nextTokenID, 1)
	t := TokenType(id)

	dynamicTokens[t] = name
	dynamicKeywords[name] = t

	return t
}

// getDynamicName returns the name of a dynamic token.
func getDynamicName(t TokenType) (string, bool) {
	name, ok := dynamicTokens[t]
	return name, ok
}

// LookupDynamicKeyword returns the token type for a dynamic keyword.
// Returns IDENT and false if the keyword is not registered.
func LookupDynamicKeyword(name string) (TokenType, bool) {
	if tok, ok := dynamicKeywords[name]; ok {
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
	result := make(map[TokenType]string, len(dynamicTokens))
	for k, v := range dynamicTokens {
		result[k] = v
	}
	return result
}
