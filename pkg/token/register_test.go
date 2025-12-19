package token

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRegisterIdempotent(t *testing.T) {
	// Register same name twice
	id1 := Register("TEST_IDEMPOTENT")
	id2 := Register("TEST_IDEMPOTENT")

	assert.Equal(t, id1, id2, "same name should return same ID")
}

func TestRegisterDifferentNames(t *testing.T) {
	id1 := Register("TEST_NAME_A")
	id2 := Register("TEST_NAME_B")

	assert.NotEqual(t, id1, id2, "different names should return different IDs")
}

func TestRegisterConcurrent(t *testing.T) {
	const numGoroutines = 100
	var wg sync.WaitGroup
	ids := make([]TokenType, numGoroutines)

	// Register same name concurrently
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			ids[idx] = Register("TEST_CONCURRENT")
		}(i)
	}
	wg.Wait()

	// All should have the same ID
	for i := 1; i < numGoroutines; i++ {
		require.Equal(t, ids[0], ids[i], "concurrent registration should return same ID")
	}
}

func TestLookupDynamicKeyword(t *testing.T) {
	name := "TEST_LOOKUP"
	expectedID := Register(name)

	gotID, ok := LookupDynamicKeyword(name)
	require.True(t, ok, "registered keyword should be found")
	assert.Equal(t, expectedID, gotID)

	_, ok = LookupDynamicKeyword("NONEXISTENT_KEYWORD_12345")
	assert.False(t, ok, "unregistered keyword should not be found")
}

func TestIsDynamic(t *testing.T) {
	// Built-in tokens are not dynamic
	assert.False(t, IsDynamic(SELECT))
	assert.False(t, IsDynamic(WHERE))
	assert.False(t, IsDynamic(EOF))

	// Registered tokens are dynamic
	dynamicToken := Register("TEST_DYNAMIC_CHECK")
	assert.True(t, IsDynamic(dynamicToken))
}

func TestRegisteredTokens(t *testing.T) {
	name := "TEST_REGISTERED_TOKENS"
	id := Register(name)

	tokens := RegisteredTokens()
	assert.Equal(t, name, tokens[id])

	// Verify it's a copy (modifications don't affect original)
	tokens[id] = "MODIFIED"
	tokens2 := RegisteredTokens()
	assert.Equal(t, name, tokens2[id], "RegisteredTokens should return a copy")
}

func TestGetDynamicName(t *testing.T) {
	name := "TEST_GET_DYNAMIC_NAME"
	id := Register(name)

	gotName, ok := getDynamicName(id)
	require.True(t, ok)
	assert.Equal(t, name, gotName)

	// Non-existent dynamic token
	_, ok = getDynamicName(TokenType(99999))
	assert.False(t, ok)
}
