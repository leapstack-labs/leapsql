package adapter

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDuckDBSelfRegistration(t *testing.T) {
	// DuckDB should be auto-registered via init()
	assert.True(t, IsRegistered("duckdb"), "duckdb adapter should be auto-registered")
}

func TestListAdapters(t *testing.T) {
	adapters := ListAdapters()

	// Should contain at least duckdb
	assert.Contains(t, adapters, "duckdb", "duckdb should be in adapter list")
}

func TestIsRegistered(t *testing.T) {
	tests := []struct {
		name     string
		adapter  string
		expected bool
	}{
		{"duckdb registered", "duckdb", true},
		{"unknown not registered", "unknown_db", false},
		{"postgres not registered yet", "postgres", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsRegistered(tt.adapter)
			assert.Equal(t, tt.expected, got, "IsRegistered(%q)", tt.adapter)
		})
	}
}

func TestGet(t *testing.T) {
	// Get existing adapter
	factory, ok := Get("duckdb")
	require.True(t, ok, "Get(duckdb) should return true")
	require.NotNil(t, factory, "Get(duckdb) should return non-nil factory")

	// Get non-existing adapter
	_, ok = Get("nonexistent")
	assert.False(t, ok, "Get(nonexistent) should return false")
}

func TestNewAdapter_Success(t *testing.T) {
	cfg := Config{
		Type: "duckdb",
		Path: ":memory:",
	}

	adapter, err := NewAdapter(cfg)
	require.NoError(t, err, "NewAdapter(duckdb) failed")
	require.NotNil(t, adapter, "NewAdapter(duckdb) returned nil adapter")
}

func TestNewAdapter_UnknownType(t *testing.T) {
	cfg := Config{
		Type: "unknown_adapter",
	}

	_, err := NewAdapter(cfg)
	require.Error(t, err, "NewAdapter(unknown_adapter) should fail")

	// Check error type
	var unknownErr *UnknownAdapterError
	require.ErrorAs(t, err, &unknownErr)

	assert.Equal(t, "unknown_adapter", unknownErr.Type, "error type")

	// Available should include duckdb
	assert.Contains(t, unknownErr.Available, "duckdb", "Available adapters should include duckdb")
}

func TestNewAdapter_EmptyType(t *testing.T) {
	cfg := Config{
		Type: "",
	}

	_, err := NewAdapter(cfg)
	require.Error(t, err, "NewAdapter with empty type should fail")

	assert.Equal(t, "adapter type not specified", err.Error(), "error message")
}

func TestUnknownAdapterError_Error(t *testing.T) {
	err := &UnknownAdapterError{
		Type:      "fake_db",
		Available: []string{"duckdb", "postgres"},
	}

	msg := err.Error()

	// Check that error message contains important info
	assert.NotEmpty(t, msg, "error message should not be empty")

	// Should mention the type
	assert.Contains(t, msg, "fake_db", "error should mention the unknown type 'fake_db'")

	// Should hint about config
	assert.Contains(t, msg, "leapsql.yaml", "error should mention config file")
}

func TestRegister(t *testing.T) {
	// Register a mock adapter
	Register("test_adapter", func() Adapter { return nil })
	defer func() {
		// Clean up by checking if test_adapter exists
		// Note: We can't unregister, but that's OK for tests
	}()

	assert.True(t, IsRegistered("test_adapter"), "test_adapter should be registered after Register()")

	factory, ok := Get("test_adapter")
	assert.True(t, ok, "Get(test_adapter) should return true after Register()")
	assert.NotNil(t, factory, "Get(test_adapter) should return non-nil factory")
}
