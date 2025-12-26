package adapter_test

import (
	"testing"

	"github.com/leapstack-labs/leapsql/pkg/adapter"
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	// Import adapter packages to ensure adapters are registered via init()
	_ "github.com/leapstack-labs/leapsql/pkg/adapters/duckdb"
	_ "github.com/leapstack-labs/leapsql/pkg/adapters/postgres"
)

func TestDuckDBSelfRegistration(t *testing.T) {
	// DuckDB should be auto-registered via init()
	assert.True(t, adapter.IsRegistered("duckdb"), "duckdb adapter should be auto-registered")
}

func TestListAdapters(t *testing.T) {
	adapters := adapter.ListAdapters()

	// Should contain at least duckdb and postgres
	assert.Contains(t, adapters, "duckdb", "duckdb should be in adapter list")
	assert.Contains(t, adapters, "postgres", "postgres should be in adapter list")
}

func TestIsRegistered(t *testing.T) {
	tests := []struct {
		name        string
		adapterName string
		expected    bool
	}{
		{"duckdb registered", "duckdb", true},
		{"postgres registered", "postgres", true},
		{"unknown not registered", "unknown_db", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := adapter.IsRegistered(tt.adapterName)
			assert.Equal(t, tt.expected, got, "IsRegistered(%q)", tt.adapterName)
		})
	}
}

func TestGet(t *testing.T) {
	// Get existing adapter
	factory, ok := adapter.Get("duckdb")
	require.True(t, ok, "Get(duckdb) should return true")
	require.NotNil(t, factory, "Get(duckdb) should return non-nil factory")

	// Get non-existing adapter
	_, ok = adapter.Get("nonexistent")
	assert.False(t, ok, "Get(nonexistent) should return false")
}

func TestNewAdapter_Success(t *testing.T) {
	cfg := core.AdapterConfig{
		Type: "duckdb",
		Path: ":memory:",
	}

	adp, err := adapter.NewAdapter(cfg, nil)
	require.NoError(t, err, "NewAdapter(duckdb) failed")
	require.NotNil(t, adp, "NewAdapter(duckdb) returned nil adapter")
}

func TestNewAdapter_UnknownType(t *testing.T) {
	cfg := core.AdapterConfig{
		Type: "unknown_adapter",
	}

	_, err := adapter.NewAdapter(cfg, nil)
	require.Error(t, err, "NewAdapter(unknown_adapter) should fail")

	// Check error type
	var unknownErr *adapter.UnknownAdapterError
	require.ErrorAs(t, err, &unknownErr)

	assert.Equal(t, "unknown_adapter", unknownErr.Type, "error type")

	// Available should include duckdb
	assert.Contains(t, unknownErr.Available, "duckdb", "Available adapters should include duckdb")
}
