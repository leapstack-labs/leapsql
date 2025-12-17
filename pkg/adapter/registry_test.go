package adapter

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
	Register("test_adapter_internal", func(_ *slog.Logger) Adapter { return nil })

	assert.True(t, IsRegistered("test_adapter_internal"), "test_adapter_internal should be registered after Register()")

	factory, ok := Get("test_adapter_internal")
	assert.True(t, ok, "Get(test_adapter_internal) should return true after Register()")
	assert.NotNil(t, factory, "Get(test_adapter_internal) should return non-nil factory")
}

func TestNewAdapter_EmptyType(t *testing.T) {
	cfg := Config{
		Type: "",
	}

	_, err := NewAdapter(cfg, nil)
	require.Error(t, err, "NewAdapter with empty type should fail")
	assert.Equal(t, "adapter type not specified", err.Error(), "error message")
}
