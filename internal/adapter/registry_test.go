package adapter

import (
	"testing"
)

func TestDuckDBSelfRegistration(t *testing.T) {
	// DuckDB should be auto-registered via init()
	if !IsRegistered("duckdb") {
		t.Error("duckdb adapter should be auto-registered")
	}
}

func TestListAdapters(t *testing.T) {
	adapters := ListAdapters()

	// Should contain at least duckdb
	found := false
	for _, name := range adapters {
		if name == "duckdb" {
			found = true
			break
		}
	}

	if !found {
		t.Errorf("duckdb should be in adapter list, got: %v", adapters)
	}
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
			if got != tt.expected {
				t.Errorf("IsRegistered(%q) = %v, want %v", tt.adapter, got, tt.expected)
			}
		})
	}
}

func TestGet(t *testing.T) {
	// Get existing adapter
	factory, ok := Get("duckdb")
	if !ok {
		t.Fatal("Get(duckdb) should return true")
	}
	if factory == nil {
		t.Fatal("Get(duckdb) should return non-nil factory")
	}

	// Get non-existing adapter
	_, ok = Get("nonexistent")
	if ok {
		t.Error("Get(nonexistent) should return false")
	}
}

func TestNewAdapter_Success(t *testing.T) {
	cfg := Config{
		Type: "duckdb",
		Path: ":memory:",
	}

	adapter, err := NewAdapter(cfg)
	if err != nil {
		t.Fatalf("NewAdapter(duckdb) failed: %v", err)
	}
	if adapter == nil {
		t.Fatal("NewAdapter(duckdb) returned nil adapter")
	}
}

func TestNewAdapter_UnknownType(t *testing.T) {
	cfg := Config{
		Type: "unknown_adapter",
	}

	_, err := NewAdapter(cfg)
	if err == nil {
		t.Fatal("NewAdapter(unknown_adapter) should fail")
	}

	// Check error type
	unknownErr, ok := err.(*UnknownAdapterError)
	if !ok {
		t.Fatalf("expected *UnknownAdapterError, got %T", err)
	}

	if unknownErr.Type != "unknown_adapter" {
		t.Errorf("error type should be 'unknown_adapter', got %q", unknownErr.Type)
	}

	// Available should include duckdb
	found := false
	for _, name := range unknownErr.Available {
		if name == "duckdb" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Available adapters should include duckdb, got: %v", unknownErr.Available)
	}
}

func TestNewAdapter_EmptyType(t *testing.T) {
	cfg := Config{
		Type: "",
	}

	_, err := NewAdapter(cfg)
	if err == nil {
		t.Fatal("NewAdapter with empty type should fail")
	}

	expected := "adapter type not specified"
	if err.Error() != expected {
		t.Errorf("error message should be %q, got %q", expected, err.Error())
	}
}

func TestUnknownAdapterError_Error(t *testing.T) {
	err := &UnknownAdapterError{
		Type:      "fake_db",
		Available: []string{"duckdb", "postgres"},
	}

	msg := err.Error()

	// Check that error message contains important info
	if msg == "" {
		t.Fatal("error message should not be empty")
	}

	// Should mention the type
	if !contains(msg, "fake_db") {
		t.Errorf("error should mention the unknown type 'fake_db'")
	}

	// Should hint about config
	if !contains(msg, "leapsql.yaml") {
		t.Errorf("error should mention config file")
	}
}

func TestRegister(t *testing.T) {
	// Register a mock adapter
	Register("test_adapter", func() Adapter { return nil })
	defer func() {
		// Clean up by checking if test_adapter exists
		// Note: We can't unregister, but that's OK for tests
	}()

	if !IsRegistered("test_adapter") {
		t.Error("test_adapter should be registered after Register()")
	}

	factory, ok := Get("test_adapter")
	if !ok {
		t.Error("Get(test_adapter) should return true after Register()")
	}
	if factory == nil {
		t.Error("Get(test_adapter) should return non-nil factory")
	}
}

// Helper function
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
