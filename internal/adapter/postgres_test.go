package adapter

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildPostgresDSN(t *testing.T) {
	tests := []struct {
		name     string
		config   Config
		expected string
	}{
		{
			name: "basic connection",
			config: Config{
				Host:     "localhost",
				Port:     5432,
				Database: "testdb",
				Username: "user",
				Password: "pass",
			},
			expected: "host=localhost port=5432 dbname=testdb sslmode=disable user=user password=pass",
		},
		{
			name: "with custom sslmode",
			config: Config{
				Host:     "prod.example.com",
				Port:     5432,
				Database: "proddb",
				Username: "admin",
				Options:  map[string]string{"sslmode": "require"},
			},
			expected: "host=prod.example.com port=5432 dbname=proddb sslmode=require user=admin",
		},
		{
			name: "defaults",
			config: Config{
				Database: "mydb",
			},
			expected: "host=localhost port=5432 dbname=mydb sslmode=disable",
		},
		{
			name: "custom port",
			config: Config{
				Host:     "db.example.com",
				Port:     5433,
				Database: "analytics",
				Username: "analyst",
			},
			expected: "host=db.example.com port=5433 dbname=analytics sslmode=disable user=analyst",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dsn := buildPostgresDSN(tt.config)
			assert.Equal(t, tt.expected, dsn)
		})
	}
}

func TestPostgresAdapter_DialectName(t *testing.T) {
	adapter := NewPostgresAdapter()
	assert.Equal(t, "postgres", adapter.DialectName())
}

func TestPostgresAdapter_NotConnected(t *testing.T) {
	tests := []struct {
		name      string
		operation func(ctx context.Context, adapter *PostgresAdapter) error
		errMsg    string
	}{
		{
			name: "exec without connect",
			operation: func(ctx context.Context, adapter *PostgresAdapter) error {
				return adapter.Exec(ctx, "SELECT 1")
			},
			errMsg: "not established",
		},
		{
			name: "query without connect",
			operation: func(ctx context.Context, adapter *PostgresAdapter) error {
				_, err := adapter.Query(ctx, "SELECT 1")
				return err
			},
			errMsg: "not established",
		},
		{
			name: "get metadata without connect",
			operation: func(ctx context.Context, adapter *PostgresAdapter) error {
				_, err := adapter.GetTableMetadata(ctx, "users")
				return err
			},
			errMsg: "not established",
		},
		{
			name: "load csv without connect",
			operation: func(ctx context.Context, adapter *PostgresAdapter) error {
				return adapter.LoadCSV(ctx, "test", "/tmp/test.csv")
			},
			errMsg: "not established",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			adapter := NewPostgresAdapter()

			err := tt.operation(ctx, adapter)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errMsg)
		})
	}
}

func TestSanitizeIdentifier(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"name", "name"},
		{"my column", "my_column"},
		{"user", `"user"`},       // reserved word
		{"order", `"order"`},     // reserved word
		{"group", `"group"`},     // reserved word
		{"table", `"table"`},     // reserved word
		{"select", `"select"`},   // reserved word
		{"from", `"from"`},       // reserved word
		{"where", `"where"`},     // reserved word
		{"index", `"index"`},     // reserved word
		{"my-field", "my_field"}, // hyphen replaced
		{"customer_id", "customer_id"},
		{"UPPERCASE", "UPPERCASE"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := sanitizeIdentifier(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsReservedWord(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{"lowercase reserved", "user", true},
		{"uppercase reserved", "USER", true},
		{"mixed case reserved", "User", true},
		{"not reserved", "customer", false},
		{"partial match", "users", false},
		{"order", "order", true},
		{"group", "group", true},
		{"table", "table", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isReservedWord(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestNewPostgresAdapter(t *testing.T) {
	adapter := NewPostgresAdapter()
	assert.NotNil(t, adapter)
	assert.Nil(t, adapter.DB)
	assert.False(t, adapter.IsConnected())
}

func TestPostgresAdapter_Close(t *testing.T) {
	tests := []struct {
		name string
	}{
		{"close without connect"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adapter := NewPostgresAdapter()
			// Close should not error even without connection
			assert.NoError(t, adapter.Close())
		})
	}
}

// TestPostgresAdapter_Registry verifies the adapter is properly registered.
func TestPostgresAdapter_Registry(t *testing.T) {
	assert.True(t, IsRegistered("postgres"), "postgres adapter should be registered")

	factory, ok := Get("postgres")
	require.True(t, ok, "should be able to get postgres factory")

	adapter := factory()
	assert.NotNil(t, adapter)

	pg, ok := adapter.(*PostgresAdapter)
	assert.True(t, ok, "factory should return *PostgresAdapter")
	assert.NotNil(t, pg)
	assert.Equal(t, "postgres", pg.DialectName())
}

// TestPostgresAdapter_InterfaceCompliance verifies the adapter implements the interface.
func TestPostgresAdapter_InterfaceCompliance(_ *testing.T) {
	var _ Adapter = (*PostgresAdapter)(nil)
	var _ Adapter = NewPostgresAdapter()
}
