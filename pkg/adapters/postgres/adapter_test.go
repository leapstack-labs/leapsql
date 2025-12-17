package postgres

import (
	"context"
	"testing"

	"github.com/leapstack-labs/leapsql/pkg/adapter"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildPostgresDSN(t *testing.T) {
	tests := []struct {
		name     string
		config   adapter.Config
		expected string
	}{
		{
			name: "basic connection",
			config: adapter.Config{
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
			config: adapter.Config{
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
			config: adapter.Config{
				Database: "mydb",
			},
			expected: "host=localhost port=5432 dbname=mydb sslmode=disable",
		},
		{
			name: "custom port",
			config: adapter.Config{
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

func TestNew(t *testing.T) {
	adp := New(nil)

	assert.NotNil(t, adp, "New() should return non-nil adapter")
	assert.Nil(t, adp.DB, "DB should be nil before Connect")
	assert.False(t, adp.IsConnected(), "should not be connected initially")
	assert.Equal(t, "postgres", adp.DialectName(), "dialect name should be postgres")

	// Verify interface compliance
	var _ adapter.Adapter = (*Adapter)(nil)
	var _ adapter.Adapter = adp
}

func TestAdapter_NotConnected(t *testing.T) {
	tests := []struct {
		name      string
		operation func(ctx context.Context, adp *Adapter) error
		errMsg    string
	}{
		{
			name: "exec without connect",
			operation: func(ctx context.Context, adp *Adapter) error {
				return adp.Exec(ctx, "SELECT 1")
			},
			errMsg: "not established",
		},
		{
			name: "query without connect",
			operation: func(ctx context.Context, adp *Adapter) error {
				_, err := adp.Query(ctx, "SELECT 1")
				return err
			},
			errMsg: "not established",
		},
		{
			name: "get metadata without connect",
			operation: func(ctx context.Context, adp *Adapter) error {
				_, err := adp.GetTableMetadata(ctx, "users")
				return err
			},
			errMsg: "not established",
		},
		{
			name: "load csv without connect",
			operation: func(ctx context.Context, adp *Adapter) error {
				return adp.LoadCSV(ctx, "test", "/tmp/test.csv")
			},
			errMsg: "not established",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			adp := New(nil)

			err := tt.operation(ctx, adp)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.errMsg)
		})
	}
}

func TestAdapter_Registry(t *testing.T) {
	assert.True(t, adapter.IsRegistered("postgres"), "postgres adapter should be registered")

	factory, ok := adapter.Get("postgres")
	require.True(t, ok, "should be able to get postgres factory")

	adp := factory(nil)
	assert.NotNil(t, adp)

	pg, ok := adp.(*Adapter)
	assert.True(t, ok, "factory should return *Adapter")
	assert.NotNil(t, pg)
	assert.Equal(t, "postgres", pg.DialectName())
}

func TestAdapter_Close(t *testing.T) {
	// Close should not error even without connection
	adp := New(nil)
	assert.NoError(t, adp.Close())
}
