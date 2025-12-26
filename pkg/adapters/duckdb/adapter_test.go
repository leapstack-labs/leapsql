package duckdb

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdapter_Connect(t *testing.T) {
	tests := []struct {
		name      string
		setupPath func(t *testing.T) string
		verify    func(t *testing.T, path string)
	}{
		{
			name: "in-memory",
			setupPath: func(_ *testing.T) string {
				return ":memory:"
			},
		},
		{
			name: "file-based",
			setupPath: func(t *testing.T) string {
				tmpDir := t.TempDir()
				return filepath.Join(tmpDir, "test.duckdb")
			},
			verify: func(t *testing.T, path string) {
				_, err := os.Stat(path)
				assert.False(t, os.IsNotExist(err), "database file was not created")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			adp := New(nil)

			dbPath := tt.setupPath(t)
			require.NoError(t, adp.Connect(ctx, core.AdapterConfig{Path: dbPath}))
			defer func() { _ = adp.Close() }()

			if tt.verify != nil {
				tt.verify(t, dbPath)
			}
		})
	}
}

func TestAdapter_NotConnected(t *testing.T) {
	tests := []struct {
		name      string
		operation func(ctx context.Context, adp *Adapter) error
	}{
		{
			name: "exec without connect",
			operation: func(ctx context.Context, adp *Adapter) error {
				return adp.Exec(ctx, "SELECT 1")
			},
		},
		{
			name: "query without connect",
			operation: func(ctx context.Context, adp *Adapter) error {
				_, err := adp.Query(ctx, "SELECT 1")
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			adp := New(nil)

			err := tt.operation(ctx, adp)
			assert.Error(t, err, "expected error when operating without connection")
		})
	}
}

func TestAdapter_Close(t *testing.T) {
	tests := []struct {
		name    string
		connect bool
	}{
		{"close without connect", false},
		{"close after connect", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			adp := New(nil)

			if tt.connect {
				require.NoError(t, adp.Connect(ctx, core.AdapterConfig{Path: ":memory:"}))
			}

			assert.NoError(t, adp.Close())
		})
	}
}

func TestAdapter_QueryExecution(t *testing.T) {
	tests := []struct {
		name      string
		setup     func(t *testing.T, ctx context.Context, adp *Adapter)
		query     string
		verify    func(t *testing.T, ctx context.Context, adp *Adapter)
		expectErr bool
	}{
		{
			name: "create table and insert",
			setup: func(t *testing.T, ctx context.Context, adp *Adapter) {
				require.NoError(t, adp.Exec(ctx, `
					CREATE TABLE test_table (
						id INTEGER PRIMARY KEY,
						name VARCHAR,
						value DOUBLE
					)
				`))
			},
			verify: func(t *testing.T, ctx context.Context, adp *Adapter) {
				require.NoError(t, adp.Exec(ctx, `
					INSERT INTO test_table VALUES 
						(1, 'alice', 100.5),
						(2, 'bob', 200.75),
						(3, 'charlie', 300.25)
				`))

				rows, err := adp.Query(ctx, `SELECT COUNT(*) FROM test_table`)
				require.NoError(t, err)
				defer func() { _ = rows.Close() }()

				var count int
				require.True(t, rows.Next())
				require.NoError(t, rows.Scan(&count))
				assert.Equal(t, 3, count)
			},
		},
		{
			name: "select with join and aggregation",
			setup: func(t *testing.T, ctx context.Context, adp *Adapter) {
				require.NoError(t, adp.Exec(ctx, `
					CREATE TABLE orders (
						order_id INTEGER,
						customer_id INTEGER,
						amount DOUBLE,
						order_date DATE
					)
				`))
				require.NoError(t, adp.Exec(ctx, `
					CREATE TABLE customers (
						customer_id INTEGER,
						name VARCHAR
					)
				`))
				require.NoError(t, adp.Exec(ctx, `
					INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')
				`))
				require.NoError(t, adp.Exec(ctx, `
					INSERT INTO orders VALUES 
						(1, 1, 100.0, '2024-01-01'),
						(2, 1, 150.0, '2024-01-15'),
						(3, 2, 200.0, '2024-01-10')
				`))
			},
			verify: func(t *testing.T, ctx context.Context, adp *Adapter) {
				rows, err := adp.Query(ctx, `
					SELECT 
						c.name,
						SUM(o.amount) as total_amount,
						COUNT(*) as order_count
					FROM customers c
					JOIN orders o ON c.customer_id = o.customer_id
					GROUP BY c.name
					ORDER BY total_amount DESC
				`)
				require.NoError(t, err)
				defer func() { _ = rows.Close() }()

				results := make(map[string]float64)
				for rows.Next() {
					var name string
					var total float64
					var count int
					require.NoError(t, rows.Scan(&name, &total, &count))
					results[name] = total
				}

				assert.InEpsilon(t, 250.0, results["Alice"], 0.001)
				assert.InEpsilon(t, 200.0, results["Bob"], 0.001)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			adp := New(nil)

			require.NoError(t, adp.Connect(ctx, core.AdapterConfig{Path: ":memory:"}))
			defer func() { _ = adp.Close() }()

			if tt.setup != nil {
				tt.setup(t, ctx, adp)
			}
			if tt.verify != nil {
				tt.verify(t, ctx, adp)
			}
		})
	}
}

func TestAdapter_GetTableMetadata(t *testing.T) {
	tests := []struct {
		name        string
		setupTable  func(t *testing.T, ctx context.Context, adp *Adapter)
		tableName   string
		wantErr     bool
		wantColumns int
		wantRows    int64
		checkFunc   func(t *testing.T, meta *core.TableMetadata)
	}{
		{
			name: "existing table with data",
			setupTable: func(t *testing.T, ctx context.Context, adp *Adapter) {
				require.NoError(t, adp.Exec(ctx, `
					CREATE TABLE products (
						product_id INTEGER NOT NULL,
						name VARCHAR,
						price DOUBLE,
						in_stock BOOLEAN
					)
				`))
				require.NoError(t, adp.Exec(ctx, `
					INSERT INTO products VALUES 
						(1, 'Widget', 9.99, true),
						(2, 'Gadget', 19.99, false)
				`))
			},
			tableName:   "products",
			wantColumns: 4,
			wantRows:    2,
			checkFunc: func(t *testing.T, meta *core.TableMetadata) {
				assert.Equal(t, "products", meta.Name)
				assert.Equal(t, "main", meta.Schema)

				expectedColumns := map[string]string{
					"product_id": "INTEGER",
					"name":       "VARCHAR",
					"price":      "DOUBLE",
					"in_stock":   "BOOLEAN",
				}

				for _, col := range meta.Columns {
					expectedType, ok := expectedColumns[col.Name]
					if !ok {
						t.Errorf("unexpected column: %s", col.Name)
						continue
					}
					assert.Equal(t, expectedType, col.Type, "column %s", col.Name)
				}
			},
		},
		{
			name:      "nonexistent table",
			tableName: "nonexistent_table",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			adp := New(nil)

			require.NoError(t, adp.Connect(ctx, core.AdapterConfig{Path: ":memory:"}))
			defer func() { _ = adp.Close() }()

			if tt.setupTable != nil {
				tt.setupTable(t, ctx, adp)
			}

			metadata, err := adp.GetTableMetadata(ctx, tt.tableName)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Len(t, metadata.Columns, tt.wantColumns)
			assert.Equal(t, tt.wantRows, metadata.RowCount)

			if tt.checkFunc != nil {
				tt.checkFunc(t, metadata)
			}
		})
	}
}

func TestAdapter_LoadCSV(t *testing.T) {
	ctx := context.Background()
	adp := New(nil)

	require.NoError(t, adp.Connect(ctx, core.AdapterConfig{Path: ":memory:"}))
	defer func() { _ = adp.Close() }()

	// Create a temporary CSV file
	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "test_data.csv")

	csvContent := `id,name,value
1,alice,100.5
2,bob,200.75
3,charlie,300.25`

	require.NoError(t, os.WriteFile(csvPath, []byte(csvContent), 0600))

	// Load the CSV
	require.NoError(t, adp.LoadCSV(ctx, "test_data", csvPath))

	// Verify the data was loaded
	rows, err := adp.Query(ctx, "SELECT COUNT(*) FROM test_data")
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()

	var count int
	if rows.Next() {
		require.NoError(t, rows.Scan(&count))
	}

	assert.Equal(t, 3, count)

	// Verify metadata
	metadata, err := adp.GetTableMetadata(ctx, "test_data")
	require.NoError(t, err)
	assert.Len(t, metadata.Columns, 3)
}

func TestBuildCreateSecretSQL(t *testing.T) {
	tests := []struct {
		name string
		cfg  SecretConfig
		want string
	}{
		{
			name: "s3 with credential chain",
			cfg: SecretConfig{
				Type:     "s3",
				Provider: "credential_chain",
				Region:   "us-west-2",
			},
			want: `CREATE SECRET (
    TYPE s3,
    PROVIDER credential_chain,
    REGION 'us-west-2'
)`,
		},
		{
			name: "s3 type only",
			cfg: SecretConfig{
				Type: "s3",
			},
			want: `CREATE SECRET (
    TYPE s3
)`,
		},
		{
			name: "s3 with single scope string",
			cfg: SecretConfig{
				Type:   "s3",
				Region: "eu-central-1",
				Scope:  "s3://my-bucket",
			},
			want: `CREATE SECRET (
    TYPE s3,
    REGION 'eu-central-1',
    SCOPE 's3://my-bucket'
)`,
		},
		{
			name: "s3 with multiple scopes as []any",
			cfg: SecretConfig{
				Type:   "s3",
				Region: "eu-central-1",
				Scope:  []any{"s3://bucket1", "s3://bucket2"},
			},
			want: `CREATE SECRET (
    TYPE s3,
    REGION 'eu-central-1',
    SCOPE ('s3://bucket1', 's3://bucket2')
)`,
		},
		{
			name: "s3 with multiple scopes as []string",
			cfg: SecretConfig{
				Type:   "s3",
				Region: "eu-central-1",
				Scope:  []string{"s3://bucket1", "s3://bucket2"},
			},
			want: `CREATE SECRET (
    TYPE s3,
    REGION 'eu-central-1',
    SCOPE ('s3://bucket1', 's3://bucket2')
)`,
		},
		{
			name: "s3 with explicit credentials",
			cfg: SecretConfig{
				Type:     "s3",
				Provider: "config",
				Region:   "us-east-1",
				KeyID:    "AKIAIOSFODNN7EXAMPLE",
				Secret:   "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
			},
			want: `CREATE SECRET (
    TYPE s3,
    PROVIDER config,
    REGION 'us-east-1',
    KEY_ID 'AKIAIOSFODNN7EXAMPLE',
    SECRET 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
)`,
		},
		{
			name: "s3 compatible with endpoint and path style",
			cfg: SecretConfig{
				Type:     "s3",
				Provider: "config",
				KeyID:    "minioadmin",
				Secret:   "minioadmin",
				Endpoint: "localhost:9000",
				URLStyle: "path",
				UseSSL:   boolPtrTest(false),
			},
			want: `CREATE SECRET (
    TYPE s3,
    PROVIDER config,
    KEY_ID 'minioadmin',
    SECRET 'minioadmin',
    ENDPOINT 'localhost:9000',
    URL_STYLE 'path',
    USE_SSL false
)`,
		},
		{
			name: "gcs with service account",
			cfg: SecretConfig{
				Type:     "gcs",
				Provider: "service_account",
				KeyID:    "my-service-account@project.iam.gserviceaccount.com",
			},
			want: `CREATE SECRET (
    TYPE gcs,
    PROVIDER service_account,
    KEY_ID 'my-service-account@project.iam.gserviceaccount.com'
)`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildCreateSecretSQL(tt.cfg)
			assert.Equal(t, tt.want, got)
		})
	}
}

func boolPtrTest(b bool) *bool {
	return &b
}

func TestConnect_WithParams(t *testing.T) {
	ctx := context.Background()
	adp := New(nil)

	cfg := core.AdapterConfig{
		Path: ":memory:",
		Params: map[string]any{
			"extensions": []any{"json"},
			"settings": map[string]any{
				"threads": "2",
			},
		},
	}

	err := adp.Connect(ctx, cfg)
	require.NoError(t, err)
	defer func() { _ = adp.Close() }()

	// Verify extension loaded by checking it's in the loaded extensions list
	rows, err := adp.Query(ctx, "SELECT extension_name FROM duckdb_extensions() WHERE loaded = true AND extension_name = 'json'")
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	require.True(t, rows.Next(), "json extension should be loaded")

	var extName string
	require.NoError(t, rows.Scan(&extName))
	assert.Equal(t, "json", extName)
}

func TestConnect_WithSettings(t *testing.T) {
	ctx := context.Background()
	adp := New(nil)

	cfg := core.AdapterConfig{
		Path: ":memory:",
		Params: map[string]any{
			"settings": map[string]any{
				"threads": "2",
			},
		},
	}

	err := adp.Connect(ctx, cfg)
	require.NoError(t, err)
	defer func() { _ = adp.Close() }()

	// Verify setting was applied
	rows, err := adp.Query(ctx, "SELECT current_setting('threads')")
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
	require.True(t, rows.Next())

	var threadsSetting string
	require.NoError(t, rows.Scan(&threadsSetting))
	assert.Equal(t, "2", threadsSetting)
}

func TestConnect_WithNilParams(t *testing.T) {
	ctx := context.Background()
	adp := New(nil)

	cfg := core.AdapterConfig{
		Path:   ":memory:",
		Params: nil,
	}

	err := adp.Connect(ctx, cfg)
	require.NoError(t, err)
	defer func() { _ = adp.Close() }()

	// Should work normally
	rows, err := adp.Query(ctx, "SELECT 1")
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
}

func TestConnect_WithEmptyParams(t *testing.T) {
	ctx := context.Background()
	adp := New(nil)

	cfg := core.AdapterConfig{
		Path:   ":memory:",
		Params: map[string]any{},
	}

	err := adp.Connect(ctx, cfg)
	require.NoError(t, err)
	defer func() { _ = adp.Close() }()

	// Should work normally
	rows, err := adp.Query(ctx, "SELECT 1")
	require.NoError(t, err)
	defer func() { _ = rows.Close() }()
}
