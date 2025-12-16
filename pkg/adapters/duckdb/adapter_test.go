package duckdb

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/leapstack-labs/leapsql/pkg/adapter"
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
			adp := New()

			dbPath := tt.setupPath(t)
			require.NoError(t, adp.Connect(ctx, adapter.Config{Path: dbPath}))
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
			adp := New()

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
			adp := New()

			if tt.connect {
				require.NoError(t, adp.Connect(ctx, adapter.Config{Path: ":memory:"}))
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
			adp := New()

			require.NoError(t, adp.Connect(ctx, adapter.Config{Path: ":memory:"}))
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
		checkFunc   func(t *testing.T, meta *adapter.Metadata)
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
			checkFunc: func(t *testing.T, meta *adapter.Metadata) {
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
			adp := New()

			require.NoError(t, adp.Connect(ctx, adapter.Config{Path: ":memory:"}))
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
	adp := New()

	require.NoError(t, adp.Connect(ctx, adapter.Config{Path: ":memory:"}))
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
