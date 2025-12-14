package adapter

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDuckDBAdapter_Connect(t *testing.T) {
	tests := []struct {
		name      string
		setupPath func(t *testing.T) string
		verify    func(t *testing.T, path string)
	}{
		{
			name: "in-memory",
			setupPath: func(t *testing.T) string {
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
			adapter := NewDuckDBAdapter()

			dbPath := tt.setupPath(t)
			require.NoError(t, adapter.Connect(ctx, Config{Path: dbPath}))
			defer adapter.Close()

			if tt.verify != nil {
				tt.verify(t, dbPath)
			}
		})
	}
}

func TestDuckDBAdapter_NotConnected(t *testing.T) {
	tests := []struct {
		name      string
		operation func(ctx context.Context, adapter *DuckDBAdapter) error
	}{
		{
			name: "exec without connect",
			operation: func(ctx context.Context, adapter *DuckDBAdapter) error {
				return adapter.Exec(ctx, "SELECT 1")
			},
		},
		{
			name: "query without connect",
			operation: func(ctx context.Context, adapter *DuckDBAdapter) error {
				_, err := adapter.Query(ctx, "SELECT 1")
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			adapter := NewDuckDBAdapter()

			err := tt.operation(ctx, adapter)
			assert.Error(t, err, "expected error when operating without connection")
		})
	}
}

func TestDuckDBAdapter_Exec(t *testing.T) {
	ctx := context.Background()
	adapter := NewDuckDBAdapter()

	require.NoError(t, adapter.Connect(ctx, Config{Path: ":memory:"}))
	defer adapter.Close()

	// Create a table
	err := adapter.Exec(ctx, `
		CREATE TABLE test_table (
			id INTEGER PRIMARY KEY,
			name VARCHAR,
			value DOUBLE
		)
	`)
	require.NoError(t, err)

	// Insert data
	err = adapter.Exec(ctx, `
		INSERT INTO test_table VALUES 
			(1, 'alice', 100.5),
			(2, 'bob', 200.75),
			(3, 'charlie', 300.25)
	`)
	require.NoError(t, err)
}

func TestDuckDBAdapter_Query(t *testing.T) {
	ctx := context.Background()
	adapter := NewDuckDBAdapter()

	require.NoError(t, adapter.Connect(ctx, Config{Path: ":memory:"}))
	defer adapter.Close()

	// Create and populate a table
	require.NoError(t, adapter.Exec(ctx, `CREATE TABLE users (id INTEGER, name VARCHAR)`))
	require.NoError(t, adapter.Exec(ctx, `INSERT INTO users VALUES (1, 'alice'), (2, 'bob')`))

	// Query the data
	rows, err := adapter.Query(ctx, `SELECT id, name FROM users ORDER BY id`)
	require.NoError(t, err)
	defer rows.Close()

	expected := []struct {
		id   int
		name string
	}{
		{1, "alice"},
		{2, "bob"},
	}

	i := 0
	for rows.Next() {
		var id int
		var name string
		require.NoError(t, rows.Scan(&id, &name))

		require.Less(t, i, len(expected), "unexpected extra row: id=%d, name=%s", id, name)
		assert.Equal(t, expected[i].id, id)
		assert.Equal(t, expected[i].name, name)
		i++
	}

	assert.Equal(t, len(expected), i)
}

func TestDuckDBAdapter_GetTableMetadata(t *testing.T) {
	tests := []struct {
		name        string
		setupTable  func(t *testing.T, adapter *DuckDBAdapter, ctx context.Context)
		tableName   string
		wantErr     bool
		wantColumns int
		wantRows    int64
		checkFunc   func(t *testing.T, meta *Metadata)
	}{
		{
			name: "existing table with data",
			setupTable: func(t *testing.T, adapter *DuckDBAdapter, ctx context.Context) {
				require.NoError(t, adapter.Exec(ctx, `
					CREATE TABLE products (
						product_id INTEGER NOT NULL,
						name VARCHAR,
						price DOUBLE,
						in_stock BOOLEAN
					)
				`))
				require.NoError(t, adapter.Exec(ctx, `
					INSERT INTO products VALUES 
						(1, 'Widget', 9.99, true),
						(2, 'Gadget', 19.99, false)
				`))
			},
			tableName:   "products",
			wantColumns: 4,
			wantRows:    2,
			checkFunc: func(t *testing.T, meta *Metadata) {
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
			adapter := NewDuckDBAdapter()

			require.NoError(t, adapter.Connect(ctx, Config{Path: ":memory:"}))
			defer adapter.Close()

			if tt.setupTable != nil {
				tt.setupTable(t, adapter, ctx)
			}

			metadata, err := adapter.GetTableMetadata(ctx, tt.tableName)

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

func TestDuckDBAdapter_LoadCSV(t *testing.T) {
	ctx := context.Background()
	adapter := NewDuckDBAdapter()

	require.NoError(t, adapter.Connect(ctx, Config{Path: ":memory:"}))
	defer adapter.Close()

	// Create a temporary CSV file
	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "test_data.csv")

	csvContent := `id,name,value
1,alice,100.5
2,bob,200.75
3,charlie,300.25`

	require.NoError(t, os.WriteFile(csvPath, []byte(csvContent), 0644))

	// Load the CSV
	require.NoError(t, adapter.LoadCSV(ctx, "test_data", csvPath))

	// Verify the data was loaded
	rows, err := adapter.Query(ctx, "SELECT COUNT(*) FROM test_data")
	require.NoError(t, err)
	defer rows.Close()

	var count int
	if rows.Next() {
		require.NoError(t, rows.Scan(&count))
	}

	assert.Equal(t, 3, count)

	// Verify metadata
	metadata, err := adapter.GetTableMetadata(ctx, "test_data")
	require.NoError(t, err)
	assert.Len(t, metadata.Columns, 3)
}

func TestDuckDBAdapter_Close(t *testing.T) {
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
			adapter := NewDuckDBAdapter()

			if tt.connect {
				require.NoError(t, adapter.Connect(ctx, Config{Path: ":memory:"}))
			}

			assert.NoError(t, adapter.Close())
		})
	}
}

func TestDuckDBAdapter_ComplexQuery(t *testing.T) {
	ctx := context.Background()
	adapter := NewDuckDBAdapter()

	require.NoError(t, adapter.Connect(ctx, Config{Path: ":memory:"}))
	defer adapter.Close()

	// Create tables
	require.NoError(t, adapter.Exec(ctx, `
		CREATE TABLE orders (
			order_id INTEGER,
			customer_id INTEGER,
			amount DOUBLE,
			order_date DATE
		)
	`))

	require.NoError(t, adapter.Exec(ctx, `
		CREATE TABLE customers (
			customer_id INTEGER,
			name VARCHAR
		)
	`))

	// Insert data
	require.NoError(t, adapter.Exec(ctx, `
		INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')
	`))

	require.NoError(t, adapter.Exec(ctx, `
		INSERT INTO orders VALUES 
			(1, 1, 100.0, '2024-01-01'),
			(2, 1, 150.0, '2024-01-15'),
			(3, 2, 200.0, '2024-01-10')
	`))

	// Run a complex query with JOIN and aggregation
	rows, err := adapter.Query(ctx, `
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
	defer rows.Close()

	results := make(map[string]float64)
	for rows.Next() {
		var name string
		var total float64
		var count int
		require.NoError(t, rows.Scan(&name, &total, &count))
		results[name] = total
	}

	assert.Equal(t, 250.0, results["Alice"])
	assert.Equal(t, 200.0, results["Bob"])
}
