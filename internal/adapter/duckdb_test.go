package adapter

import (
	"context"
	"os"
	"path/filepath"
	"testing"
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
				if _, err := os.Stat(path); os.IsNotExist(err) {
					t.Error("database file was not created")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			adapter := NewDuckDBAdapter()

			dbPath := tt.setupPath(t)
			err := adapter.Connect(ctx, Config{Path: dbPath})
			if err != nil {
				t.Fatalf("failed to connect: %v", err)
			}
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
			if err == nil {
				t.Error("expected error when operating without connection, got nil")
			}
		})
	}
}

func TestDuckDBAdapter_Exec(t *testing.T) {
	ctx := context.Background()
	adapter := NewDuckDBAdapter()

	if err := adapter.Connect(ctx, Config{Path: ":memory:"}); err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer adapter.Close()

	// Create a table
	err := adapter.Exec(ctx, `
		CREATE TABLE test_table (
			id INTEGER PRIMARY KEY,
			name VARCHAR,
			value DOUBLE
		)
	`)
	if err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	// Insert data
	err = adapter.Exec(ctx, `
		INSERT INTO test_table VALUES 
			(1, 'alice', 100.5),
			(2, 'bob', 200.75),
			(3, 'charlie', 300.25)
	`)
	if err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}
}

func TestDuckDBAdapter_Query(t *testing.T) {
	ctx := context.Background()
	adapter := NewDuckDBAdapter()

	if err := adapter.Connect(ctx, Config{Path: ":memory:"}); err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer adapter.Close()

	// Create and populate a table
	if err := adapter.Exec(ctx, `CREATE TABLE users (id INTEGER, name VARCHAR)`); err != nil {
		t.Fatalf("failed to create table: %v", err)
	}

	if err := adapter.Exec(ctx, `INSERT INTO users VALUES (1, 'alice'), (2, 'bob')`); err != nil {
		t.Fatalf("failed to insert data: %v", err)
	}

	// Query the data
	rows, err := adapter.Query(ctx, `SELECT id, name FROM users ORDER BY id`)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
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
		if err := rows.Scan(&id, &name); err != nil {
			t.Fatalf("failed to scan row: %v", err)
		}

		if i >= len(expected) {
			t.Fatalf("unexpected extra row: id=%d, name=%s", id, name)
		}

		if id != expected[i].id || name != expected[i].name {
			t.Errorf("row %d: got (%d, %s), want (%d, %s)",
				i, id, name, expected[i].id, expected[i].name)
		}
		i++
	}

	if i != len(expected) {
		t.Errorf("got %d rows, want %d", i, len(expected))
	}
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
				if err := adapter.Exec(ctx, `
					CREATE TABLE products (
						product_id INTEGER NOT NULL,
						name VARCHAR,
						price DOUBLE,
						in_stock BOOLEAN
					)
				`); err != nil {
					t.Fatalf("failed to create table: %v", err)
				}
				if err := adapter.Exec(ctx, `
					INSERT INTO products VALUES 
						(1, 'Widget', 9.99, true),
						(2, 'Gadget', 19.99, false)
				`); err != nil {
					t.Fatalf("failed to insert data: %v", err)
				}
			},
			tableName:   "products",
			wantColumns: 4,
			wantRows:    2,
			checkFunc: func(t *testing.T, meta *Metadata) {
				if meta.Name != "products" {
					t.Errorf("got table name %q, want %q", meta.Name, "products")
				}
				if meta.Schema != "main" {
					t.Errorf("got schema %q, want %q", meta.Schema, "main")
				}

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
					if col.Type != expectedType {
						t.Errorf("column %s: got type %q, want %q", col.Name, col.Type, expectedType)
					}
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

			if err := adapter.Connect(ctx, Config{Path: ":memory:"}); err != nil {
				t.Fatalf("failed to connect: %v", err)
			}
			defer adapter.Close()

			if tt.setupTable != nil {
				tt.setupTable(t, adapter, ctx)
			}

			metadata, err := adapter.GetTableMetadata(ctx, tt.tableName)

			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if len(metadata.Columns) != tt.wantColumns {
				t.Errorf("got %d columns, want %d", len(metadata.Columns), tt.wantColumns)
			}

			if metadata.RowCount != tt.wantRows {
				t.Errorf("got row count %d, want %d", metadata.RowCount, tt.wantRows)
			}

			if tt.checkFunc != nil {
				tt.checkFunc(t, metadata)
			}
		})
	}
}

func TestDuckDBAdapter_LoadCSV(t *testing.T) {
	ctx := context.Background()
	adapter := NewDuckDBAdapter()

	if err := adapter.Connect(ctx, Config{Path: ":memory:"}); err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer adapter.Close()

	// Create a temporary CSV file
	tmpDir := t.TempDir()
	csvPath := filepath.Join(tmpDir, "test_data.csv")

	csvContent := `id,name,value
1,alice,100.5
2,bob,200.75
3,charlie,300.25`

	if err := os.WriteFile(csvPath, []byte(csvContent), 0644); err != nil {
		t.Fatalf("failed to write CSV file: %v", err)
	}

	// Load the CSV
	if err := adapter.LoadCSV(ctx, "test_data", csvPath); err != nil {
		t.Fatalf("failed to load CSV: %v", err)
	}

	// Verify the data was loaded
	rows, err := adapter.Query(ctx, "SELECT COUNT(*) FROM test_data")
	if err != nil {
		t.Fatalf("failed to query loaded data: %v", err)
	}
	defer rows.Close()

	var count int
	if rows.Next() {
		if err := rows.Scan(&count); err != nil {
			t.Fatalf("failed to scan count: %v", err)
		}
	}

	if count != 3 {
		t.Errorf("got %d rows, want 3", count)
	}

	// Verify metadata
	metadata, err := adapter.GetTableMetadata(ctx, "test_data")
	if err != nil {
		t.Fatalf("failed to get metadata: %v", err)
	}

	if len(metadata.Columns) != 3 {
		t.Errorf("got %d columns, want 3", len(metadata.Columns))
	}
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
				if err := adapter.Connect(ctx, Config{Path: ":memory:"}); err != nil {
					t.Fatalf("failed to connect: %v", err)
				}
			}

			if err := adapter.Close(); err != nil {
				t.Errorf("close should not error: %v", err)
			}
		})
	}
}

func TestDuckDBAdapter_ComplexQuery(t *testing.T) {
	ctx := context.Background()
	adapter := NewDuckDBAdapter()

	if err := adapter.Connect(ctx, Config{Path: ":memory:"}); err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer adapter.Close()

	// Create tables
	if err := adapter.Exec(ctx, `
		CREATE TABLE orders (
			order_id INTEGER,
			customer_id INTEGER,
			amount DOUBLE,
			order_date DATE
		)
	`); err != nil {
		t.Fatalf("failed to create orders table: %v", err)
	}

	if err := adapter.Exec(ctx, `
		CREATE TABLE customers (
			customer_id INTEGER,
			name VARCHAR
		)
	`); err != nil {
		t.Fatalf("failed to create customers table: %v", err)
	}

	// Insert data
	if err := adapter.Exec(ctx, `
		INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')
	`); err != nil {
		t.Fatalf("failed to insert customers: %v", err)
	}

	if err := adapter.Exec(ctx, `
		INSERT INTO orders VALUES 
			(1, 1, 100.0, '2024-01-01'),
			(2, 1, 150.0, '2024-01-15'),
			(3, 2, 200.0, '2024-01-10')
	`); err != nil {
		t.Fatalf("failed to insert orders: %v", err)
	}

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
	if err != nil {
		t.Fatalf("failed to run complex query: %v", err)
	}
	defer rows.Close()

	results := make(map[string]float64)
	for rows.Next() {
		var name string
		var total float64
		var count int
		if err := rows.Scan(&name, &total, &count); err != nil {
			t.Fatalf("failed to scan row: %v", err)
		}
		results[name] = total
	}

	if results["Alice"] != 250.0 {
		t.Errorf("Alice total: got %.2f, want 250.00", results["Alice"])
	}

	if results["Bob"] != 200.0 {
		t.Errorf("Bob total: got %.2f, want 200.00", results["Bob"])
	}
}
