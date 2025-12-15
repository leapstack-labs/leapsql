package adapter

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"

	_ "github.com/marcboeker/go-duckdb" // duckdb driver
)

func init() {
	Register("duckdb", func() Adapter { return NewDuckDBAdapter() })
}

// DuckDBAdapter implements the Adapter interface for DuckDB.
type DuckDBAdapter struct {
	db     *sql.DB
	config Config
}

// NewDuckDBAdapter creates a new DuckDB adapter instance.
func NewDuckDBAdapter() *DuckDBAdapter {
	return &DuckDBAdapter{}
}

// Connect establishes a connection to DuckDB.
// Use ":memory:" as the path for an in-memory database.
func (a *DuckDBAdapter) Connect(ctx context.Context, cfg Config) error {
	path := cfg.Path
	if path == "" {
		path = ":memory:"
	}

	db, err := sql.Open("duckdb", path)
	if err != nil {
		return fmt.Errorf("failed to open duckdb connection: %w", err)
	}

	// Test the connection
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return fmt.Errorf("failed to ping duckdb: %w", err)
	}

	a.db = db
	a.config = cfg

	return nil
}

// Close closes the DuckDB connection.
func (a *DuckDBAdapter) Close() error {
	if a.db != nil {
		return a.db.Close()
	}
	return nil
}

// Exec executes a SQL statement that doesn't return rows.
func (a *DuckDBAdapter) Exec(ctx context.Context, sqlStr string) error {
	if a.db == nil {
		return fmt.Errorf("database connection not established")
	}

	_, err := a.db.ExecContext(ctx, sqlStr)
	if err != nil {
		return fmt.Errorf("failed to execute SQL: %w", err)
	}

	return nil
}

// Query executes a SQL statement that returns rows.
func (a *DuckDBAdapter) Query(ctx context.Context, sqlStr string) (*Rows, error) {
	if a.db == nil {
		return nil, fmt.Errorf("database connection not established")
	}

	//nolint:rowserrcheck // rows.Err() must be checked by caller after iteration completes
	rows, err := a.db.QueryContext(ctx, sqlStr)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	return &Rows{Rows: rows}, nil
}

// GetTableMetadata retrieves metadata for a specified table.
func (a *DuckDBAdapter) GetTableMetadata(ctx context.Context, table string) (*Metadata, error) {
	if a.db == nil {
		return nil, fmt.Errorf("database connection not established")
	}

	// Parse schema.table if provided
	schema := "main"
	tableName := table
	if parts := strings.Split(table, "."); len(parts) == 2 {
		schema = parts[0]
		tableName = parts[1]
	}

	// Query column information using DuckDB's information_schema
	query := `
		SELECT 
			column_name,
			data_type,
			is_nullable,
			ordinal_position
		FROM information_schema.columns 
		WHERE table_schema = ? AND table_name = ?
		ORDER BY ordinal_position
	`

	rows, err := a.db.QueryContext(ctx, query, schema, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query column metadata: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var columns []Column
	for rows.Next() {
		var col Column
		var nullable string
		if err := rows.Scan(&col.Name, &col.Type, &nullable, &col.Position); err != nil {
			return nil, fmt.Errorf("failed to scan column metadata: %w", err)
		}
		col.Nullable = nullable == "YES"
		columns = append(columns, col)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating column metadata: %w", err)
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("table %s not found", table)
	}

	// Get row count
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", schema, tableName) //nolint:gosec // Table names are validated by caller
	var rowCount int64
	if err := a.db.QueryRowContext(ctx, countQuery).Scan(&rowCount); err != nil {
		// Non-fatal error, just set to 0
		rowCount = 0
	}

	return &Metadata{
		Schema:   schema,
		Name:     tableName,
		Columns:  columns,
		RowCount: rowCount,
	}, nil
}

// LoadCSV loads data from a CSV file into a table.
// DuckDB will automatically infer the schema from the CSV file.
func (a *DuckDBAdapter) LoadCSV(ctx context.Context, tableName string, filePath string) error {
	if a.db == nil {
		return fmt.Errorf("database connection not established")
	}

	// Get absolute path for the file
	absPath, err := filepath.Abs(filePath)
	if err != nil {
		return fmt.Errorf("failed to get absolute path: %w", err)
	}

	// Use DuckDB's read_csv_auto to load the CSV with automatic schema detection
	query := fmt.Sprintf(
		"CREATE OR REPLACE TABLE %s AS SELECT * FROM read_csv_auto('%s', header=true)",
		tableName,
		absPath,
	)

	if err := a.Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to load CSV: %w", err)
	}

	return nil
}

// Ensure DuckDBAdapter implements Adapter interface
var _ Adapter = (*DuckDBAdapter)(nil)
