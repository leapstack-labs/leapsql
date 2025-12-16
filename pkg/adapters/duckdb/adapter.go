// Package duckdb provides a DuckDB database adapter for LeapSQL.
package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/leapstack-labs/leapsql/pkg/adapter"

	_ "github.com/marcboeker/go-duckdb" // duckdb driver
)

// Adapter implements the adapter.Adapter interface for DuckDB.
type Adapter struct {
	adapter.BaseSQLAdapter
}

// New creates a new DuckDB adapter instance.
func New() *Adapter {
	return &Adapter{}
}

// DialectName returns the SQL dialect for this adapter.
func (a *Adapter) DialectName() string {
	return "duckdb"
}

// Connect establishes a connection to DuckDB.
// Use ":memory:" as the path for an in-memory database.
func (a *Adapter) Connect(ctx context.Context, cfg adapter.Config) error {
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

	a.DB = db
	a.Cfg = cfg

	return nil
}

// GetTableMetadata retrieves metadata for a specified table.
func (a *Adapter) GetTableMetadata(ctx context.Context, table string) (*adapter.Metadata, error) {
	if a.DB == nil {
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

	rows, err := a.DB.QueryContext(ctx, query, schema, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query column metadata: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var columns []adapter.Column
	for rows.Next() {
		var col adapter.Column
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
	if err := a.DB.QueryRowContext(ctx, countQuery).Scan(&rowCount); err != nil {
		// Non-fatal error, just set to 0
		rowCount = 0
	}

	return &adapter.Metadata{
		Schema:   schema,
		Name:     tableName,
		Columns:  columns,
		RowCount: rowCount,
	}, nil
}

// LoadCSV loads data from a CSV file into a table.
// DuckDB will automatically infer the schema from the CSV file.
func (a *Adapter) LoadCSV(ctx context.Context, tableName string, filePath string) error {
	if a.DB == nil {
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

// Ensure Adapter implements adapter.Adapter interface
var _ adapter.Adapter = (*Adapter)(nil)
