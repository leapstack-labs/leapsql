// Package duckdb provides a DuckDB database adapter for LeapSQL.
package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"path/filepath"

	"github.com/leapstack-labs/leapsql/pkg/adapter"
	duckdbdialect "github.com/leapstack-labs/leapsql/pkg/adapters/duckdb/dialect"
	"github.com/leapstack-labs/leapsql/pkg/dialect"

	_ "github.com/marcboeker/go-duckdb" // duckdb driver
)

// Adapter implements the adapter.Adapter interface for DuckDB.
type Adapter struct {
	adapter.BaseSQLAdapter
}

// New creates a new DuckDB adapter instance.
// If logger is nil, a discard logger is used.
func New(logger *slog.Logger) *Adapter {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Adapter{
		BaseSQLAdapter: adapter.BaseSQLAdapter{Logger: logger},
	}
}

// Dialect returns the SQL dialect configuration for this adapter.
func (a *Adapter) Dialect() *dialect.Dialect {
	return duckdbdialect.DuckDB
}

// Connect establishes a connection to DuckDB.
// Use ":memory:" as the path for an in-memory database.
func (a *Adapter) Connect(ctx context.Context, cfg adapter.Config) error {
	path := cfg.Path
	if path == "" {
		path = ":memory:"
	}

	a.Logger.Debug("connecting to duckdb", slog.String("path", path))

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
	return a.GetTableMetadataCommon(ctx, table, a.Dialect())
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
