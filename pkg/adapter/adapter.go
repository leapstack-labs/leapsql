// Package adapter provides database adapter interfaces and implementations
// for LeapSQL's data transformation engine.
//
// This package contains the public contract that all database adapters must implement.
// Concrete adapter implementations are in pkg/adapters/ subdirectories.
package adapter

import (
	"context"

	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/dialect"
)

// Adapter defines the interface that all database adapters must implement.
// It provides methods for connecting to databases, executing SQL, and
// retrieving metadata.
type Adapter interface {
	// Connect establishes a connection to the database using the provided config.
	Connect(ctx context.Context, cfg core.AdapterConfig) error

	// Close closes the database connection and releases resources.
	Close() error

	// Exec executes a SQL statement that doesn't return rows (e.g., INSERT, UPDATE, CREATE).
	Exec(ctx context.Context, sql string) error

	// Query executes a SQL statement that returns rows.
	Query(ctx context.Context, sql string) (*core.Rows, error)

	// GetTableMetadata retrieves metadata for a specified table.
	GetTableMetadata(ctx context.Context, table string) (*core.TableMetadata, error)

	// LoadCSV loads data from a CSV file into a table.
	// If the table doesn't exist, it will be created with inferred schema.
	LoadCSV(ctx context.Context, tableName string, filePath string) error

	// Dialect returns the SQL dialect configuration for this adapter.
	// This is used to select the appropriate SQL dialect for lineage analysis,
	// get reserved words, format placeholders, and access dialect-specific settings.
	Dialect() *dialect.Dialect
}
