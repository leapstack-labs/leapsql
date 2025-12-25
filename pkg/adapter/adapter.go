// Package adapter provides database adapter interfaces and implementations
// for LeapSQL's data transformation engine.
//
// This package contains the public contract that all database adapters must implement.
// Concrete adapter implementations are in pkg/adapters/ subdirectories.
//
// Note: Core types (Config, Column, Metadata, Rows) are now defined in pkg/core.
// This package re-exports them via type aliases for backward compatibility.
// New code should import pkg/core directly.
package adapter

import (
	"context"

	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/dialect"
)

// Type aliases for backward compatibility - these types are now defined in pkg/core.
// Use core.* types directly in new code.
type (
	// Config is an alias for core.AdapterConfig.
	Config = core.AdapterConfig

	// Column is an alias for core.Column.
	Column = core.Column

	// Metadata is an alias for core.TableMetadata.
	Metadata = core.TableMetadata

	// Rows is an alias for core.Rows.
	Rows = core.Rows
)

// Adapter defines the interface that all database adapters must implement.
// It provides methods for connecting to databases, executing SQL, and
// retrieving metadata.
type Adapter interface {
	// Connect establishes a connection to the database using the provided config.
	Connect(ctx context.Context, cfg Config) error

	// Close closes the database connection and releases resources.
	Close() error

	// Exec executes a SQL statement that doesn't return rows (e.g., INSERT, UPDATE, CREATE).
	Exec(ctx context.Context, sql string) error

	// Query executes a SQL statement that returns rows.
	Query(ctx context.Context, sql string) (*Rows, error)

	// GetTableMetadata retrieves metadata for a specified table.
	GetTableMetadata(ctx context.Context, table string) (*Metadata, error)

	// LoadCSV loads data from a CSV file into a table.
	// If the table doesn't exist, it will be created with inferred schema.
	LoadCSV(ctx context.Context, tableName string, filePath string) error

	// Dialect returns the SQL dialect configuration for this adapter.
	// This is used to select the appropriate SQL dialect for lineage analysis,
	// get reserved words, format placeholders, and access dialect-specific settings.
	Dialect() *dialect.Dialect
}
