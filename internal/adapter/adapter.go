// Package adapter provides database adapter interfaces and implementations
// for DBGo's data transformation engine.
package adapter

import (
	"context"
	"database/sql"
)

// Config holds the configuration for connecting to a database.
type Config struct {
	// Type specifies the database type (e.g., "duckdb", "postgres")
	Type string

	// Path is the file path for file-based databases (e.g., DuckDB, SQLite)
	// Use ":memory:" for in-memory databases
	Path string

	// Host is the hostname for network-based databases
	Host string

	// Port is the port number for network-based databases
	Port int

	// Database is the database name
	Database string

	// Username for authentication
	Username string

	// Password for authentication
	Password string

	// Schema is the default schema to use
	Schema string

	// Options contains additional driver-specific options
	Options map[string]string
}

// Column represents a column in a database table.
type Column struct {
	// Name is the column name
	Name string

	// Type is the data type of the column
	Type string

	// Nullable indicates whether the column allows NULL values
	Nullable bool

	// PrimaryKey indicates whether the column is part of the primary key
	PrimaryKey bool

	// Position is the ordinal position of the column in the table
	Position int
}

// Metadata holds metadata about a database table.
type Metadata struct {
	// Schema is the schema containing the table
	Schema string

	// Name is the table name
	Name string

	// Columns contains metadata for each column
	Columns []Column

	// RowCount is the approximate number of rows (may not be exact)
	RowCount int64

	// SizeBytes is the approximate size of the table in bytes
	SizeBytes int64
}

// Rows wraps sql.Rows to provide a consistent interface across adapters.
type Rows struct {
	*sql.Rows
}

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

	// DialectName returns the SQL dialect name for this adapter (e.g., "duckdb", "postgres").
	// This is used to automatically select the appropriate SQL dialect for lineage analysis.
	DialectName() string
}
