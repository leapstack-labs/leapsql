package core

import (
	"context"
	"database/sql"
)

// Adapter defines the interface that all database adapters must implement.
type Adapter interface {
	// Connect establishes a connection to the database.
	Connect(ctx context.Context, cfg AdapterConfig) error

	// Close closes the database connection.
	Close() error

	// Exec executes a SQL statement that doesn't return rows.
	Exec(ctx context.Context, sql string) error

	// Query executes a SQL statement that returns rows.
	Query(ctx context.Context, sql string) (*Rows, error)

	// GetTableMetadata retrieves metadata for a table.
	GetTableMetadata(ctx context.Context, table string) (*TableMetadata, error)

	// LoadCSV loads data from a CSV file into a table.
	LoadCSV(ctx context.Context, tableName, filePath string) error

	// DialectConfig returns the static dialect configuration.
	DialectConfig() *DialectConfig
}

// AdapterConfig holds configuration for connecting to a database.
type AdapterConfig struct {
	Type     string
	Path     string
	Host     string
	Port     int
	Database string
	Username string
	Password string
	Schema   string
	Options  map[string]string
	Params   map[string]any
}

// Column represents a column in a database table.
type Column struct {
	Name       string
	Type       string
	Nullable   bool
	PrimaryKey bool
	Position   int
}

// TableMetadata holds metadata about a database table.
type TableMetadata struct {
	Schema    string
	Name      string
	Columns   []Column
	RowCount  int64
	SizeBytes int64
}

// Rows wraps sql.Rows to provide a consistent interface.
type Rows struct {
	*sql.Rows
}
