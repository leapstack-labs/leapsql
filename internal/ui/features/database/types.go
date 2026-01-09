// Package database provides database browser handlers for the UI.
package database

// ConnectionStatus represents the database connection status.
type ConnectionStatus struct {
	Connected   bool
	DialectName string
	Message     string
}

// SchemaInfo represents a database schema.
type SchemaInfo struct {
	Name string
}

// TableInfo represents a table in a schema.
type TableInfo struct {
	Name string
	Type string // "table" or "view"
}

// TableMeta represents detailed table metadata.
type TableMeta struct {
	Schema  string
	Name    string
	Columns []ColumnMeta
}

// ColumnMeta represents column metadata.
type ColumnMeta struct {
	Name     string
	Type     string
	Nullable bool
}
