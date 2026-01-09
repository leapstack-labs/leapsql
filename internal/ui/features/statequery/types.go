// Package statequery provides handlers for querying the state database.
package statequery

// QueryViewData holds data for the state query page.
type QueryViewData struct {
	Tables []TableItem
	Views  []TableItem
}

// TableItem represents a table or view in the sidebar.
type TableItem struct {
	Name string
	Type string // "table" or "view"
}

// QueryResult represents execution results.
type QueryResult struct {
	Columns   []string
	Rows      [][]string
	RowCount  int
	Truncated bool
	QueryMS   int64
	Error     string
}

// SchemaData for table schema display.
type SchemaData struct {
	Name    string
	Type    string
	Columns []ColumnSchema
}

// ColumnSchema represents a column in a table schema.
type ColumnSchema struct {
	Name     string
	Type     string
	Nullable bool
	Default  string
	IsPK     bool
}

// SearchResultItem represents a search result.
type SearchResultItem struct {
	Path        string
	Name        string
	Description string
}
