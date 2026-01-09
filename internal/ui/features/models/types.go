// Package models provides model detail handlers for the UI.
package models

// ModelViewData holds all data for the model view including all tab content.
type ModelViewData struct {
	// Model info
	Path         string
	Name         string
	FilePath     string
	Materialized string
	Schema       string
	Description  string
	Owner        string
	Tags         []string

	// Tab content (all pre-rendered)
	SourceSQL    string
	CompiledSQL  string
	CompileError string // If compilation failed

	// Preview data (may be nil if not available)
	Preview      *PreviewData
	PreviewError string // If preview query failed
}

// PreviewData holds data preview results.
type PreviewData struct {
	Columns  []string
	Rows     [][]string
	RowCount int
	Limited  bool
}

// ModelContext holds data for the context panel.
type ModelContext struct {
	Path       string
	Name       string
	Type       string
	Schema     string
	DependsOn  []string
	Dependents []string
	Columns    []ColumnData
}

// ColumnData holds column information.
type ColumnData struct {
	Name    string
	Type    string
	Sources []string
}
