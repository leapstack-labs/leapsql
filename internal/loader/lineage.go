// Package loader provides SQL model file parsing with pragma extraction.
package loader

import "github.com/leapstack-labs/leapsql/pkg/core"

// LineageExtractor extracts table/column lineage from SQL.
// This interface allows the loader to be decoupled from the internal/lineage package.
// Implementations are wired in internal/engine.
type LineageExtractor interface {
	// Extract extracts lineage information from the given SQL.
	// Returns sources (table names), columns with their lineage, and whether SELECT * is used.
	Extract(sql string, dialect *core.Dialect) (*LineageResult, error)
}

// LineageResult holds extracted lineage information.
type LineageResult struct {
	// Sources contains all source table names (deduplicated, sorted)
	Sources []string

	// Columns contains lineage information for each output column
	Columns []core.ColumnInfo

	// UsesSelectStar is true if SELECT * or t.* is detected
	UsesSelectStar bool
}
