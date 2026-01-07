package engine

import (
	"github.com/leapstack-labs/leapsql/internal/lineage"
	"github.com/leapstack-labs/leapsql/internal/loader"
	"github.com/leapstack-labs/leapsql/pkg/core"
)

// lineageExtractorAdapter adapts internal/lineage to loader.LineageExtractor.
// This allows the loader package to remain decoupled from the lineage package
// while still being able to extract lineage information when wired through engine.
type lineageExtractorAdapter struct{}

// NewLineageExtractor creates a new LineageExtractor that uses the internal/lineage package.
func NewLineageExtractor() loader.LineageExtractor {
	return &lineageExtractorAdapter{}
}

// Extract implements loader.LineageExtractor.
func (a *lineageExtractorAdapter) Extract(sql string, d *core.Dialect) (*loader.LineageResult, error) {
	result, err := lineage.ExtractLineageWithOptions(sql, lineage.ExtractLineageOptions{
		Dialect: d,
	})
	if err != nil {
		return nil, err
	}

	// Convert lineage.ColumnLineage to core.ColumnInfo
	columns := make([]core.ColumnInfo, 0, len(result.Columns))
	for i, col := range result.Columns {
		columns = append(columns, core.ColumnInfo{
			Name:          col.Name,
			Index:         i,
			TransformType: col.Transform,
			Function:      col.Function,
			Sources:       col.Sources,
		})
	}

	return &loader.LineageResult{
		Sources:        result.Sources,
		Columns:        columns,
		UsesSelectStar: result.UsesSelectStar,
	}, nil
}
