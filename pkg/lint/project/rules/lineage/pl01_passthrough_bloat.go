package lineage

import (
	"fmt"

	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/project"
)

func init() {
	project.Register(project.RuleDef{
		ID:          "PL01",
		Name:        "passthrough-bloat",
		Group:       "lineage",
		Description: "Model has too many passthrough columns",
		Severity:    project.SeverityWarning,
		Check:       checkPassthroughBloat,
		ConfigKeys:  []string{"threshold"},
	})
}

// checkPassthroughBloat flags models that have too many columns that are
// simple passthroughs (no transformation). This indicates a "SELECT *" style
// model that doesn't add value and increases data movement.
//
// A passthrough column is one where:
//   - TransformType is "" (direct passthrough)
//   - The column comes from a single source without any function applied
//
// Default threshold: 20 columns (configurable)
func checkPassthroughBloat(ctx *project.Context) []project.Diagnostic {
	var diagnostics []project.Diagnostic
	threshold := ctx.GetConfig().PassthroughColumnThreshold
	if threshold <= 0 {
		threshold = 20 // default
	}

	for _, model := range ctx.Models() {
		if len(model.Columns) == 0 {
			continue // No column info available
		}

		// Count passthrough columns
		passthroughCount := 0
		for _, col := range model.Columns {
			if isPassthrough(col) {
				passthroughCount++
			}
		}

		// Flag if too many passthroughs
		if passthroughCount > threshold {
			totalColumns := len(model.Columns)
			diagnostics = append(diagnostics, project.Diagnostic{
				RuleID:   "PL01",
				Severity: project.SeverityWarning,
				Message: fmt.Sprintf(
					"Model '%s' has %d/%d passthrough columns (threshold: %d); consider explicit column selection",
					model.Name, passthroughCount, totalColumns, threshold),
				Model:    model.Path,
				FilePath: model.FilePath,
			})
		}
	}

	return diagnostics
}

// isPassthrough checks if a column is a simple passthrough (no transformation).
func isPassthrough(col lint.ColumnInfo) bool {
	// A passthrough column has no transformation and comes from a single source
	if col.TransformType != "" {
		return false // Has transformation
	}
	if col.Function != "" {
		return false // Uses a function (aggregate, window, etc.)
	}
	if len(col.Sources) != 1 {
		return false // Multiple sources means some kind of transformation/combination
	}
	return true
}
