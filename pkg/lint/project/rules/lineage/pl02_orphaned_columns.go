package lineage

import (
	"fmt"
	"sort"
	"strings"

	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/project"
)

func init() {
	project.Register(project.RuleDef{
		ID:          "PL02",
		Name:        "orphaned-columns",
		Group:       "lineage",
		Description: "Columns not used by any downstream model",
		Severity:    lint.SeverityInfo,
		Check:       checkOrphanedColumns,
	})
}

// checkOrphanedColumns flags columns that are never used by any downstream model.
// These "orphan" columns represent unnecessary data movement and storage costs.
//
// This is a LeapSQL-exclusive rule that requires column-level lineage tracking
// across the entire project. It identifies:
//   - Columns in non-leaf models that no downstream model references
//   - Potential candidates for removal to reduce data footprint
//
// Note: Leaf models (no downstream consumers) are excluded since their columns
// are the final output.
//
// Best practice: Remove unused columns to reduce compute and storage costs.
func checkOrphanedColumns(ctx *project.Context) []project.Diagnostic {
	var diagnostics []project.Diagnostic

	// Build a set of all consumed columns across the project
	// Key: "model_path.column_name" (the source)
	consumedColumns := make(map[string]bool)

	for _, model := range ctx.Models() {
		for _, col := range model.Columns {
			for _, src := range col.Sources {
				// Mark this source column as consumed
				key := src.Table + "." + src.Column
				consumedColumns[key] = true
			}
		}
	}

	// Check each model's columns to see if they're consumed downstream
	for path, model := range ctx.Models() {
		children := ctx.GetChildren(path)

		// Skip leaf models - their columns are the final output
		if len(children) == 0 {
			continue
		}

		// Skip models with no column info
		if len(model.Columns) == 0 {
			continue
		}

		// Find orphaned columns
		var orphaned []string
		for _, col := range model.Columns {
			key := path + "." + col.Name
			if !consumedColumns[key] {
				orphaned = append(orphaned, col.Name)
			}
		}

		// Report if there are orphaned columns
		if len(orphaned) > 0 {
			sort.Strings(orphaned)

			// Limit the displayed columns to avoid very long messages
			displayColumns := orphaned
			suffix := ""
			if len(orphaned) > 5 {
				displayColumns = orphaned[:5]
				suffix = fmt.Sprintf(" and %d more", len(orphaned)-5)
			}

			diagnostics = append(diagnostics, project.Diagnostic{
				RuleID:   "PL02",
				Severity: lint.SeverityInfo,
				Message: fmt.Sprintf(
					"Model '%s' has %d columns not used by downstream models: %s%s; consider removing them",
					model.Name, len(orphaned), strings.Join(displayColumns, ", "), suffix),
				Model:            model.Path,
				FilePath:         model.FilePath,
				DocumentationURL: lint.BuildDocURL("PL02"),
				ImpactScore:      lint.ImpactLow.Int(),
				AutoFixable:      false,
			})
		}
	}

	return diagnostics
}
