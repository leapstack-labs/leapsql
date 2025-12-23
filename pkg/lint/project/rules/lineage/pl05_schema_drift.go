package lineage

import (
	"fmt"
	"strings"

	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/project"
)

func init() {
	project.Register(project.RuleDef{
		ID:          "PL05",
		Name:        "schema-drift",
		Group:       "lineage",
		Description: "SELECT * from source with changed schema since last run",
		Severity:    lint.SeverityWarning,
		Check:       checkSchemaDrift,
	})
}

// checkSchemaDrift detects when source tables used in SELECT * have changed
// columns since the last successful run. This helps catch breaking changes
// in upstream tables before they cause data issues.
//
// The rule compares the current columns of a source table against a snapshot
// taken after the last successful run. If columns have been added or removed,
// a warning is raised.
func checkSchemaDrift(ctx *project.Context) []project.Diagnostic {
	var diagnostics []project.Diagnostic

	// This rule requires access to the store for snapshots
	// In a full implementation, the Context would provide access to the store
	// For now, we skip this check if snapshot data isn't available

	for _, model := range ctx.Models() {
		// Skip models that don't use SELECT *
		// This would be detected during SQL parsing and stored in model metadata
		if !usesSelectStar(model) {
			continue
		}

		// For each source (parent model or external table)
		for _, source := range model.Sources {
			// Check if we have a baseline snapshot
			// In a full implementation, this would query the snapshot store
			snapshotCols := getSnapshotColumns(ctx, model.Path, source)
			if len(snapshotCols) == 0 {
				continue // No baseline yet
			}

			// Get current columns from source
			currentCols := getCurrentColumns(ctx, source)
			if len(currentCols) == 0 {
				continue // Source not found or no columns
			}

			// Compare
			added, removed := diffColumns(snapshotCols, currentCols)

			if len(added) > 0 || len(removed) > 0 {
				msg := buildDriftMessage(source, added, removed)

				diagnostics = append(diagnostics, project.Diagnostic{
					RuleID:           "PL05",
					Severity:         lint.SeverityWarning,
					Message:          msg,
					Model:            model.Path,
					FilePath:         model.FilePath,
					DocumentationURL: lint.BuildDocURL("PL05"),
					ImpactScore:      lint.ImpactHigh.Int(),
					AutoFixable:      false,
				})
			}
		}
	}

	return diagnostics
}

// usesSelectStar checks if a model uses SELECT * anywhere.
// In a full implementation, this would be stored in model metadata during parsing.
func usesSelectStar(model *project.ModelInfo) bool {
	// For now, we check if the model has columns but they're all passthrough
	// A more accurate check would be done during SQL parsing
	if len(model.Columns) == 0 {
		return false
	}

	// Check if all columns are direct passthroughs (no transformation)
	passthroughCount := 0
	for _, col := range model.Columns {
		if col.TransformType == "" && col.Function == "" {
			passthroughCount++
		}
	}

	// If most columns are passthrough, likely SELECT *
	return passthroughCount > len(model.Columns)/2
}

// getSnapshotColumns retrieves the snapshot columns for a model/source combination.
// Returns nil if no snapshot exists or if the store is not available.
func getSnapshotColumns(ctx *project.Context, modelPath, source string) []string {
	store := ctx.Store()
	if store == nil {
		return nil // No store available, skip schema drift detection
	}

	columns, _, err := store.GetColumnSnapshot(modelPath, source)
	if err != nil {
		return nil // Error getting snapshot, skip silently
	}

	return columns
}

// getCurrentColumns gets the current columns for a source table.
func getCurrentColumns(ctx *project.Context, source string) []string {
	// Check if source is a known model
	if model, ok := ctx.GetModel(source); ok {
		cols := make([]string, len(model.Columns))
		for i, c := range model.Columns {
			cols[i] = c.Name
		}
		return cols
	}

	// Source is an external table - would need adapter to query schema
	return nil
}

// diffColumns compares old and current column lists.
func diffColumns(old, current []string) (added, removed []string) {
	oldSet := make(map[string]bool)
	for _, c := range old {
		oldSet[c] = true
	}

	currentSet := make(map[string]bool)
	for _, c := range current {
		currentSet[c] = true
		if !oldSet[c] {
			added = append(added, c)
		}
	}

	for _, c := range old {
		if !currentSet[c] {
			removed = append(removed, c)
		}
	}

	return added, removed
}

// buildDriftMessage creates a human-readable message about schema drift.
func buildDriftMessage(source string, added, removed []string) string {
	var parts []string

	parts = append(parts, fmt.Sprintf("Source '%s' schema changed", source))

	if len(added) > 0 {
		if len(added) <= 3 {
			parts = append(parts, fmt.Sprintf("added: %s", strings.Join(added, ", ")))
		} else {
			parts = append(parts, fmt.Sprintf("added %d columns", len(added)))
		}
	}

	if len(removed) > 0 {
		if len(removed) <= 3 {
			parts = append(parts, fmt.Sprintf("removed: %s", strings.Join(removed, ", ")))
		} else {
			parts = append(parts, fmt.Sprintf("removed %d columns", len(removed)))
		}
	}

	return strings.Join(parts, "; ")
}
