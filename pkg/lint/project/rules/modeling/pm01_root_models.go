package modeling

import (
	"fmt"

	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/project"
)

func init() {
	project.Register(project.RuleDef{
		ID:          "PM01",
		Name:        "root-models",
		Group:       "modeling",
		Description: "Models with no sources (broken DAG lineage)",
		Severity:    project.SeverityWarning,
		Check:       checkRootModels,
	})
}

// checkRootModels flags models that have no sources (upstream dependencies).
// These are "root" models that don't reference any tables, which usually
// indicates a broken lineage or a model that should be a seed.
//
// Staging models are expected to reference external sources (seeds/raw tables),
// so only non-staging models without sources are flagged.
func checkRootModels(ctx *project.Context) []project.Diagnostic {
	var diagnostics []project.Diagnostic

	for _, model := range ctx.Models() {
		// Skip staging models - they're expected to reference external sources
		// which may not be in our model list
		if model.Type == lint.ModelTypeStaging {
			continue
		}

		// Check if model has no sources
		if len(model.Sources) == 0 {
			diagnostics = append(diagnostics, project.Diagnostic{
				RuleID:   "PM01",
				Severity: project.SeverityWarning,
				Message:  fmt.Sprintf("Model '%s' has no upstream dependencies (broken lineage)", model.Name),
				Model:    model.Path,
				FilePath: model.FilePath,
			})
		}
	}

	return diagnostics
}
