package projectrules

import (
	"fmt"

	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/project"
)

func init() {
	project.Register(project.RuleDef{
		ID:          "PM01",
		Name:        "root-models",
		Group:       "modeling",
		Description: "Models with no sources (broken DAG lineage)",
		Severity:    lint.SeverityWarning,
		Check:       checkRootModels,

		Rationale: `Non-staging models without upstream dependencies indicate broken lineage. These "root" models 
don't reference any tables, suggesting either a configuration error, a model that should be a seed, 
or missing FROM/JOIN clauses. Proper DAG lineage is essential for understanding data flow.`,

		BadExample: `-- models/marts/fct_orders.sql
SELECT 1 AS id, 'test' AS name  -- No FROM clause, no sources`,

		GoodExample: `-- models/marts/fct_orders.sql
SELECT id, name
FROM {{ ref('stg_orders') }}`,

		Fix: "Add appropriate FROM/JOIN clauses to reference upstream models or sources, or convert to a seed if this is static data.",
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
		if model.Type == core.ModelTypeStaging {
			continue
		}

		// Check if model has no sources
		if len(model.Sources) == 0 {
			diagnostics = append(diagnostics, project.Diagnostic{
				RuleID:           "PM01",
				Severity:         lint.SeverityWarning,
				Message:          fmt.Sprintf("Model '%s' has no upstream dependencies (broken lineage)", model.Name),
				Model:            model.Path,
				FilePath:         model.FilePath,
				DocumentationURL: lint.BuildDocURL("PM01"),
				ImpactScore:      lint.ImpactMedium.Int(),
				AutoFixable:      false,
			})
		}
	}

	return diagnostics
}
