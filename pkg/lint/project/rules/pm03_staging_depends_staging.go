package projectrules

import (
	"fmt"

	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/project"
)

func init() {
	project.Register(project.RuleDef{
		ID:          "PM03",
		Name:        "staging-depends-staging",
		Group:       "modeling",
		Description: "Staging model references another staging model",
		Severity:    core.SeverityWarning,
		Check:       checkStagingDependsStaging,

		Rationale: `Staging models should only reference raw sources, not other staging models. When staging models 
depend on each other, it blurs the boundary between data cleaning (staging) and data transformation 
(intermediate/marts). If you need to combine staging models, create an intermediate model instead.`,

		BadExample: `-- models/staging/stg_orders_enhanced.sql
SELECT o.*, c.name
FROM {{ ref('stg_orders') }} o  -- Staging depending on staging
JOIN {{ ref('stg_customers') }} c ON o.customer_id = c.id`,

		GoodExample: `-- models/intermediate/int_orders_with_customers.sql
SELECT o.*, c.name
FROM {{ ref('stg_orders') }} o
JOIN {{ ref('stg_customers') }} c ON o.customer_id = c.id`,

		Fix: "Move the model to the intermediate layer if it combines staging models, or reference raw sources directly if it's truly staging.",
	})
}

// checkStagingDependsStaging flags staging models that depend on other
// staging models. Staging models should only reference raw sources,
// not other staging models.
//
// Best practice: Staging models clean and normalize raw data. If you need
// to combine staging models, create an intermediate model instead.
func checkStagingDependsStaging(ctx *project.Context) []project.Diagnostic {
	var diagnostics []project.Diagnostic

	for _, model := range ctx.Models() {
		// Only check staging models
		if model.Type != core.ModelTypeStaging {
			continue
		}

		// Check if any of its sources are also staging models
		for _, source := range model.Sources {
			sourceModel, ok := ctx.GetModel(source)
			if !ok {
				continue // External source, not a model
			}

			if sourceModel.Type == core.ModelTypeStaging {
				diagnostics = append(diagnostics, project.Diagnostic{
					RuleID:   "PM03",
					Severity: core.SeverityWarning,
					Message: fmt.Sprintf(
						"Staging model '%s' depends on staging model '%s'; staging should only reference raw sources",
						model.Name, sourceModel.Name),
					Model:            model.Path,
					FilePath:         model.FilePath,
					DocumentationURL: lint.BuildDocURL("PM03"),
					ImpactScore:      lint.ImpactMedium.Int(),
					AutoFixable:      false,
				})
			}
		}
	}

	return diagnostics
}
