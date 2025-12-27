package projectrules

import (
	"fmt"

	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/project"
)

func init() {
	project.Register(project.RuleDef{
		ID:          "PM06",
		Name:        "downstream-on-source",
		Group:       "modeling",
		Description: "Marts or intermediate model depends directly on source (not staging)",
		Severity:    lint.SeverityWarning,
		Check:       checkDownstreamOnSource,

		Rationale: `The recommended transformation pattern is Sources → Staging → Intermediate → Marts. When marts 
or intermediate models reference sources directly, they bypass data cleaning in staging, leading to 
duplicated transformation logic and making lineage harder to understand.`,

		BadExample: `-- models/marts/fct_orders.sql
SELECT * FROM raw.orders  -- Direct source reference in marts`,

		GoodExample: `-- models/staging/stg_orders.sql
SELECT * FROM raw.orders

-- models/marts/fct_orders.sql  
SELECT * FROM {{ ref('stg_orders') }}  -- Reference staging`,

		Fix: "Create a staging model for the source and reference it instead of the raw source.",
	})
}

// checkDownstreamOnSource flags marts and intermediate models that depend
// directly on raw sources instead of staging models. This breaks the
// recommended data transformation pattern:
//
//	Sources → Staging → Intermediate → Marts
//
// When marts/intermediate models reference sources directly, it:
//   - Bypasses data cleaning in staging
//   - Leads to duplicated transformation logic
//   - Makes lineage harder to understand
//
// Best practice: All raw sources should flow through staging models first.
func checkDownstreamOnSource(ctx *project.Context) []project.Diagnostic {
	var diagnostics []project.Diagnostic

	for _, model := range ctx.Models() {
		// Only check marts and intermediate models
		if model.Type != core.ModelTypeMarts && model.Type != core.ModelTypeIntermediate {
			continue
		}

		// Check if any of its sources are external (not a known model)
		for _, source := range model.Sources {
			if !ctx.IsModel(source) {
				// This is an external source - marts/intermediate shouldn't reference it directly
				diagnostics = append(diagnostics, project.Diagnostic{
					RuleID:   "PM06",
					Severity: lint.SeverityWarning,
					Message: fmt.Sprintf(
						"%s model '%s' depends directly on source '%s'; use a staging model instead",
						string(model.Type), model.Name, source),
					Model:            model.Path,
					FilePath:         model.FilePath,
					DocumentationURL: lint.BuildDocURL("PM06"),
					ImpactScore:      lint.ImpactHigh.Int(),
					AutoFixable:      false,
				})
			}
		}
	}

	return diagnostics
}
