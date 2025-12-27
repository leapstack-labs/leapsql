package projectrules

import (
	"fmt"
	"strings"

	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/project"
)

func init() {
	project.Register(project.RuleDef{
		ID:          "PS01",
		Name:        "model-naming",
		Group:       "structure",
		Description: "Model naming convention mismatch",
		Severity:    core.SeverityWarning,
		Check:       checkModelNaming,

		Rationale: `Consistent naming conventions make it easy to identify model types at a glance. Models in specific 
directories should follow the expected prefix convention: staging models use 'stg_', intermediate 
models use 'int_', and marts models use 'fct_' or 'dim_'.`,

		BadExample: `-- models/staging/orders.sql (missing stg_ prefix)
-- models/marts/order_metrics.sql (missing fct_ or dim_ prefix)`,

		GoodExample: `-- models/staging/stg_orders.sql
-- models/marts/fct_order_metrics.sql
-- models/marts/dim_customers.sql`,

		Fix: "Rename the model to include the appropriate prefix for its directory location.",
	})
}

// checkModelNaming flags models where the directory location and name prefix
// don't match the expected convention.
//
// Expected patterns:
//   - Models in /staging/ should have names starting with stg_
//   - Models in /intermediate/ should have names starting with int_
//   - Models in /marts/ should have names starting with fct_ or dim_
func checkModelNaming(ctx *project.Context) []project.Diagnostic {
	var diagnostics []project.Diagnostic

	for _, model := range ctx.Models() {
		pathLower := strings.ToLower(model.FilePath)
		nameLower := strings.ToLower(model.Name)

		// Check staging directory convention
		if strings.Contains(pathLower, "/staging/") {
			if !strings.HasPrefix(nameLower, "stg_") {
				diagnostics = append(diagnostics, project.Diagnostic{
					RuleID:           "PS01",
					Severity:         core.SeverityWarning,
					Message:          fmt.Sprintf("Model '%s' is in staging directory but doesn't have 'stg_' prefix", model.Name),
					Model:            model.Path,
					FilePath:         model.FilePath,
					DocumentationURL: lint.BuildDocURL("PS01"),
					ImpactScore:      lint.ImpactLow.Int(),
					AutoFixable:      false,
				})
			}
		}

		// Check intermediate directory convention
		if strings.Contains(pathLower, "/intermediate/") {
			if !strings.HasPrefix(nameLower, "int_") {
				diagnostics = append(diagnostics, project.Diagnostic{
					RuleID:           "PS01",
					Severity:         core.SeverityWarning,
					Message:          fmt.Sprintf("Model '%s' is in intermediate directory but doesn't have 'int_' prefix", model.Name),
					Model:            model.Path,
					FilePath:         model.FilePath,
					DocumentationURL: lint.BuildDocURL("PS01"),
					ImpactScore:      lint.ImpactLow.Int(),
					AutoFixable:      false,
				})
			}
		}

		// Check marts directory convention
		if strings.Contains(pathLower, "/marts/") {
			isFact := strings.HasPrefix(nameLower, "fct_")
			isDim := strings.HasPrefix(nameLower, "dim_")
			// Also allow intermediate models in marts (some teams structure this way)
			isInt := strings.HasPrefix(nameLower, "int_")

			if !isFact && !isDim && !isInt {
				// Only warn if the model type is marts (inferred)
				if model.Type == core.ModelTypeMarts {
					diagnostics = append(diagnostics, project.Diagnostic{
						RuleID:           "PS01",
						Severity:         core.SeverityWarning,
						Message:          fmt.Sprintf("Model '%s' is in marts directory but doesn't have 'fct_' or 'dim_' prefix", model.Name),
						Model:            model.Path,
						FilePath:         model.FilePath,
						DocumentationURL: lint.BuildDocURL("PS01"),
						ImpactScore:      lint.ImpactLow.Int(),
						AutoFixable:      false,
					})
				}
			}
		}
	}

	return diagnostics
}
