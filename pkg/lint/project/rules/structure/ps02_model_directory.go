package structure

import (
	"fmt"
	"strings"

	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/project"
)

func init() {
	project.Register(project.RuleDef{
		ID:          "PS02",
		Name:        "model-directory",
		Group:       "structure",
		Description: "Model directory mismatch",
		Severity:    lint.SeverityWarning,
		Check:       checkModelDirectory,
	})
}

// checkModelDirectory flags models where the name prefix suggests a type
// but the model is not in the expected directory.
//
// For example:
//   - stg_customers.sql should be in staging/, not in marts/
//   - fct_orders.sql should be in marts/, not in staging/
func checkModelDirectory(ctx *project.Context) []project.Diagnostic {
	var diagnostics []project.Diagnostic

	for _, model := range ctx.Models() {
		pathLower := strings.ToLower(model.FilePath)
		nameLower := strings.ToLower(model.Name)

		// Check if stg_ prefix model is in staging directory
		if strings.HasPrefix(nameLower, "stg_") {
			if !strings.Contains(pathLower, "/staging/") {
				diagnostics = append(diagnostics, project.Diagnostic{
					RuleID:           "PS02",
					Severity:         lint.SeverityWarning,
					Message:          fmt.Sprintf("Model '%s' has 'stg_' prefix but is not in staging directory", model.Name),
					Model:            model.Path,
					FilePath:         model.FilePath,
					DocumentationURL: lint.BuildDocURL("PS02"),
					ImpactScore:      lint.ImpactLow.Int(),
					AutoFixable:      false,
				})
			}
		}

		// Check if int_ prefix model is in intermediate directory
		if strings.HasPrefix(nameLower, "int_") {
			if !strings.Contains(pathLower, "/intermediate/") {
				diagnostics = append(diagnostics, project.Diagnostic{
					RuleID:           "PS02",
					Severity:         lint.SeverityWarning,
					Message:          fmt.Sprintf("Model '%s' has 'int_' prefix but is not in intermediate directory", model.Name),
					Model:            model.Path,
					FilePath:         model.FilePath,
					DocumentationURL: lint.BuildDocURL("PS02"),
					ImpactScore:      lint.ImpactLow.Int(),
					AutoFixable:      false,
				})
			}
		}

		// Check if fct_/dim_ prefix model is in marts directory
		if strings.HasPrefix(nameLower, "fct_") || strings.HasPrefix(nameLower, "dim_") {
			if !strings.Contains(pathLower, "/marts/") {
				diagnostics = append(diagnostics, project.Diagnostic{
					RuleID:           "PS02",
					Severity:         lint.SeverityWarning,
					Message:          fmt.Sprintf("Model '%s' has 'fct_' or 'dim_' prefix but is not in marts directory", model.Name),
					Model:            model.Path,
					FilePath:         model.FilePath,
					DocumentationURL: lint.BuildDocURL("PS02"),
					ImpactScore:      lint.ImpactLow.Int(),
					AutoFixable:      false,
				})
			}
		}
	}

	return diagnostics
}
