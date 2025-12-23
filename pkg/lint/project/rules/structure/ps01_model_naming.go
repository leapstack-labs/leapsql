package structure

import (
	"fmt"
	"strings"

	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/project"
)

func init() {
	project.Register(project.RuleDef{
		ID:          "PS01",
		Name:        "model-naming",
		Group:       "structure",
		Description: "Model naming convention mismatch",
		Severity:    lint.SeverityWarning,
		Check:       checkModelNaming,
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
					Severity:         lint.SeverityWarning,
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
					Severity:         lint.SeverityWarning,
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
				if model.Type == lint.ModelTypeMarts {
					diagnostics = append(diagnostics, project.Diagnostic{
						RuleID:           "PS01",
						Severity:         lint.SeverityWarning,
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
