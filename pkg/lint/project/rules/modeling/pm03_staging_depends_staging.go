package modeling

import (
	"fmt"

	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/project"
)

func init() {
	project.Register(project.RuleDef{
		ID:          "PM03",
		Name:        "staging-depends-staging",
		Group:       "modeling",
		Description: "Staging model references another staging model",
		Severity:    lint.SeverityWarning,
		Check:       checkStagingDependsStaging,
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
		if model.Type != lint.ModelTypeStaging {
			continue
		}

		// Check if any of its sources are also staging models
		for _, source := range model.Sources {
			sourceModel, ok := ctx.GetModel(source)
			if !ok {
				continue // External source, not a model
			}

			if sourceModel.Type == lint.ModelTypeStaging {
				diagnostics = append(diagnostics, project.Diagnostic{
					RuleID:   "PM03",
					Severity: lint.SeverityWarning,
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
