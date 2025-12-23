package modeling

import (
	"fmt"
	"sort"
	"strings"

	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/project"
)

func init() {
	project.Register(project.RuleDef{
		ID:          "PM05",
		Name:        "too-many-joins",
		Group:       "modeling",
		Description: "Model references too many upstream models",
		Severity:    lint.SeverityWarning,
		Check:       checkTooManyJoins,
		ConfigKeys:  []string{"threshold"},
	})
}

// checkTooManyJoins flags models that reference too many upstream sources.
// High join counts often indicate a "God Model" that tries to do too much
// in a single query, which can be:
//   - Hard to understand and maintain
//   - Slow to execute (many JOINs)
//   - A sign of missing intermediate models
//
// Default threshold: 7 upstream models (configurable)
//
// Best practice: Break complex queries into smaller intermediate models
// that can be composed together.
func checkTooManyJoins(ctx *project.Context) []project.Diagnostic {
	var diagnostics []project.Diagnostic
	threshold := ctx.GetConfig().TooManyJoinsThreshold
	if threshold <= 0 {
		threshold = 7 // default
	}

	for _, model := range ctx.Models() {
		// Count unique upstream model references
		if len(model.Sources) > threshold {
			// Sort sources for consistent output
			sortedSources := make([]string, len(model.Sources))
			copy(sortedSources, model.Sources)
			sort.Strings(sortedSources)

			diagnostics = append(diagnostics, project.Diagnostic{
				RuleID:   "PM05",
				Severity: lint.SeverityWarning,
				Message: fmt.Sprintf(
					"Model '%s' references %d upstream sources (threshold: %d): %s; consider creating intermediate models",
					model.Name, len(model.Sources), threshold, strings.Join(sortedSources, ", ")),
				Model:            model.Path,
				FilePath:         model.FilePath,
				DocumentationURL: lint.BuildDocURL("PM05"),
				ImpactScore:      lint.ImpactMedium.Int(),
				AutoFixable:      false,
			})
		}
	}

	return diagnostics
}
