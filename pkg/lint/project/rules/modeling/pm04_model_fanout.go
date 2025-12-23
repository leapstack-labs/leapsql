package modeling

import (
	"fmt"
	"sort"
	"strings"

	"github.com/leapstack-labs/leapsql/pkg/lint/project"
)

func init() {
	project.Register(project.RuleDef{
		ID:          "PM04",
		Name:        "model-fanout",
		Group:       "modeling",
		Description: "Model has too many direct downstream consumers",
		Severity:    project.SeverityWarning,
		Check:       checkModelFanout,
		ConfigKeys:  []string{"threshold"},
	})
}

// checkModelFanout flags models that have too many direct downstream consumers.
// This indicates a potential "God Model" that is doing too much and should be
// refactored into smaller, more focused models.
//
// Default threshold: 3 direct downstream models (configurable)
//
// Best practice: If a model has many consumers, consider whether it should be
// split into multiple focused models, or if an abstraction layer is needed.
func checkModelFanout(ctx *project.Context) []project.Diagnostic {
	var diagnostics []project.Diagnostic
	threshold := ctx.GetConfig().ModelFanoutThreshold
	if threshold <= 0 {
		threshold = 3 // default
	}

	for path, model := range ctx.Models() {
		children := ctx.GetChildren(path)
		if len(children) > threshold {
			// Sort children for consistent output
			sortedChildren := make([]string, len(children))
			copy(sortedChildren, children)
			sort.Strings(sortedChildren)

			diagnostics = append(diagnostics, project.Diagnostic{
				RuleID:   "PM04",
				Severity: project.SeverityWarning,
				Message: fmt.Sprintf(
					"Model '%s' has %d direct downstream consumers (threshold: %d): %s; consider creating an intermediate abstraction",
					model.Name, len(children), threshold, strings.Join(sortedChildren, ", ")),
				Model:    model.Path,
				FilePath: model.FilePath,
			})
		}
	}

	return diagnostics
}
