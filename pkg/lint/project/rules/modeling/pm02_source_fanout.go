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
		ID:          "PM02",
		Name:        "source-fanout",
		Group:       "modeling",
		Description: "Source referenced by multiple non-staging models",
		Severity:    project.SeverityWarning,
		Check:       checkSourceFanout,
	})
}

// checkSourceFanout flags sources (external tables) that are referenced by
// more than one non-staging model. This indicates that the source should
// be abstracted into a single staging model.
//
// Best practice: Each raw source should be referenced by exactly one
// staging model, which then provides a clean interface for downstream models.
func checkSourceFanout(ctx *project.Context) []project.Diagnostic {
	var diagnostics []project.Diagnostic

	// Build a map of source -> non-staging models that reference it
	sourceRefs := make(map[string][]string)

	for _, model := range ctx.Models() {
		// Only look at non-staging models
		if model.Type == lint.ModelTypeStaging {
			continue
		}

		for _, source := range model.Sources {
			// Check if this source is an external table (not a known model)
			if !ctx.IsModel(source) {
				sourceRefs[source] = append(sourceRefs[source], model.Path)
			}
		}
	}

	// Find sources with multiple non-staging consumers
	for source, consumers := range sourceRefs {
		if len(consumers) > 1 {
			sort.Strings(consumers)
			diagnostics = append(diagnostics, project.Diagnostic{
				RuleID:   "PM02",
				Severity: project.SeverityWarning,
				Message: fmt.Sprintf(
					"Source '%s' is referenced by %d non-staging models (%s); consider creating a staging model",
					source, len(consumers), strings.Join(consumers, ", ")),
				Model: consumers[0], // Associate with first consumer
			})
		}
	}

	return diagnostics
}
