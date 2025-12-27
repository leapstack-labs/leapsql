package projectrules

import (
	"fmt"
	"sort"
	"strings"

	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/project"
)

func init() {
	project.Register(project.RuleDef{
		ID:          "PM02",
		Name:        "source-fanout",
		Group:       "modeling",
		Description: "Source referenced by multiple non-staging models",
		Severity:    lint.SeverityWarning,
		Check:       checkSourceFanout,

		Rationale: `Each raw source should be referenced by exactly one staging model, which then provides a clean 
interface for downstream models. When multiple non-staging models reference the same source directly, 
transformation logic gets duplicated and changes to the source require updates in multiple places.`,

		BadExample: `-- models/marts/fct_orders.sql
SELECT * FROM raw_orders  -- Direct source reference

-- models/marts/fct_revenue.sql  
SELECT * FROM raw_orders  -- Same source, duplicated reference`,

		GoodExample: `-- models/staging/stg_orders.sql
SELECT * FROM raw_orders  -- Single staging model for source

-- models/marts/fct_orders.sql
SELECT * FROM {{ ref('stg_orders') }}  -- Reference staging model`,

		Fix: "Create a staging model for the source and have all downstream models reference the staging model instead.",
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
		if model.Type == core.ModelTypeStaging {
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
				Severity: lint.SeverityWarning,
				Message: fmt.Sprintf(
					"Source '%s' is referenced by %d non-staging models (%s); consider creating a staging model",
					source, len(consumers), strings.Join(consumers, ", ")),
				Model:            consumers[0], // Associate with first consumer
				DocumentationURL: lint.BuildDocURL("PM02"),
				ImpactScore:      lint.ImpactMedium.Int(),
				AutoFixable:      false,
			})
		}
	}

	return diagnostics
}
