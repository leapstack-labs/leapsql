package projectrules

import (
	"fmt"

	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/project"
)

func init() {
	project.Register(project.RuleDef{
		ID:          "PM07",
		Name:        "rejoining-upstream",
		Group:       "modeling",
		Description: "Unnecessary intermediate model in a fan-in pattern (A→B, A→C, B→C where B has no other consumers)",
		Severity:    core.SeverityWarning,
		Check:       checkRejoiningUpstream,

		Rationale: `When model B has exactly one consumer (C), and B's upstream (A) is also a direct upstream of C, 
model B serves no purpose as a reusable abstraction. The pattern A→B→C with A→C means B's logic could 
be inlined into C, eliminating an unnecessary model and simplifying the DAG.`,

		BadExample: `-- stg_orders (A) → int_order_totals (B) → fct_report (C)
-- stg_orders (A) → fct_report (C)
-- int_order_totals only has one consumer and doesn't add reusable value`,

		GoodExample: `-- Either inline B into C:
-- stg_orders → fct_report (with B's logic inlined)

-- Or give B more consumers to justify its existence:
-- stg_orders → int_order_totals → fct_report
-- stg_orders → int_order_totals → fct_dashboard`,

		Fix: "Either inline the intermediate model's logic into its single consumer, or add more consumers to justify it as a reusable abstraction.",
	})
}

// checkRejoiningUpstream detects wasteful intermediate models in a fan-in pattern.
//
// Pattern detected:
//
//	A → B → C
//	A -----→ C
//
// Where B has no consumers other than C. In this case, B serves no purpose
// as an abstraction since both A and C would have direct access to the data,
// and B could be inlined into C.
//
// This rule looks for:
//  1. Model B that has exactly one downstream consumer (C)
//  2. Where B's upstream (A) is also a direct upstream of C
//
// Best practice: Either remove B and inline its logic into C, or add more
// consumers to B to justify its existence as a reusable abstraction.
func checkRejoiningUpstream(ctx *project.Context) []project.Diagnostic {
	var diagnostics []project.Diagnostic

	for pathB, modelB := range ctx.Models() {
		childrenB := ctx.GetChildren(pathB)

		// B must have exactly one downstream consumer (C)
		if len(childrenB) != 1 {
			continue
		}
		pathC := childrenB[0]

		modelC, ok := ctx.GetModel(pathC)
		if !ok {
			continue
		}

		// Check if any upstream of B is also a direct upstream of C
		parentsB := ctx.GetParents(pathB)
		parentsC := ctx.GetParents(pathC)

		// Create a set of C's parents for fast lookup
		parentsCSet := make(map[string]bool)
		for _, p := range parentsC {
			parentsCSet[p] = true
		}

		// Check each parent of B
		for _, pathA := range parentsB {
			if pathA == pathC {
				continue // Skip self-reference
			}

			// If A is also a direct parent of C, we have the pattern
			if parentsCSet[pathA] {
				modelA, hasA := ctx.GetModel(pathA)
				aName := pathA
				if hasA {
					aName = modelA.Name
				}

				diagnostics = append(diagnostics, project.Diagnostic{
					RuleID:   "PM07",
					Severity: core.SeverityWarning,
					Message: fmt.Sprintf(
						"Model '%s' is an unnecessary intermediate: '%s' → '%s' → '%s', but '%s' also depends directly on '%s'; consider inlining '%s' into '%s'",
						modelB.Name, aName, modelB.Name, modelC.Name, modelC.Name, aName, modelB.Name, modelC.Name),
					Model:            modelB.Path,
					FilePath:         modelB.FilePath,
					DocumentationURL: lint.BuildDocURL("PM07"),
					ImpactScore:      lint.ImpactMedium.Int(),
					AutoFixable:      false,
				})
				break // One finding per model is enough
			}
		}
	}

	return diagnostics
}
