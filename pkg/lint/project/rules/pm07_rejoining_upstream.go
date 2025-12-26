package projectrules

import (
	"fmt"

	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/project"
)

func init() {
	project.Register(project.RuleDef{
		ID:          "PM07",
		Name:        "rejoining-upstream",
		Group:       "modeling",
		Description: "Unnecessary intermediate model in a fan-in pattern (A→B, A→C, B→C where B has no other consumers)",
		Severity:    lint.SeverityWarning,
		Check:       checkRejoiningUpstream,
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
					Severity: lint.SeverityWarning,
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
