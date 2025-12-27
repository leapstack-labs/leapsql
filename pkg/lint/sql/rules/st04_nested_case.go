package rules

import (
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql/internal/ast"
)

func init() {
	sql.Register(NestedCase)
}

// NestedCase warns about nested CASE expressions which reduce readability.
var NestedCase = sql.RuleDef{
	ID:          "ST04",
	Name:        "structure.nested_case",
	Group:       "structure",
	Description: "Nested CASE expressions reduce readability.",
	Severity:    core.SeverityInfo,
	Check:       checkNestedCase,

	Rationale: `Nested CASE expressions are difficult to read and understand. They often indicate complex 
business logic that could be simplified by restructuring the query, using CTEs, or extracting the logic 
into a separate model or view.`,

	BadExample: `SELECT
  CASE
    WHEN status = 'A' THEN
      CASE
        WHEN priority = 1 THEN 'High Active'
        ELSE 'Low Active'
      END
    ELSE 'Inactive'
  END AS label
FROM tasks`,

	GoodExample: `SELECT
  CASE
    WHEN status = 'A' AND priority = 1 THEN 'High Active'
    WHEN status = 'A' THEN 'Low Active'
    ELSE 'Inactive'
  END AS label
FROM tasks`,

	Fix: "Flatten nested CASE expressions by combining conditions, or extract complex logic into a CTE or separate model.",
}

func checkNestedCase(stmt any, _ lint.DialectInfo, _ map[string]any) []lint.Diagnostic {
	selectStmt, ok := stmt.(*core.SelectStmt)
	if !ok {
		return nil
	}

	var diagnostics []lint.Diagnostic
	for _, caseExpr := range ast.CollectCaseExprs(selectStmt) {
		if hasNestedCaseST04(caseExpr) {
			diagnostics = append(diagnostics, lint.Diagnostic{
				RuleID:           "ST04",
				Severity:         core.SeverityInfo,
				Message:          "Nested CASE expressions reduce readability; consider refactoring",
				DocumentationURL: lint.BuildDocURL("ST04"),
				ImpactScore:      lint.ImpactMedium.Int(),
				AutoFixable:      false,
			})
		}
	}
	return diagnostics
}

func hasNestedCaseST04(caseExpr *core.CaseExpr) bool {
	// Check in WHEN conditions and results
	for _, when := range caseExpr.Whens {
		if containsCaseST04(when.Condition) || containsCaseST04(when.Result) {
			return true
		}
	}
	// Check in ELSE
	if caseExpr.Else != nil && containsCaseST04(caseExpr.Else) {
		return true
	}
	return false
}

func containsCaseST04(expr core.Expr) bool {
	found := false
	ast.Walk(expr, func(node any) bool {
		if _, ok := node.(*core.CaseExpr); ok {
			found = true
			return false
		}
		return true
	})
	return found
}
