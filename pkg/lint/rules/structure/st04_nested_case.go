package structure

import (
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/internal/ast"
	"github.com/leapstack-labs/leapsql/pkg/parser"
)

func init() {
	lint.Register(NestedCase)
}

// NestedCase warns about nested CASE expressions which reduce readability.
var NestedCase = lint.RuleDef{
	ID:          "ST04",
	Name:        "structure.nested_case",
	Group:       "structure",
	Description: "Nested CASE expressions reduce readability.",
	Severity:    lint.SeverityInfo,
	Check:       checkNestedCase,
}

func checkNestedCase(stmt any, _ lint.DialectInfo) []lint.Diagnostic {
	selectStmt, ok := stmt.(*parser.SelectStmt)
	if !ok {
		return nil
	}

	var diagnostics []lint.Diagnostic
	for _, caseExpr := range ast.CollectCaseExprs(selectStmt) {
		if hasNestedCase(caseExpr) {
			diagnostics = append(diagnostics, lint.Diagnostic{
				RuleID:   "ST04",
				Severity: lint.SeverityInfo,
				Message:  "Nested CASE expressions reduce readability; consider refactoring",
			})
		}
	}
	return diagnostics
}

func hasNestedCase(caseExpr *parser.CaseExpr) bool {
	// Check in WHEN conditions and results
	for _, when := range caseExpr.Whens {
		if containsCase(when.Condition) || containsCase(when.Result) {
			return true
		}
	}
	// Check in ELSE
	if caseExpr.Else != nil && containsCase(caseExpr.Else) {
		return true
	}
	return false
}

func containsCase(expr parser.Expr) bool {
	found := false
	ast.Walk(expr, func(node any) bool {
		if _, ok := node.(*parser.CaseExpr); ok {
			found = true
			return false
		}
		return true
	})
	return found
}
