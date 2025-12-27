package rules

import (
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql/internal/ast"
	"github.com/leapstack-labs/leapsql/pkg/token"
)

func init() {
	sql.Register(IsNullComparison)
}

// IsNullComparison warns about using = NULL instead of IS NULL.
var IsNullComparison = sql.RuleDef{
	ID:          "CV05",
	Name:        "convention.is_null",
	Group:       "convention",
	Description: "Use IS NULL instead of = NULL for NULL comparisons.",
	Severity:    core.SeverityWarning,
	Check:       checkIsNullComparison,

	Rationale: `In SQL, NULL represents unknown, and comparing anything to NULL with = 
or != always yields NULL (unknown), not true or false. This is a common source of 
bugs. Use IS NULL or IS NOT NULL for correct NULL handling.`,

	BadExample: `SELECT * FROM orders
WHERE shipped_date = NULL`,

	GoodExample: `SELECT * FROM orders
WHERE shipped_date IS NULL`,

	Fix: "Replace = NULL with IS NULL, and != NULL with IS NOT NULL.",
}

func checkIsNullComparison(stmt any, _ lint.DialectInfo, _ map[string]any) []lint.Diagnostic {
	selectStmt, ok := stmt.(*core.SelectStmt)
	if !ok {
		return nil
	}

	var diagnostics []lint.Diagnostic
	for _, binExpr := range ast.CollectBinaryExprs(selectStmt) {
		// Check for = NULL or != NULL
		if binExpr.Op != token.EQ && binExpr.Op != token.NE {
			continue
		}

		// Check if either side is a NULL literal
		leftNull := isNullLiteralCV05(binExpr.Left)
		rightNull := isNullLiteralCV05(binExpr.Right)

		if leftNull || rightNull {
			msg := "Use IS NULL instead of = NULL"
			if binExpr.Op == token.NE {
				msg = "Use IS NOT NULL instead of != NULL"
			}
			diagnostics = append(diagnostics, lint.Diagnostic{
				RuleID:           "CV05",
				Severity:         core.SeverityWarning,
				Message:          msg + "; = NULL always evaluates to NULL, not true or false",
				DocumentationURL: lint.BuildDocURL("CV05"),
				ImpactScore:      lint.ImpactHigh.Int(),
				AutoFixable:      false,
			})
		}
	}
	return diagnostics
}

func isNullLiteralCV05(expr core.Expr) bool {
	lit, ok := expr.(*core.Literal)
	return ok && lit.Type == core.LiteralNull
}
