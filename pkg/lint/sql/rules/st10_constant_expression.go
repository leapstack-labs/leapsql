package rules

import (
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql/internal/ast"
	"github.com/leapstack-labs/leapsql/pkg/token"
)

func init() {
	sql.Register(ConstantExpression)
}

// ConstantExpression warns about unnecessary constant expressions like WHERE 1=1.
var ConstantExpression = sql.RuleDef{
	ID:          "ST10",
	Name:        "structure.constant_expression",
	Group:       "structure",
	Description: "Unnecessary constant expressions like WHERE 1=1 or WHERE true.",
	Severity:    core.SeverityInfo,
	Check:       checkConstantExpression,

	Rationale: `Constant expressions like WHERE 1=1 or WHERE true are often artifacts of dynamic SQL 
generation. In static SQL models, they add noise without affecting results. Removing them makes the 
query cleaner and easier to understand.`,

	BadExample: `SELECT *
FROM orders
WHERE 1=1
  AND status = 'active'`,

	GoodExample: `SELECT *
FROM orders
WHERE status = 'active'`,

	Fix: "Remove constant expressions from WHERE clauses.",
}

func checkConstantExpression(stmt any, _ lint.DialectInfo, _ map[string]any) []lint.Diagnostic {
	selectStmt, ok := stmt.(*core.SelectStmt)
	if !ok {
		return nil
	}

	var diagnostics []lint.Diagnostic

	// Check WHERE clauses in all SELECT cores
	for _, selectCore := range ast.CollectSelectCores(selectStmt) {
		if selectCore.Where == nil {
			continue
		}

		// Check the WHERE expression for constant patterns
		diags := findConstantExpressionsST10(selectCore.Where, selectCore.Span.Start)
		diagnostics = append(diagnostics, diags...)
	}

	return diagnostics
}

// findConstantExpressionsST10 recursively finds constant expressions.
func findConstantExpressionsST10(expr core.Expr, pos token.Position) []lint.Diagnostic {
	var diagnostics []lint.Diagnostic

	switch e := expr.(type) {
	case *core.BinaryExpr:
		// Check for 1=1, 'a'='a', etc.
		if isConstantEqualityST10(e) {
			diagnostics = append(diagnostics, lint.Diagnostic{
				RuleID:           "ST10",
				Severity:         core.SeverityInfo,
				Message:          "Unnecessary constant expression; this condition is always true",
				Pos:              pos,
				DocumentationURL: lint.BuildDocURL("ST10"),
				ImpactScore:      lint.ImpactLow.Int(),
				AutoFixable:      false,
			})
		}

		// Check nested AND/OR expressions
		if e.Op == token.AND || e.Op == token.OR {
			diagnostics = append(diagnostics, findConstantExpressionsST10(e.Left, pos)...)
			diagnostics = append(diagnostics, findConstantExpressionsST10(e.Right, pos)...)
		}

	case *core.Literal:
		// Check for WHERE true, WHERE false, WHERE 1, WHERE 0
		if isConstantBooleanLiteralST10(e) {
			diagnostics = append(diagnostics, lint.Diagnostic{
				RuleID:           "ST10",
				Severity:         core.SeverityInfo,
				Message:          "Unnecessary constant expression; this condition is always " + boolValueST10(e),
				Pos:              pos,
				DocumentationURL: lint.BuildDocURL("ST10"),
				ImpactScore:      lint.ImpactLow.Int(),
				AutoFixable:      false,
			})
		}

	case *core.ParenExpr:
		// Check inside parentheses
		diagnostics = append(diagnostics, findConstantExpressionsST10(e.Expr, pos)...)
	}

	return diagnostics
}

// isConstantEqualityST10 checks if a binary expression is a constant equality like 1=1.
func isConstantEqualityST10(expr *core.BinaryExpr) bool {
	if expr.Op != token.EQ {
		return false
	}

	leftLit, leftOk := expr.Left.(*core.Literal)
	rightLit, rightOk := expr.Right.(*core.Literal)

	if !leftOk || !rightOk {
		return false
	}

	// Check if both literals have the same value
	// Compare by type and value
	if leftLit.Type != rightLit.Type {
		return false
	}

	return leftLit.Value == rightLit.Value
}

// isConstantBooleanLiteralST10 checks if a literal is a constant boolean value.
func isConstantBooleanLiteralST10(lit *core.Literal) bool {
	switch lit.Type {
	case core.LiteralBool:
		return true
	case core.LiteralNumber:
		// 1 and 0 in WHERE context act as true/false
		return lit.Value == "1" || lit.Value == "0"
	}
	return false
}

// boolValueST10 returns the boolean interpretation of a literal.
func boolValueST10(lit *core.Literal) string {
	switch lit.Type {
	case core.LiteralBool:
		return lit.Value
	case core.LiteralNumber:
		if lit.Value == "1" {
			return "true"
		}
		return "false"
	}
	return "unknown"
}
