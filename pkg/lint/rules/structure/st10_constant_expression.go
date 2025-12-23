package structure

import (
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/internal/ast"
	"github.com/leapstack-labs/leapsql/pkg/parser"
	"github.com/leapstack-labs/leapsql/pkg/token"
)

func init() {
	lint.Register(ConstantExpression)
}

// ConstantExpression warns about unnecessary constant expressions like WHERE 1=1.
var ConstantExpression = lint.RuleDef{
	ID:          "ST10",
	Name:        "structure.constant_expression",
	Group:       "structure",
	Description: "Unnecessary constant expressions like WHERE 1=1 or WHERE true.",
	Severity:    lint.SeverityInfo,
	Check:       checkConstantExpression,
}

func checkConstantExpression(stmt any, _ lint.DialectInfo, _ map[string]any) []lint.Diagnostic {
	selectStmt, ok := stmt.(*parser.SelectStmt)
	if !ok {
		return nil
	}

	var diagnostics []lint.Diagnostic

	// Check WHERE clauses in all SELECT cores
	for _, core := range ast.CollectSelectCores(selectStmt) {
		if core.Where == nil {
			continue
		}

		// Check the WHERE expression for constant patterns
		diags := findConstantExpressions(core.Where, core.Span.Start)
		diagnostics = append(diagnostics, diags...)
	}

	return diagnostics
}

// findConstantExpressions recursively finds constant expressions.
func findConstantExpressions(expr parser.Expr, pos token.Position) []lint.Diagnostic {
	var diagnostics []lint.Diagnostic

	switch e := expr.(type) {
	case *parser.BinaryExpr:
		// Check for 1=1, 'a'='a', etc.
		if isConstantEquality(e) {
			diagnostics = append(diagnostics, lint.Diagnostic{
				RuleID:           "ST10",
				Severity:         lint.SeverityInfo,
				Message:          "Unnecessary constant expression; this condition is always true",
				Pos:              pos,
				DocumentationURL: lint.BuildDocURL("ST10"),
				ImpactScore:      lint.ImpactLow.Int(),
				AutoFixable:      false,
			})
		}

		// Check nested AND/OR expressions
		if e.Op == token.AND || e.Op == token.OR {
			diagnostics = append(diagnostics, findConstantExpressions(e.Left, pos)...)
			diagnostics = append(diagnostics, findConstantExpressions(e.Right, pos)...)
		}

	case *parser.Literal:
		// Check for WHERE true, WHERE false, WHERE 1, WHERE 0
		if isConstantBooleanLiteral(e) {
			diagnostics = append(diagnostics, lint.Diagnostic{
				RuleID:           "ST10",
				Severity:         lint.SeverityInfo,
				Message:          "Unnecessary constant expression; this condition is always " + boolValue(e),
				Pos:              pos,
				DocumentationURL: lint.BuildDocURL("ST10"),
				ImpactScore:      lint.ImpactLow.Int(),
				AutoFixable:      false,
			})
		}

	case *parser.ParenExpr:
		// Check inside parentheses
		diagnostics = append(diagnostics, findConstantExpressions(e.Expr, pos)...)
	}

	return diagnostics
}

// isConstantEquality checks if a binary expression is a constant equality like 1=1.
func isConstantEquality(expr *parser.BinaryExpr) bool {
	if expr.Op != token.EQ {
		return false
	}

	leftLit, leftOk := expr.Left.(*parser.Literal)
	rightLit, rightOk := expr.Right.(*parser.Literal)

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

// isConstantBooleanLiteral checks if a literal is a constant boolean value.
func isConstantBooleanLiteral(lit *parser.Literal) bool {
	switch lit.Type {
	case parser.LiteralBool:
		return true
	case parser.LiteralNumber:
		// 1 and 0 in WHERE context act as true/false
		return lit.Value == "1" || lit.Value == "0"
	}
	return false
}

// boolValue returns the boolean interpretation of a literal.
func boolValue(lit *parser.Literal) string {
	switch lit.Type {
	case parser.LiteralBool:
		return lit.Value
	case parser.LiteralNumber:
		if lit.Value == "1" {
			return "true"
		}
		return "false"
	}
	return "unknown"
}
