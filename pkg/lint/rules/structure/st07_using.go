package structure

import (
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/internal/ast"
	"github.com/leapstack-labs/leapsql/pkg/parser"
	"github.com/leapstack-labs/leapsql/pkg/token"
)

func init() {
	lint.Register(PreferUsing)
}

// PreferUsing recommends USING instead of ON for simple equality joins.
var PreferUsing = lint.RuleDef{
	ID:          "ST07",
	Name:        "structure.using",
	Group:       "structure",
	Description: "Prefer USING clause for simple equality joins on same-named columns.",
	Severity:    lint.SeverityHint,
	Check:       checkPreferUsing,
}

func checkPreferUsing(stmt any, _ lint.DialectInfo, _ map[string]any) []lint.Diagnostic {
	selectStmt, ok := stmt.(*parser.SelectStmt)
	if !ok {
		return nil
	}

	var diagnostics []lint.Diagnostic
	for _, join := range ast.CollectJoins(selectStmt) {
		// Skip if already using USING or NATURAL
		if len(join.Using) > 0 || join.Natural || join.Condition == nil {
			continue
		}

		// Check if condition is a simple equality on same-named columns
		if canUseUsing(join.Condition) {
			diagnostics = append(diagnostics, lint.Diagnostic{
				RuleID:   "ST07",
				Severity: lint.SeverityHint,
				Message:  "Consider using USING clause for join on same-named columns",
				Pos:      join.Span.Start,
			})
		}
	}
	return diagnostics
}

func canUseUsing(condition parser.Expr) bool {
	binExpr, ok := condition.(*parser.BinaryExpr)
	if !ok || binExpr.Op != token.EQ {
		return false
	}

	leftCol, leftOk := binExpr.Left.(*parser.ColumnRef)
	rightCol, rightOk := binExpr.Right.(*parser.ColumnRef)

	if !leftOk || !rightOk {
		return false
	}

	// Both must have table qualifiers and same column name
	return leftCol.Table != "" && rightCol.Table != "" &&
		leftCol.Column == rightCol.Column
}
