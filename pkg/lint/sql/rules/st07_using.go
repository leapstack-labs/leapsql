package rules

import (
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql/internal/ast"
	"github.com/leapstack-labs/leapsql/pkg/parser"
	"github.com/leapstack-labs/leapsql/pkg/token"
)

func init() {
	sql.Register(PreferUsing)
}

// PreferUsing recommends USING instead of ON for simple equality joins.
var PreferUsing = sql.RuleDef{
	ID:          "ST07",
	Name:        "structure.using",
	Group:       "structure",
	Description: "Prefer USING clause for simple equality joins on same-named columns.",
	Severity:    core.SeverityHint,
	Check:       checkPreferUsing,

	Rationale: `The USING clause is more concise than ON when joining tables on columns with identical names. 
It clearly communicates that the join is on matching column names and automatically deduplicates the join 
column in the result set.`,

	BadExample: `SELECT *
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id`,

	GoodExample: `SELECT *
FROM orders o
JOIN customers c USING (customer_id)`,

	Fix: "Replace ON with USING when joining on columns that have the same name in both tables.",
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
		if canUseUsingST07(join.Condition) {
			diagnostics = append(diagnostics, lint.Diagnostic{
				RuleID:           "ST07",
				Severity:         core.SeverityHint,
				Message:          "Consider using USING clause for join on same-named columns",
				Pos:              join.Span.Start,
				DocumentationURL: lint.BuildDocURL("ST07"),
				ImpactScore:      lint.ImpactLow.Int(),
				AutoFixable:      false,
			})
		}
	}
	return diagnostics
}

func canUseUsingST07(condition parser.Expr) bool {
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
