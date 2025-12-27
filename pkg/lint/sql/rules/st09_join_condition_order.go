package rules

import (
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql/internal/ast"
	"github.com/leapstack-labs/leapsql/pkg/token"
)

func init() {
	sql.Register(JoinConditionOrder)
}

// JoinConditionOrder checks that join conditions reference the left table first.
var JoinConditionOrder = sql.RuleDef{
	ID:          "ST09",
	Name:        "structure.join_condition_order",
	Group:       "structure",
	Description: "Join condition should reference left table first (e.g., a.id = b.id, not b.id = a.id).",
	Severity:    core.SeverityHint,
	Check:       checkJoinConditionOrder,

	Rationale: `Consistently ordering join conditions with the left (existing) table first improves readability. 
It follows the natural reading order of the query: FROM table_a JOIN table_b ON table_a.col = table_b.col. 
This convention makes it easier to trace relationships through the query.`,

	BadExample: `SELECT *
FROM orders o
JOIN customers c ON c.id = o.customer_id`,

	GoodExample: `SELECT *
FROM orders o
JOIN customers c ON o.customer_id = c.id`,

	Fix: "Reorder the join condition to reference the left table first.",
}

func checkJoinConditionOrder(stmt any, _ lint.DialectInfo, _ map[string]any) []lint.Diagnostic {
	selectStmt, ok := stmt.(*core.SelectStmt)
	if !ok {
		return nil
	}

	var diagnostics []lint.Diagnostic

	// Process each SelectCore to find joins
	for _, selectCore := range ast.CollectSelectCores(selectStmt) {
		if selectCore.From == nil {
			continue
		}

		// Track tables in order: FROM source first, then each joined table
		tableOrder := make([]string, 0)

		// Get the source table(s)
		sourceNames := getTableNamesST09(selectCore.From.Source)
		tableOrder = append(tableOrder, sourceNames...)

		// Process each join
		for _, join := range selectCore.From.Joins {
			// Get the right table name/alias
			rightNames := getTableNamesST09(join.Right)

			// Check the join condition
			if join.Condition != nil {
				diag := checkConditionOrderST09(join.Condition, tableOrder, rightNames, join.Span.Start)
				if diag != nil {
					diagnostics = append(diagnostics, *diag)
				}
			}

			// Add right table to the order for subsequent joins
			tableOrder = append(tableOrder, rightNames...)
		}
	}

	return diagnostics
}

// getTableNamesST09 extracts table name or alias from a TableRef.
func getTableNamesST09(ref core.TableRef) []string {
	if ref == nil {
		return nil
	}

	switch t := ref.(type) {
	case *core.TableName:
		if t.Alias != "" {
			return []string{t.Alias}
		}
		return []string{t.Name}
	case *core.DerivedTable:
		if t.Alias != "" {
			return []string{t.Alias}
		}
		return nil // Derived tables without alias can't be referenced
	case *core.LateralTable:
		if t.Alias != "" {
			return []string{t.Alias}
		}
		return nil
	}
	return nil
}

// checkConditionOrderST09 checks if the join condition has the right table column on the left side of equality.
func checkConditionOrderST09(condition core.Expr, leftTables, rightTables []string, pos token.Position) *lint.Diagnostic {
	binExpr, ok := condition.(*core.BinaryExpr)
	if !ok || binExpr.Op != token.EQ {
		return nil
	}

	leftCol, leftOk := binExpr.Left.(*core.ColumnRef)
	rightCol, rightOk := binExpr.Right.(*core.ColumnRef)

	if !leftOk || !rightOk {
		return nil
	}

	// Both columns need table qualifiers to check order
	if leftCol.Table == "" || rightCol.Table == "" {
		return nil
	}

	// Build lookup sets
	leftSet := make(map[string]bool)
	for _, t := range leftTables {
		leftSet[t] = true
	}
	rightSet := make(map[string]bool)
	for _, t := range rightTables {
		rightSet[t] = true
	}

	// Check if the expression has the right table on the left side
	// Bad: b.id = a.id (right table first)
	// Good: a.id = b.id (left table first)
	if rightSet[leftCol.Table] && leftSet[rightCol.Table] {
		return &lint.Diagnostic{
			RuleID:           "ST09",
			Severity:         core.SeverityHint,
			Message:          "Join condition should reference left table first; consider rewriting as '" + rightCol.Table + "." + rightCol.Column + " = " + leftCol.Table + "." + leftCol.Column + "'",
			Pos:              pos,
			DocumentationURL: lint.BuildDocURL("ST09"),
			ImpactScore:      lint.ImpactLow.Int(),
			AutoFixable:      false,
		}
	}

	return nil
}
