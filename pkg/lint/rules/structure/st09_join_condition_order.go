package structure

import (
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/internal/ast"
	"github.com/leapstack-labs/leapsql/pkg/parser"
	"github.com/leapstack-labs/leapsql/pkg/token"
)

func init() {
	lint.Register(JoinConditionOrder)
}

// JoinConditionOrder checks that join conditions reference the left table first.
var JoinConditionOrder = lint.RuleDef{
	ID:          "ST09",
	Name:        "structure.join_condition_order",
	Group:       "structure",
	Description: "Join condition should reference left table first (e.g., a.id = b.id, not b.id = a.id).",
	Severity:    lint.SeverityHint,
	Check:       checkJoinConditionOrder,
}

func checkJoinConditionOrder(stmt any, _ lint.DialectInfo, _ map[string]any) []lint.Diagnostic {
	selectStmt, ok := stmt.(*parser.SelectStmt)
	if !ok {
		return nil
	}

	var diagnostics []lint.Diagnostic

	// Process each SelectCore to find joins
	for _, core := range ast.CollectSelectCores(selectStmt) {
		if core.From == nil {
			continue
		}

		// Track tables in order: FROM source first, then each joined table
		tableOrder := make([]string, 0)

		// Get the source table(s)
		sourceNames := getTableNames(core.From.Source)
		tableOrder = append(tableOrder, sourceNames...)

		// Process each join
		for _, join := range core.From.Joins {
			// Get the right table name/alias
			rightNames := getTableNames(join.Right)

			// Check the join condition
			if join.Condition != nil {
				diag := checkConditionOrder(join.Condition, tableOrder, rightNames, join.Span.Start)
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

// getTableNames extracts table name or alias from a TableRef.
func getTableNames(ref parser.TableRef) []string {
	if ref == nil {
		return nil
	}

	switch t := ref.(type) {
	case *parser.TableName:
		if t.Alias != "" {
			return []string{t.Alias}
		}
		return []string{t.Name}
	case *parser.DerivedTable:
		if t.Alias != "" {
			return []string{t.Alias}
		}
		return nil // Derived tables without alias can't be referenced
	case *parser.LateralTable:
		if t.Alias != "" {
			return []string{t.Alias}
		}
		return nil
	}
	return nil
}

// checkConditionOrder checks if the join condition has the right table column on the left side of equality.
func checkConditionOrder(condition parser.Expr, leftTables, rightTables []string, pos token.Position) *lint.Diagnostic {
	binExpr, ok := condition.(*parser.BinaryExpr)
	if !ok || binExpr.Op != token.EQ {
		return nil
	}

	leftCol, leftOk := binExpr.Left.(*parser.ColumnRef)
	rightCol, rightOk := binExpr.Right.(*parser.ColumnRef)

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
			RuleID:   "ST09",
			Severity: lint.SeverityHint,
			Message:  "Join condition should reference left table first; consider rewriting as '" + rightCol.Table + "." + rightCol.Column + " = " + leftCol.Table + "." + leftCol.Column + "'",
			Pos:      pos,
		}
	}

	return nil
}
