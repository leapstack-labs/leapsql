package rules

import (
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql/internal/ast"
	"github.com/leapstack-labs/leapsql/pkg/parser"
)

func init() {
	sql.Register(JoinConditionTables)
}

// JoinConditionTables warns when join condition doesn't reference both tables.
var JoinConditionTables = sql.RuleDef{
	ID:          "AM08",
	Name:        "ambiguous.join_condition",
	Group:       "ambiguous",
	Description: "Join condition should reference both tables being joined.",
	Severity:    lint.SeverityWarning,
	Check:       checkJoinConditionTables,

	Rationale: `A JOIN condition that doesn't reference the joined table is likely a bug. 
It effectively creates a cross join filtered by the condition, which is rarely intended. 
Each JOIN's ON clause should reference both the preceding and joining tables.`,

	BadExample: `SELECT c.name, o.total
FROM customers c
JOIN orders o ON c.status = 'active'`,

	GoodExample: `SELECT c.name, o.total
FROM customers c
JOIN orders o ON o.customer_id = c.id`,

	Fix: "Ensure the JOIN condition references columns from both the left and right tables.",
}

func checkJoinConditionTables(stmt any, _ lint.DialectInfo, _ map[string]any) []lint.Diagnostic {
	selectStmt, ok := stmt.(*parser.SelectStmt)
	if !ok {
		return nil
	}

	var diagnostics []lint.Diagnostic
	for _, join := range ast.CollectJoins(selectStmt) {
		// Skip joins without conditions (CROSS JOIN, NATURAL JOIN, USING)
		if join.Condition == nil || join.Natural || len(join.Using) > 0 {
			continue
		}

		// Get table aliases/names involved in this join
		rightTable := getTableNameAM08(join.Right)
		if rightTable == "" {
			continue
		}

		// Check if the condition references the right table
		refs := collectColumnRefsInExprAM08(join.Condition)
		hasRightRef := false
		for _, ref := range refs {
			if ref.Table == rightTable {
				hasRightRef = true
				break
			}
		}

		if !hasRightRef && len(refs) > 0 {
			diagnostics = append(diagnostics, lint.Diagnostic{
				RuleID:           "AM08",
				Severity:         lint.SeverityWarning,
				Message:          "Join condition does not appear to reference the joined table '" + rightTable + "'",
				Pos:              join.Span.Start,
				DocumentationURL: lint.BuildDocURL("AM08"),
				ImpactScore:      lint.ImpactHigh.Int(),
				AutoFixable:      false,
			})
		}
	}
	return diagnostics
}

func getTableNameAM08(ref parser.TableRef) string {
	switch t := ref.(type) {
	case *parser.TableName:
		if t.Alias != "" {
			return t.Alias
		}
		return t.Name
	case *parser.DerivedTable:
		return t.Alias
	case *parser.LateralTable:
		return t.Alias
	default:
		return ""
	}
}

func collectColumnRefsInExprAM08(expr parser.Expr) []*parser.ColumnRef {
	var refs []*parser.ColumnRef
	ast.Walk(expr, func(node any) bool {
		if cr, ok := node.(*parser.ColumnRef); ok {
			refs = append(refs, cr)
		}
		return true
	})
	return refs
}
