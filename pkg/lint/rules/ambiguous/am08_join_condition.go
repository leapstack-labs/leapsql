package ambiguous

import (
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/internal/ast"
	"github.com/leapstack-labs/leapsql/pkg/parser"
)

func init() {
	lint.Register(JoinConditionTables)
}

// JoinConditionTables warns when join condition doesn't reference both tables.
var JoinConditionTables = lint.RuleDef{
	ID:          "AM08",
	Name:        "ambiguous.join_condition",
	Group:       "ambiguous",
	Description: "Join condition should reference both tables being joined.",
	Severity:    lint.SeverityWarning,
	Check:       checkJoinConditionTables,
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
		rightTable := getTableName(join.Right)
		if rightTable == "" {
			continue
		}

		// Check if the condition references the right table
		refs := collectColumnRefsInExpr(join.Condition)
		hasRightRef := false
		for _, ref := range refs {
			if ref.Table == rightTable {
				hasRightRef = true
				break
			}
		}

		if !hasRightRef && len(refs) > 0 {
			diagnostics = append(diagnostics, lint.Diagnostic{
				RuleID:   "AM08",
				Severity: lint.SeverityWarning,
				Message:  "Join condition does not appear to reference the joined table '" + rightTable + "'",
				Pos:      join.Span.Start,
			})
		}
	}
	return diagnostics
}

func getTableName(ref parser.TableRef) string {
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

func collectColumnRefsInExpr(expr parser.Expr) []*parser.ColumnRef {
	var refs []*parser.ColumnRef
	ast.Walk(expr, func(node any) bool {
		if cr, ok := node.(*parser.ColumnRef); ok {
			refs = append(refs, cr)
		}
		return true
	})
	return refs
}
