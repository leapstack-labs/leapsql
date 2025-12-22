package aliasing

import (
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/internal/ast"
	"github.com/leapstack-labs/leapsql/pkg/parser"
)

func init() {
	lint.Register(ExpressionAlias)
}

// ExpressionAlias recommends adding aliases to expression columns.
var ExpressionAlias = lint.RuleDef{
	ID:          "AL03",
	Name:        "aliasing.expression",
	Group:       "aliasing",
	Description: "Expression columns should have explicit aliases.",
	Severity:    lint.SeverityInfo,
	Check:       checkExpressionAlias,
}

func checkExpressionAlias(stmt any, _ lint.DialectInfo) []lint.Diagnostic {
	selectStmt, ok := stmt.(*parser.SelectStmt)
	if !ok {
		return nil
	}

	core := ast.GetSelectCore(selectStmt)
	if core == nil {
		return nil
	}

	var diagnostics []lint.Diagnostic
	for _, col := range core.Columns {
		// Skip if it's a star, has an alias, or is a simple column ref
		if col.Star || col.TableStar != "" || col.Alias != "" {
			continue
		}

		// Check if expression is more complex than a column ref
		switch col.Expr.(type) {
		case *parser.ColumnRef:
			// Simple column reference - no alias needed
			continue
		case *parser.FuncCall, *parser.CaseExpr, *parser.BinaryExpr, *parser.CastExpr:
			// Complex expressions should have aliases
			diagnostics = append(diagnostics, lint.Diagnostic{
				RuleID:   "AL03",
				Severity: lint.SeverityInfo,
				Message:  "Expression column should have an explicit alias for clarity",
				Pos:      core.Span.Start,
			})
		}
	}
	return diagnostics
}
