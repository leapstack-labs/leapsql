package rules

import (
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql/internal/ast"
	"github.com/leapstack-labs/leapsql/pkg/parser"
)

func init() {
	sql.Register(ExpressionAlias)
}

// ExpressionAlias recommends adding aliases to expression columns.
var ExpressionAlias = sql.RuleDef{
	ID:          "AL03",
	Name:        "aliasing.expression",
	Group:       "aliasing",
	Description: "Expression columns should have explicit aliases.",
	Severity:    core.SeverityInfo,
	Check:       checkExpressionAlias,

	Rationale: `Expressions without aliases produce auto-generated column names that vary 
by database (e.g., "?column?", "expr0", "count(*)"). Explicit aliases make query results 
predictable and self-documenting, improving usability for downstream consumers.`,

	BadExample: `SELECT
    first_name || ' ' || last_name,
    UPPER(email),
    COUNT(*)
FROM users
GROUP BY 1, 2`,

	GoodExample: `SELECT
    first_name || ' ' || last_name AS full_name,
    UPPER(email) AS email_upper,
    COUNT(*) AS user_count
FROM users
GROUP BY 1, 2`,

	Fix: "Add an explicit alias using AS to give the expression a meaningful name.",
}

func checkExpressionAlias(stmt any, _ lint.DialectInfo, _ map[string]any) []lint.Diagnostic {
	selectStmt, ok := stmt.(*parser.SelectStmt)
	if !ok {
		return nil
	}

	selectCore := ast.GetSelectCore(selectStmt)
	if selectCore == nil {
		return nil
	}

	var diagnostics []lint.Diagnostic
	for _, col := range selectCore.Columns {
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
				RuleID:           "AL03",
				Severity:         core.SeverityInfo,
				Message:          "Expression column should have an explicit alias for clarity",
				Pos:              selectCore.Span.Start,
				DocumentationURL: lint.BuildDocURL("AL03"),
				ImpactScore:      lint.ImpactMedium.Int(),
				AutoFixable:      false,
			})
		}
	}
	return diagnostics
}
