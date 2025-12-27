package rules

import (
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql/internal/ast"
	"github.com/leapstack-labs/leapsql/pkg/parser"
)

func init() {
	sql.Register(ImplicitJoin)
}

// ImplicitJoin warns about comma-separated tables (implicit cross join).
var ImplicitJoin = sql.RuleDef{
	ID:          "AM05",
	Name:        "ambiguous.join",
	Group:       "ambiguous",
	Description: "Comma-separated tables create an implicit cross join.",
	Severity:    lint.SeverityInfo,
	Check:       checkImplicitJoin,

	Rationale: `The old-style comma join syntax (FROM a, b WHERE a.id = b.id) is harder 
to read than explicit JOIN syntax. It's easy to accidentally create a cross join 
by forgetting the WHERE condition. Explicit JOINs make intent clear.`,

	BadExample: `SELECT c.name, o.total
FROM customers c, orders o
WHERE c.id = o.customer_id`,

	GoodExample: `SELECT c.name, o.total
FROM customers c
JOIN orders o ON c.id = o.customer_id`,

	Fix: "Replace comma-separated tables with explicit JOIN syntax (INNER JOIN, LEFT JOIN, etc.).",
}

func checkImplicitJoin(stmt any, _ lint.DialectInfo, _ map[string]any) []lint.Diagnostic {
	selectStmt, ok := stmt.(*parser.SelectStmt)
	if !ok {
		return nil
	}

	var diagnostics []lint.Diagnostic
	for _, join := range ast.CollectJoins(selectStmt) {
		if join.Type == parser.JoinComma {
			diagnostics = append(diagnostics, lint.Diagnostic{
				RuleID:           "AM05",
				Severity:         lint.SeverityInfo,
				Message:          "Comma-separated tables create an implicit cross join; consider using explicit JOIN syntax",
				Pos:              join.Span.Start,
				DocumentationURL: lint.BuildDocURL("AM05"),
				ImpactScore:      lint.ImpactMedium.Int(),
				AutoFixable:      false,
			})
		}
	}
	return diagnostics
}
