package rules

import (
	"strings"

	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql/internal/ast"
	"github.com/leapstack-labs/leapsql/pkg/parser"
)

func init() {
	sql.Register(PreferLeftJoin)
}

// PreferLeftJoin recommends LEFT JOIN over RIGHT JOIN.
var PreferLeftJoin = sql.RuleDef{
	ID:          "CV08",
	Name:        "convention.left_join",
	Group:       "convention",
	Description: "Prefer LEFT JOIN over RIGHT JOIN for consistency.",
	Severity:    core.SeverityHint,
	Check:       checkPreferLeftJoin,

	Rationale: `LEFT JOIN is more intuitive because it preserves all rows from the table 
you naturally read first (left to right). RIGHT JOIN can always be rewritten as 
LEFT JOIN by swapping table order. Consistently using LEFT JOIN improves readability.`,

	BadExample: `SELECT o.id, c.name
FROM orders o
RIGHT JOIN customers c ON c.id = o.customer_id`,

	GoodExample: `SELECT o.id, c.name
FROM customers c
LEFT JOIN orders o ON o.customer_id = c.id`,

	Fix: "Swap the table order and use LEFT JOIN instead of RIGHT JOIN.",
}

func checkPreferLeftJoin(stmt any, _ lint.DialectInfo, _ map[string]any) []lint.Diagnostic {
	selectStmt, ok := stmt.(*parser.SelectStmt)
	if !ok {
		return nil
	}

	var diagnostics []lint.Diagnostic
	for _, join := range ast.CollectJoins(selectStmt) {
		if strings.ToUpper(string(join.Type)) == "RIGHT" {
			diagnostics = append(diagnostics, lint.Diagnostic{
				RuleID:           "CV08",
				Severity:         core.SeverityHint,
				Message:          "Consider using LEFT JOIN instead of RIGHT JOIN for better readability",
				Pos:              join.Span.Start,
				DocumentationURL: lint.BuildDocURL("CV08"),
				ImpactScore:      lint.ImpactLow.Int(),
				AutoFixable:      false,
			})
		}
	}
	return diagnostics
}
