package rules

import (
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql/internal/ast"
)

func init() {
	sql.Register(OrderByLimitWithUnion)
}

// OrderByLimitWithUnion warns about ORDER BY/LIMIT ambiguity with set operations.
var OrderByLimitWithUnion = sql.RuleDef{
	ID:          "AM09",
	Name:        "ambiguous.order_by_limit",
	Group:       "ambiguous",
	Description: "ORDER BY/LIMIT with set operation may have unexpected scope.",
	Severity:    core.SeverityWarning,
	Check:       checkOrderByLimitWithUnion,

	Rationale: `In set operations, ORDER BY and LIMIT without parentheses apply to the 
entire combined result, not individual queries. This behavior may be surprising. 
Use parentheses to make the intended scope explicit.`,

	BadExample: `SELECT name FROM customers
UNION ALL
SELECT name FROM suppliers
ORDER BY name
LIMIT 10`,

	GoodExample: `-- To order/limit the final result:
(SELECT name FROM customers
UNION ALL
SELECT name FROM suppliers)
ORDER BY name
LIMIT 10

-- To order/limit individual queries:
(SELECT name FROM customers ORDER BY name LIMIT 10)
UNION ALL
(SELECT name FROM suppliers ORDER BY name LIMIT 10)`,

	Fix: "Use parentheses to clarify whether ORDER BY/LIMIT applies to individual queries or the combined result.",
}

func checkOrderByLimitWithUnion(stmt any, _ lint.DialectInfo, _ map[string]any) []lint.Diagnostic {
	selectStmt, ok := stmt.(*core.SelectStmt)
	if !ok || selectStmt == nil {
		return nil
	}

	// Check if this is a set operation
	bodies := ast.CollectSelectBodies(selectStmt)
	hasSetOp := false
	for _, body := range bodies {
		if body != nil && body.Op != core.SetOpNone {
			hasSetOp = true
			break
		}
	}
	if !hasSetOp {
		return nil
	}

	// Get the main core
	selectCore := ast.GetSelectCore(selectStmt)
	if selectCore == nil {
		return nil
	}

	var diagnostics []lint.Diagnostic

	// ORDER BY without parentheses applies to entire set operation
	if len(selectCore.OrderBy) > 0 {
		diagnostics = append(diagnostics, lint.Diagnostic{
			RuleID:           "AM09",
			Severity:         core.SeverityWarning,
			Message:          "ORDER BY in set operation applies to the entire result; use parentheses if you intend to order individual queries",
			DocumentationURL: lint.BuildDocURL("AM09"),
			ImpactScore:      lint.ImpactMedium.Int(),
			AutoFixable:      false,
		})
	}

	// LIMIT without parentheses applies to entire set operation
	if selectCore.Limit != nil {
		diagnostics = append(diagnostics, lint.Diagnostic{
			RuleID:           "AM09",
			Severity:         core.SeverityWarning,
			Message:          "LIMIT in set operation applies to the entire result; use parentheses if you intend to limit individual queries",
			DocumentationURL: lint.BuildDocURL("AM09"),
			ImpactScore:      lint.ImpactMedium.Int(),
			AutoFixable:      false,
		})
	}

	return diagnostics
}
