package ambiguous

import (
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/internal/ast"
	"github.com/leapstack-labs/leapsql/pkg/parser"
)

func init() {
	lint.Register(OrderByLimitWithUnion)
}

// OrderByLimitWithUnion warns about ORDER BY/LIMIT ambiguity with set operations.
var OrderByLimitWithUnion = lint.RuleDef{
	ID:          "AM09",
	Name:        "ambiguous.order_by_limit",
	Group:       "ambiguous",
	Description: "ORDER BY/LIMIT with set operation may have unexpected scope.",
	Severity:    lint.SeverityWarning,
	Check:       checkOrderByLimitWithUnion,
}

func checkOrderByLimitWithUnion(stmt any, _ lint.DialectInfo, _ map[string]any) []lint.Diagnostic {
	selectStmt, ok := stmt.(*parser.SelectStmt)
	if !ok || selectStmt == nil {
		return nil
	}

	// Check if this is a set operation
	bodies := ast.CollectSelectBodies(selectStmt)
	hasSetOp := false
	for _, body := range bodies {
		if body != nil && body.Op != parser.SetOpNone {
			hasSetOp = true
			break
		}
	}
	if !hasSetOp {
		return nil
	}

	// Get the main core
	core := ast.GetSelectCore(selectStmt)
	if core == nil {
		return nil
	}

	var diagnostics []lint.Diagnostic

	// ORDER BY without parentheses applies to entire set operation
	if len(core.OrderBy) > 0 {
		diagnostics = append(diagnostics, lint.Diagnostic{
			RuleID:   "AM09",
			Severity: lint.SeverityWarning,
			Message:  "ORDER BY in set operation applies to the entire result; use parentheses if you intend to order individual queries",
		})
	}

	// LIMIT without parentheses applies to entire set operation
	if core.Limit != nil {
		diagnostics = append(diagnostics, lint.Diagnostic{
			RuleID:   "AM09",
			Severity: lint.SeverityWarning,
			Message:  "LIMIT in set operation applies to the entire result; use parentheses if you intend to limit individual queries",
		})
	}

	return diagnostics
}
