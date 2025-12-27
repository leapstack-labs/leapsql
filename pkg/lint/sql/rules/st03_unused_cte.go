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
	sql.Register(UnusedCTE)
}

// UnusedCTE warns about CTEs that are defined but never used.
var UnusedCTE = sql.RuleDef{
	ID:          "ST03",
	Name:        "structure.unused_cte",
	Group:       "structure",
	Description: "CTE is defined but never referenced.",
	Severity:    core.SeverityWarning,
	Check:       checkUnusedCTE,

	Rationale: `Unused CTEs add complexity without benefit. They consume mental overhead 
for readers trying to understand the query, and may indicate incomplete refactoring 
or copy-paste errors. Removing them improves query clarity.`,

	BadExample: `WITH unused_cte AS (
    SELECT * FROM orders
),
active_customers AS (
    SELECT * FROM customers WHERE active = true
)
SELECT * FROM active_customers`,

	GoodExample: `WITH active_customers AS (
    SELECT * FROM customers WHERE active = true
)
SELECT * FROM active_customers`,

	Fix: "Remove the unused CTE definition, or reference it in your query if it was intended to be used.",
}

func checkUnusedCTE(stmt any, _ lint.DialectInfo, _ map[string]any) []lint.Diagnostic {
	selectStmt, ok := stmt.(*parser.SelectStmt)
	if !ok || selectStmt.With == nil {
		return nil
	}

	// Collect all CTE names
	cteNames := make(map[string]*parser.CTE)
	for _, cte := range selectStmt.With.CTEs {
		cteNames[strings.ToLower(cte.Name)] = cte
	}

	// Collect all referenced table names
	usedTables := make(map[string]bool)
	collectTableNamesST03(selectStmt.Body, usedTables)

	// Also check for references within CTEs to other CTEs
	for _, cte := range selectStmt.With.CTEs {
		collectTableNamesST03(cte.Select.Body, usedTables)
	}

	// Find unused CTEs
	var diagnostics []lint.Diagnostic
	for _, cte := range selectStmt.With.CTEs {
		if !usedTables[strings.ToLower(cte.Name)] {
			diagnostics = append(diagnostics, lint.Diagnostic{
				RuleID:           "ST03",
				Severity:         core.SeverityWarning,
				Message:          "CTE '" + cte.Name + "' is defined but never referenced",
				Pos:              cte.Span.Start,
				DocumentationURL: lint.BuildDocURL("ST03"),
				ImpactScore:      lint.ImpactMedium.Int(),
				AutoFixable:      false,
			})
		}
	}
	return diagnostics
}

func collectTableNamesST03(body *parser.SelectBody, used map[string]bool) {
	if body == nil {
		return
	}

	ast.Walk(body, func(node any) bool {
		if tn, ok := node.(*parser.TableName); ok {
			used[strings.ToLower(tn.Name)] = true
		}
		return true
	})
}
