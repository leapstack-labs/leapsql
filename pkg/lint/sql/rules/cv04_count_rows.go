package rules

import (
	"strings"

	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql/internal/ast"
)

func init() {
	sql.Register(CountStyle)
}

// CountStyle enforces consistent COUNT style (COUNT(*) vs COUNT(1)).
var CountStyle = sql.RuleDef{
	ID:          "CV04",
	Name:        "convention.count_rows",
	Group:       "convention",
	Description: "Prefer COUNT(*) over COUNT(1) for counting rows.",
	Severity:    core.SeverityHint,
	Check:       checkCountStyle,

	Rationale: `COUNT(*) is the standard and most readable way to count rows. COUNT(1) 
achieves the same result but is less intuitive. Modern query optimizers treat them 
identically, so there's no performance benefit to COUNT(1). Use COUNT(*) for clarity.`,

	BadExample: `SELECT
    department,
    COUNT(1) AS employee_count
FROM employees
GROUP BY department`,

	GoodExample: `SELECT
    department,
    COUNT(*) AS employee_count
FROM employees
GROUP BY department`,

	Fix: "Replace COUNT(1) with COUNT(*) for counting rows.",
}

func checkCountStyle(stmt any, _ lint.DialectInfo, _ map[string]any) []lint.Diagnostic {
	selectStmt, ok := stmt.(*core.SelectStmt)
	if !ok {
		return nil
	}

	var diagnostics []lint.Diagnostic
	for _, fn := range ast.CollectFuncCalls(selectStmt) {
		if strings.ToUpper(fn.Name) != "COUNT" || fn.Star {
			continue
		}
		// Check if COUNT(1) pattern
		if len(fn.Args) == 1 {
			if lit, ok := fn.Args[0].(*core.Literal); ok {
				if lit.Type == core.LiteralNumber && lit.Value == "1" {
					diagnostics = append(diagnostics, lint.Diagnostic{
						RuleID:           "CV04",
						Severity:         core.SeverityHint,
						Message:          "Prefer COUNT(*) over COUNT(1) for counting rows",
						DocumentationURL: lint.BuildDocURL("CV04"),
						ImpactScore:      lint.ImpactLow.Int(),
						AutoFixable:      false,
					})
				}
			}
		}
	}
	return diagnostics
}
