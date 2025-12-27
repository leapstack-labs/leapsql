package rules

import (
	"strings"

	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql/internal/ast"
)

func init() {
	sql.Register(PreferCoalesce)
}

// PreferCoalesce recommends COALESCE over IFNULL/NVL.
var PreferCoalesce = sql.RuleDef{
	ID:          "CV02",
	Name:        "convention.coalesce",
	Group:       "convention",
	Description: "Prefer COALESCE over IFNULL/NVL for better portability.",
	Severity:    core.SeverityHint,
	Check:       checkPreferCoalesce,

	Rationale: `COALESCE is ANSI SQL standard and works across all major databases. IFNULL 
(MySQL) and NVL (Oracle) are database-specific. Using COALESCE improves query 
portability and is more flexible as it can handle multiple arguments.`,

	BadExample: `SELECT
    IFNULL(phone, 'N/A') AS phone,
    NVL(email, 'unknown') AS email
FROM contacts`,

	GoodExample: `SELECT
    COALESCE(phone, 'N/A') AS phone,
    COALESCE(email, 'unknown') AS email
FROM contacts`,

	Fix: "Replace IFNULL or NVL with COALESCE for better SQL portability.",
}

func checkPreferCoalesce(stmt any, _ lint.DialectInfo, _ map[string]any) []lint.Diagnostic {
	selectStmt, ok := stmt.(*core.SelectStmt)
	if !ok {
		return nil
	}

	var diagnostics []lint.Diagnostic
	for _, fn := range ast.CollectFuncCalls(selectStmt) {
		name := strings.ToUpper(fn.Name)
		if name == "IFNULL" || name == "NVL" {
			diagnostics = append(diagnostics, lint.Diagnostic{
				RuleID:           "CV02",
				Severity:         core.SeverityHint,
				Message:          "Prefer COALESCE over " + name + " for better SQL portability",
				DocumentationURL: lint.BuildDocURL("CV02"),
				ImpactScore:      lint.ImpactLow.Int(),
				AutoFixable:      false,
			})
		}
	}
	return diagnostics
}
