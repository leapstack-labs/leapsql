package rules

import (
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql/internal/ast"
)

func init() {
	sql.Register(ElseNull)
}

// ElseNull warns about redundant ELSE NULL in CASE expressions.
var ElseNull = sql.RuleDef{
	ID:          "ST01",
	Name:        "structure.else_null",
	Group:       "structure",
	Description: "ELSE NULL is redundant in CASE expressions.",
	Severity:    core.SeverityHint,
	Check:       checkElseNull,

	Rationale: `CASE expressions implicitly return NULL when no WHEN clause matches and no ELSE is specified. 
Writing ELSE NULL explicitly adds verbosity without changing behavior. Removing it keeps the query concise 
while maintaining the same semantics.`,

	BadExample: `SELECT
  CASE status
    WHEN 'active' THEN 1
    WHEN 'inactive' THEN 0
    ELSE NULL
  END AS status_code
FROM users`,

	GoodExample: `SELECT
  CASE status
    WHEN 'active' THEN 1
    WHEN 'inactive' THEN 0
  END AS status_code
FROM users`,

	Fix: "Remove the ELSE NULL clause from the CASE expression.",
}

func checkElseNull(stmt any, _ lint.DialectInfo, _ map[string]any) []lint.Diagnostic {
	selectStmt, ok := stmt.(*core.SelectStmt)
	if !ok {
		return nil
	}

	var diagnostics []lint.Diagnostic
	for _, caseExpr := range ast.CollectCaseExprs(selectStmt) {
		if caseExpr.Else == nil {
			continue
		}
		// Check if ELSE is NULL literal
		if lit, ok := caseExpr.Else.(*core.Literal); ok && lit.Type == core.LiteralNull {
			diagnostics = append(diagnostics, lint.Diagnostic{
				RuleID:           "ST01",
				Severity:         core.SeverityHint,
				Message:          "ELSE NULL is redundant; CASE expressions return NULL by default when no ELSE is specified",
				DocumentationURL: lint.BuildDocURL("ST01"),
				ImpactScore:      lint.ImpactLow.Int(),
				AutoFixable:      false,
			})
		}
	}
	return diagnostics
}
