package rules

import (
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql/internal/ast"
	"github.com/leapstack-labs/leapsql/pkg/parser"
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
	Severity:    lint.SeverityHint,
	Check:       checkElseNull,
}

func checkElseNull(stmt any, _ lint.DialectInfo, _ map[string]any) []lint.Diagnostic {
	selectStmt, ok := stmt.(*parser.SelectStmt)
	if !ok {
		return nil
	}

	var diagnostics []lint.Diagnostic
	for _, caseExpr := range ast.CollectCaseExprs(selectStmt) {
		if caseExpr.Else == nil {
			continue
		}
		// Check if ELSE is NULL literal
		if lit, ok := caseExpr.Else.(*parser.Literal); ok && lit.Type == parser.LiteralNull {
			diagnostics = append(diagnostics, lint.Diagnostic{
				RuleID:           "ST01",
				Severity:         lint.SeverityHint,
				Message:          "ELSE NULL is redundant; CASE expressions return NULL by default when no ELSE is specified",
				DocumentationURL: lint.BuildDocURL("ST01"),
				ImpactScore:      lint.ImpactLow.Int(),
				AutoFixable:      false,
			})
		}
	}
	return diagnostics
}
