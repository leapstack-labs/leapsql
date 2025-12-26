package rules

import (
	"strings"

	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql/internal/ast"
	"github.com/leapstack-labs/leapsql/pkg/parser"
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
	Severity:    lint.SeverityHint,
	Check:       checkCountStyle,
}

func checkCountStyle(stmt any, _ lint.DialectInfo, _ map[string]any) []lint.Diagnostic {
	selectStmt, ok := stmt.(*parser.SelectStmt)
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
			if lit, ok := fn.Args[0].(*parser.Literal); ok {
				if lit.Type == parser.LiteralNumber && lit.Value == "1" {
					diagnostics = append(diagnostics, lint.Diagnostic{
						RuleID:           "CV04",
						Severity:         lint.SeverityHint,
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
