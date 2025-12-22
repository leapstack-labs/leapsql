package convention

import (
	"strings"

	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/internal/ast"
	"github.com/leapstack-labs/leapsql/pkg/parser"
)

func init() {
	lint.Register(CountStyle)
}

// CountStyle enforces consistent COUNT style (COUNT(*) vs COUNT(1)).
var CountStyle = lint.RuleDef{
	ID:          "CV04",
	Name:        "convention.count_rows",
	Group:       "convention",
	Description: "Prefer COUNT(*) over COUNT(1) for counting rows.",
	Severity:    lint.SeverityHint,
	Check:       checkCountStyle,
}

func checkCountStyle(stmt any, _ lint.DialectInfo) []lint.Diagnostic {
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
						RuleID:   "CV04",
						Severity: lint.SeverityHint,
						Message:  "Prefer COUNT(*) over COUNT(1) for counting rows",
					})
				}
			}
		}
	}
	return diagnostics
}
