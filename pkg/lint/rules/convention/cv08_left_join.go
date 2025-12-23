package convention

import (
	"strings"

	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/internal/ast"
	"github.com/leapstack-labs/leapsql/pkg/parser"
)

func init() {
	lint.Register(PreferLeftJoin)
}

// PreferLeftJoin recommends LEFT JOIN over RIGHT JOIN.
var PreferLeftJoin = lint.RuleDef{
	ID:          "CV08",
	Name:        "convention.left_join",
	Group:       "convention",
	Description: "Prefer LEFT JOIN over RIGHT JOIN for consistency.",
	Severity:    lint.SeverityHint,
	Check:       checkPreferLeftJoin,
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
				Severity:         lint.SeverityHint,
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
