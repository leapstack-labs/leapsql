package rules

import (
	"strings"

	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql/internal/ast"
	"github.com/leapstack-labs/leapsql/pkg/parser"
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
	Severity:    lint.SeverityHint,
	Check:       checkPreferCoalesce,
}

func checkPreferCoalesce(stmt any, _ lint.DialectInfo, _ map[string]any) []lint.Diagnostic {
	selectStmt, ok := stmt.(*parser.SelectStmt)
	if !ok {
		return nil
	}

	var diagnostics []lint.Diagnostic
	for _, fn := range ast.CollectFuncCalls(selectStmt) {
		name := strings.ToUpper(fn.Name)
		if name == "IFNULL" || name == "NVL" {
			diagnostics = append(diagnostics, lint.Diagnostic{
				RuleID:           "CV02",
				Severity:         lint.SeverityHint,
				Message:          "Prefer COALESCE over " + name + " for better SQL portability",
				DocumentationURL: lint.BuildDocURL("CV02"),
				ImpactScore:      lint.ImpactLow.Int(),
				AutoFixable:      false,
			})
		}
	}
	return diagnostics
}
