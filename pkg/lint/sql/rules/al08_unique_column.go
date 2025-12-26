package rules

import (
	"strings"

	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql/internal/ast"
	"github.com/leapstack-labs/leapsql/pkg/parser"
)

func init() {
	sql.Register(UniqueColumnAlias)
}

// UniqueColumnAlias warns about duplicate column aliases.
var UniqueColumnAlias = sql.RuleDef{
	ID:          "AL08",
	Name:        "aliasing.unique_column",
	Group:       "aliasing",
	Description: "Column aliases should be unique within SELECT clause.",
	Severity:    lint.SeverityWarning,
	Check:       checkUniqueColumnAlias,
}

func checkUniqueColumnAlias(stmt any, _ lint.DialectInfo, _ map[string]any) []lint.Diagnostic {
	selectStmt, ok := stmt.(*parser.SelectStmt)
	if !ok {
		return nil
	}

	core := ast.GetSelectCore(selectStmt)
	if core == nil {
		return nil
	}

	// Collect column aliases
	aliases := make(map[string]int)
	for _, col := range core.Columns {
		if col.Alias != "" {
			aliases[strings.ToLower(col.Alias)]++
		}
	}

	// Find duplicates
	var diagnostics []lint.Diagnostic
	for alias, count := range aliases {
		if count > 1 {
			diagnostics = append(diagnostics, lint.Diagnostic{
				RuleID:           "AL08",
				Severity:         lint.SeverityWarning,
				Message:          "Column alias '" + alias + "' is used multiple times in SELECT clause",
				Pos:              core.Span.Start,
				DocumentationURL: lint.BuildDocURL("AL08"),
				ImpactScore:      lint.ImpactMedium.Int(),
				AutoFixable:      false,
			})
		}
	}
	return diagnostics
}
