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
	sql.Register(UniqueColumnAlias)
}

// UniqueColumnAlias warns about duplicate column aliases.
var UniqueColumnAlias = sql.RuleDef{
	ID:          "AL08",
	Name:        "aliasing.unique_column",
	Group:       "aliasing",
	Description: "Column aliases should be unique within SELECT clause.",
	Severity:    core.SeverityWarning,
	Check:       checkUniqueColumnAlias,

	Rationale: `Duplicate column aliases create ambiguity in the result set. Downstream 
consumers (reports, APIs, other queries) may not be able to reliably reference the 
correct column. Some databases will error, others will silently pick one column.`,

	BadExample: `SELECT
    first_name AS name,
    last_name AS name,
    company_name AS name
FROM contacts`,

	GoodExample: `SELECT
    first_name AS first_name,
    last_name AS last_name,
    company_name AS company_name
FROM contacts`,

	Fix: "Rename column aliases to be unique within the SELECT clause.",
}

func checkUniqueColumnAlias(stmt any, _ lint.DialectInfo, _ map[string]any) []lint.Diagnostic {
	selectStmt, ok := stmt.(*parser.SelectStmt)
	if !ok {
		return nil
	}

	selectCore := ast.GetSelectCore(selectStmt)
	if selectCore == nil {
		return nil
	}

	// Collect column aliases
	aliases := make(map[string]int)
	for _, col := range selectCore.Columns {
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
				Severity:         core.SeverityWarning,
				Message:          "Column alias '" + alias + "' is used multiple times in SELECT clause",
				Pos:              selectCore.Span.Start,
				DocumentationURL: lint.BuildDocURL("AL08"),
				ImpactScore:      lint.ImpactMedium.Int(),
				AutoFixable:      false,
			})
		}
	}
	return diagnostics
}
