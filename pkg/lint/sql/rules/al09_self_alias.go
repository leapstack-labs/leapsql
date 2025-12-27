package rules

import (
	"strings"

	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql/internal/ast"
)

func init() {
	sql.Register(SelfAlias)
}

// SelfAlias warns about tables aliased to their own name.
var SelfAlias = sql.RuleDef{
	ID:          "AL09",
	Name:        "aliasing.self_alias",
	Group:       "aliasing",
	Description: "Table aliased to its own name is redundant.",
	Severity:    core.SeverityHint,
	Check:       checkSelfAlias,

	Rationale: `Aliasing a table to its own name (e.g., customers AS customers) adds 
verbosity without any benefit. It may indicate copy-paste errors or incomplete 
refactoring. Either use a shorter alias or remove the redundant alias entirely.`,

	BadExample: `SELECT customers.id, customers.name
FROM customers AS customers
WHERE customers.status = 'active'`,

	GoodExample: `SELECT customers.id, customers.name
FROM customers
WHERE customers.status = 'active'`,

	Fix: "Remove the redundant alias, or use a shorter meaningful alias if abbreviation is desired.",
}

func checkSelfAlias(stmt any, _ lint.DialectInfo, _ map[string]any) []lint.Diagnostic {
	selectStmt, ok := stmt.(*core.SelectStmt)
	if !ok {
		return nil
	}

	var diagnostics []lint.Diagnostic
	for _, ref := range ast.CollectTableRefs(selectStmt) {
		tn, ok := ref.(*core.TableName)
		if !ok || tn.Alias == "" {
			continue
		}

		// Check if alias equals table name (case-insensitive)
		if strings.EqualFold(tn.Name, tn.Alias) {
			diagnostics = append(diagnostics, lint.Diagnostic{
				RuleID:           "AL09",
				Severity:         core.SeverityHint,
				Message:          "Table '" + tn.Name + "' is aliased to its own name; this is redundant",
				Pos:              tn.Span.Start,
				DocumentationURL: lint.BuildDocURL("AL09"),
				ImpactScore:      lint.ImpactLow.Int(),
				AutoFixable:      false,
			})
		}
	}
	return diagnostics
}
