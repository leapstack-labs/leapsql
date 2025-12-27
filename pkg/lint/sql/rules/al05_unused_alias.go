package rules

import (
	"strings"

	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql/internal/ast"
	"github.com/leapstack-labs/leapsql/pkg/parser"
)

func init() {
	sql.Register(UnusedTableAlias)
}

// UnusedTableAlias warns about defined but unused table aliases.
var UnusedTableAlias = sql.RuleDef{
	ID:          "AL05",
	Name:        "aliasing.unused",
	Group:       "aliasing",
	Description: "Table alias is defined but not referenced.",
	Severity:    lint.SeverityWarning,
	Check:       checkUnusedTableAlias,

	Rationale: `Unused table aliases add noise without providing clarity. They may indicate 
incomplete refactoring or copy-paste errors. If you alias a table, use that alias 
consistently to improve query readability.`,

	BadExample: `SELECT id, name, email
FROM customers c
WHERE status = 'active'`,

	GoodExample: `SELECT c.id, c.name, c.email
FROM customers c
WHERE c.status = 'active'`,

	Fix: "Either use the alias when referencing columns from this table, or remove the alias if it's not needed.",
}

func checkUnusedTableAlias(stmt any, _ lint.DialectInfo, _ map[string]any) []lint.Diagnostic {
	selectStmt, ok := stmt.(*parser.SelectStmt)
	if !ok {
		return nil
	}

	// Collect all table aliases
	aliases := make(map[string]bool)
	for _, ref := range ast.CollectTableRefs(selectStmt) {
		var alias string
		switch t := ref.(type) {
		case *parser.TableName:
			if t.Alias != "" {
				alias = strings.ToLower(t.Alias)
			}
		case *parser.DerivedTable:
			if t.Alias != "" {
				alias = strings.ToLower(t.Alias)
			}
		case *parser.LateralTable:
			if t.Alias != "" {
				alias = strings.ToLower(t.Alias)
			}
		}
		if alias != "" {
			aliases[alias] = false // not yet referenced
		}
	}

	// Mark aliases as referenced from column refs
	for _, ref := range ast.CollectColumnRefs(selectStmt) {
		if ref.Table != "" {
			aliases[strings.ToLower(ref.Table)] = true
		}
	}

	// Find unused aliases
	var diagnostics []lint.Diagnostic
	for alias, used := range aliases {
		if !used {
			diagnostics = append(diagnostics, lint.Diagnostic{
				RuleID:           "AL05",
				Severity:         lint.SeverityWarning,
				Message:          "Table alias '" + alias + "' is defined but never referenced",
				DocumentationURL: lint.BuildDocURL("AL05"),
				ImpactScore:      lint.ImpactMedium.Int(),
				AutoFixable:      false,
			})
		}
	}
	return diagnostics
}
