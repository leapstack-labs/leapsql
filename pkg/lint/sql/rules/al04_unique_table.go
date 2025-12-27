package rules

import (
	"strings"

	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql/internal/ast"
	"github.com/leapstack-labs/leapsql/pkg/parser"
)

func init() {
	sql.Register(UniqueTableAlias)
}

// UniqueTableAlias warns about duplicate table aliases.
var UniqueTableAlias = sql.RuleDef{
	ID:          "AL04",
	Name:        "aliasing.unique_table",
	Group:       "aliasing",
	Description: "Table aliases should be unique within a query.",
	Severity:    lint.SeverityError,
	Check:       checkUniqueTableAlias,

	Rationale: `Duplicate table aliases cause ambiguity when referencing columns. Most 
databases will reject queries with duplicate aliases. Even if accepted, it makes the 
query confusing and error-prone. Each table reference should have a unique alias.`,

	BadExample: `SELECT a.id, a.name
FROM customers a
JOIN orders a ON a.customer_id = a.id`,

	GoodExample: `SELECT c.id, c.name
FROM customers c
JOIN orders o ON o.customer_id = c.id`,

	Fix: "Rename one of the duplicate aliases to be unique within the query.",
}

func checkUniqueTableAlias(stmt any, _ lint.DialectInfo, _ map[string]any) []lint.Diagnostic {
	selectStmt, ok := stmt.(*parser.SelectStmt)
	if !ok {
		return nil
	}

	// Collect all table aliases
	aliases := make(map[string]int) // alias -> count
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
			aliases[alias]++
		}
	}

	// Find duplicates
	var diagnostics []lint.Diagnostic
	for alias, count := range aliases {
		if count > 1 {
			diagnostics = append(diagnostics, lint.Diagnostic{
				RuleID:           "AL04",
				Severity:         lint.SeverityError,
				Message:          "Table alias '" + alias + "' is used " + string(rune('0'+count)) + " times; aliases must be unique",
				DocumentationURL: lint.BuildDocURL("AL04"),
				ImpactScore:      lint.ImpactCritical.Int(),
				AutoFixable:      false,
			})
		}
	}
	return diagnostics
}
