package aliasing

import (
	"strings"

	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/internal/ast"
	"github.com/leapstack-labs/leapsql/pkg/parser"
)

func init() {
	lint.Register(UniqueTableAlias)
}

// UniqueTableAlias warns about duplicate table aliases.
var UniqueTableAlias = lint.RuleDef{
	ID:          "AL04",
	Name:        "aliasing.unique_table",
	Group:       "aliasing",
	Description: "Table aliases should be unique within a query.",
	Severity:    lint.SeverityError,
	Check:       checkUniqueTableAlias,
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
