package aliasing

import (
	"strings"

	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/internal/ast"
	"github.com/leapstack-labs/leapsql/pkg/parser"
)

func init() {
	lint.Register(AliasLength)
}

// AliasLength enforces alias length constraints.
var AliasLength = lint.RuleDef{
	ID:          "AL06",
	Name:        "aliasing.length",
	Group:       "aliasing",
	Description: "Alias length should be between min and max characters.",
	Severity:    lint.SeverityInfo,
	ConfigKeys:  []string{"min_length", "max_length"},
	Check:       checkAliasLength,
}

const (
	defaultMinLength = 1
	defaultMaxLength = 30
)

func checkAliasLength(stmt any, _ lint.DialectInfo) []lint.Diagnostic {
	selectStmt, ok := stmt.(*parser.SelectStmt)
	if !ok {
		return nil
	}

	minLen := defaultMinLength
	maxLen := defaultMaxLength

	var diagnostics []lint.Diagnostic

	// Check table aliases
	for _, ref := range ast.CollectTableRefs(selectStmt) {
		var alias string
		switch t := ref.(type) {
		case *parser.TableName:
			alias = t.Alias
		case *parser.DerivedTable:
			alias = t.Alias
		case *parser.LateralTable:
			alias = t.Alias
		}
		if alias != "" {
			if len(alias) < minLen {
				diagnostics = append(diagnostics, lint.Diagnostic{
					RuleID:   "AL06",
					Severity: lint.SeverityInfo,
					Message:  "Table alias '" + alias + "' is too short; minimum length is " + string(rune('0'+minLen)),
				})
			}
			if len(alias) > maxLen {
				diagnostics = append(diagnostics, lint.Diagnostic{
					RuleID:   "AL06",
					Severity: lint.SeverityInfo,
					Message:  "Table alias '" + alias + "' is too long; maximum length is " + string(rune('0'+maxLen)),
				})
			}
		}
	}

	// Check column aliases
	core := ast.GetSelectCore(selectStmt)
	if core != nil {
		for _, col := range core.Columns {
			if col.Alias != "" {
				alias := strings.TrimSpace(col.Alias)
				if len(alias) < minLen {
					diagnostics = append(diagnostics, lint.Diagnostic{
						RuleID:   "AL06",
						Severity: lint.SeverityInfo,
						Message:  "Column alias '" + alias + "' is too short",
					})
				}
				if len(alias) > maxLen {
					diagnostics = append(diagnostics, lint.Diagnostic{
						RuleID:   "AL06",
						Severity: lint.SeverityInfo,
						Message:  "Column alias '" + alias + "' is too long",
					})
				}
			}
		}
	}

	return diagnostics
}
