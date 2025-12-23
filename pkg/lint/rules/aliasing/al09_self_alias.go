package aliasing

import (
	"strings"

	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/internal/ast"
	"github.com/leapstack-labs/leapsql/pkg/parser"
)

func init() {
	lint.Register(SelfAlias)
}

// SelfAlias warns about tables aliased to their own name.
var SelfAlias = lint.RuleDef{
	ID:          "AL09",
	Name:        "aliasing.self_alias",
	Group:       "aliasing",
	Description: "Table aliased to its own name is redundant.",
	Severity:    lint.SeverityHint,
	Check:       checkSelfAlias,
}

func checkSelfAlias(stmt any, _ lint.DialectInfo, _ map[string]any) []lint.Diagnostic {
	selectStmt, ok := stmt.(*parser.SelectStmt)
	if !ok {
		return nil
	}

	var diagnostics []lint.Diagnostic
	for _, ref := range ast.CollectTableRefs(selectStmt) {
		tn, ok := ref.(*parser.TableName)
		if !ok || tn.Alias == "" {
			continue
		}

		// Check if alias equals table name (case-insensitive)
		if strings.EqualFold(tn.Name, tn.Alias) {
			diagnostics = append(diagnostics, lint.Diagnostic{
				RuleID:   "AL09",
				Severity: lint.SeverityHint,
				Message:  "Table '" + tn.Name + "' is aliased to its own name; this is redundant",
				Pos:      tn.Span.Start,
			})
		}
	}
	return diagnostics
}
