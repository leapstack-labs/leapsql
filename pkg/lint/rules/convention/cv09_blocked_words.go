package convention

import (
	"strings"

	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/internal/ast"
	"github.com/leapstack-labs/leapsql/pkg/parser"
)

func init() {
	lint.Register(BlockedWords)
}

// BlockedWords warns about dangerous SQL keywords.
var BlockedWords = lint.RuleDef{
	ID:          "CV09",
	Name:        "convention.blocked_words",
	Group:       "convention",
	Description: "Block dangerous SQL keywords like DELETE, DROP, TRUNCATE.",
	Severity:    lint.SeverityWarning,
	ConfigKeys:  []string{"blocked_words"},
	Check:       checkBlockedWords,
}

// Default blocked words
var defaultBlockedWords = map[string]bool{
	"DELETE":   true,
	"DROP":     true,
	"TRUNCATE": true,
}

func checkBlockedWords(stmt any, _ lint.DialectInfo) []lint.Diagnostic {
	selectStmt, ok := stmt.(*parser.SelectStmt)
	if !ok {
		return nil
	}

	// Currently we can only detect blocked words as function calls
	// or table names (for detecting dangerous keywords in CTEs etc.)
	var diagnostics []lint.Diagnostic

	// Check function names
	for _, fn := range ast.CollectFuncCalls(selectStmt) {
		if defaultBlockedWords[strings.ToUpper(fn.Name)] {
			diagnostics = append(diagnostics, lint.Diagnostic{
				RuleID:   "CV09",
				Severity: lint.SeverityWarning,
				Message:  "Use of blocked word '" + strings.ToUpper(fn.Name) + "' detected",
			})
		}
	}

	return diagnostics
}
