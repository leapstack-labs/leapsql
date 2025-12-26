package rules

import (
	"strings"

	"github.com/leapstack-labs/leapsql/pkg/lint"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql"
	"github.com/leapstack-labs/leapsql/pkg/lint/sql/internal/ast"
	"github.com/leapstack-labs/leapsql/pkg/parser"
)

func init() {
	sql.Register(BlockedWords)
}

// BlockedWords warns about dangerous SQL keywords.
var BlockedWords = sql.RuleDef{
	ID:          "CV09",
	Name:        "convention.blocked_words",
	Group:       "convention",
	Description: "Block dangerous SQL keywords like DELETE, DROP, TRUNCATE.",
	Severity:    lint.SeverityWarning,
	ConfigKeys:  []string{"blocked_words"},
	Check:       checkBlockedWords,
}

// Default blocked words
var defaultBlockedWordsCV09 = map[string]bool{
	"DELETE":   true,
	"DROP":     true,
	"TRUNCATE": true,
}

func checkBlockedWords(stmt any, _ lint.DialectInfo, opts map[string]any) []lint.Diagnostic {
	selectStmt, ok := stmt.(*parser.SelectStmt)
	if !ok {
		return nil
	}

	// Build blocked words set from config or use defaults
	blocked := defaultBlockedWordsCV09
	if configWords := lint.GetStringSliceOption(opts, "blocked_words", nil); len(configWords) > 0 {
		blocked = make(map[string]bool)
		for _, w := range configWords {
			blocked[strings.ToUpper(w)] = true
		}
	}

	// Currently we can only detect blocked words as function calls
	// or table names (for detecting dangerous keywords in CTEs etc.)
	var diagnostics []lint.Diagnostic

	// Check function names
	for _, fn := range ast.CollectFuncCalls(selectStmt) {
		if blocked[strings.ToUpper(fn.Name)] {
			diagnostics = append(diagnostics, lint.Diagnostic{
				RuleID:           "CV09",
				Severity:         lint.SeverityWarning,
				Message:          "Use of blocked word '" + strings.ToUpper(fn.Name) + "' detected",
				DocumentationURL: lint.BuildDocURL("CV09"),
				ImpactScore:      lint.ImpactHigh.Int(),
				AutoFixable:      false,
			})
		}
	}

	return diagnostics
}
