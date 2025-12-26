package sql

import (
	"github.com/leapstack-labs/leapsql/pkg/lint"
)

// Analyzer runs SQL lint rules against parsed statements.
type Analyzer struct {
	config  *lint.Config
	dialect string // Filter rules by dialect (empty = all)
}

// NewAnalyzer creates a new SQL analyzer with optional configuration.
func NewAnalyzer(config *lint.Config, dialect string) *Analyzer {
	if config == nil {
		config = lint.NewConfig()
	}
	return &Analyzer{
		config:  config,
		dialect: dialect,
	}
}

// Analyze runs all registered SQL rules against the statement.
// The stmt parameter should be *parser.SelectStmt.
func (a *Analyzer) Analyze(stmt any, dialect lint.DialectInfo) []lint.Diagnostic {
	if stmt == nil {
		return nil
	}

	var diagnostics []lint.Diagnostic
	dialectName := a.dialect
	if dialectName == "" && dialect != nil {
		dialectName = dialect.GetName()
	}

	var rules []lint.SQLRule
	if dialectName != "" {
		rules = lint.GetSQLRulesByDialect(dialectName)
	} else {
		rules = lint.GetAllSQLRules()
	}

	for _, rule := range rules {
		// Skip disabled rules
		if a.config.IsDisabled(rule.ID()) {
			continue
		}

		// Check dialect filter (redundant if we filtered above, but safe)
		dialects := rule.Dialects()
		if len(dialects) > 0 && dialectName != "" && !containsDialect(dialects, dialectName) {
			continue
		}

		// Get rule-specific options
		opts := a.config.GetRuleOptions(rule.ID())

		// Run the rule with options
		diags := rule.CheckSQL(stmt, dialect, opts)

		// Apply severity overrides
		for i := range diags {
			diags[i].Severity = a.config.GetSeverity(rule.ID(), diags[i].Severity)
		}

		diagnostics = append(diagnostics, diags...)
	}

	return diagnostics
}

// AnalyzeMultiple runs analysis on multiple statements.
func (a *Analyzer) AnalyzeMultiple(stmts []any, dialect lint.DialectInfo) []lint.Diagnostic {
	var diagnostics []lint.Diagnostic
	for _, stmt := range stmts {
		diagnostics = append(diagnostics, a.Analyze(stmt, dialect)...)
	}
	return diagnostics
}

func containsDialect(dialects []string, name string) bool {
	for _, d := range dialects {
		if d == name {
			return true
		}
	}
	return false
}
