package lint

// RuleProvider provides lint rules. Implemented by dialect.Dialect.
type RuleProvider interface {
	DialectInfo
	LintRules() []RuleDef
}

// Analyzer runs lint rules against parsed SQL.
type Analyzer struct {
	config *Config
}

// NewAnalyzer creates a new analyzer with optional configuration.
func NewAnalyzer(config *Config) *Analyzer {
	if config == nil {
		config = NewConfig()
	}
	return &Analyzer{config: config}
}

// Analyze runs all rules from the dialect against the statement.
// The stmt parameter should be *parser.SelectStmt.
func (a *Analyzer) Analyze(stmt any, provider RuleProvider) []Diagnostic {
	if stmt == nil {
		return nil
	}

	var diagnostics []Diagnostic

	for _, rule := range provider.LintRules() {
		// Skip disabled rules
		if a.config.IsDisabled(rule.ID) {
			continue
		}

		// Run the rule
		diags := rule.Check(stmt, provider)

		// Apply severity overrides
		for i := range diags {
			diags[i].Severity = a.config.GetSeverity(rule.ID, diags[i].Severity)
		}

		diagnostics = append(diagnostics, diags...)
	}

	return diagnostics
}

// AnalyzeMultiple runs analysis on multiple statements.
func (a *Analyzer) AnalyzeMultiple(stmts []any, provider RuleProvider) []Diagnostic {
	var diagnostics []Diagnostic
	for _, stmt := range stmts {
		diagnostics = append(diagnostics, a.Analyze(stmt, provider)...)
	}
	return diagnostics
}
