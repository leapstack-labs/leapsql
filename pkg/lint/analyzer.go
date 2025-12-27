package lint

// Analyzer runs lint rules against parsed SQL.
type Analyzer struct {
	config  *Config
	dialect string // Filter rules by dialect (empty = all)
}

// NewAnalyzer creates a new analyzer with optional configuration.
func NewAnalyzer(config *Config) *Analyzer {
	if config == nil {
		config = NewConfig()
	}
	return &Analyzer{config: config}
}

// NewAnalyzerWithRegistry creates an analyzer that uses registry rules
// filtered by dialect.
func NewAnalyzerWithRegistry(config *Config, dialect string) *Analyzer {
	if config == nil {
		config = NewConfig()
	}
	return &Analyzer{
		config:  config,
		dialect: dialect,
	}
}

// Analyze runs all rules from the dialect against the statement.
// The stmt parameter should be *core.SelectStmt.
func (a *Analyzer) Analyze(stmt any, dialect DialectInfo) []Diagnostic {
	if stmt == nil {
		return nil
	}

	var diagnostics []Diagnostic
	dialectName := a.dialect
	if dialectName == "" && dialect != nil {
		dialectName = dialect.GetName()
	}

	var rules []SQLRule
	if dialectName != "" {
		rules = GetSQLRulesByDialect(dialectName)
	} else {
		rules = GetAllSQLRules()
	}

	for _, rule := range rules {
		// Skip disabled rules
		if a.config.IsDisabled(rule.ID()) {
			continue
		}

		// Check dialect filter
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

// AnalyzeWithRegistryRules runs all registered rules against the statement.
// This is an alias for Analyze for backward compatibility.
func (a *Analyzer) AnalyzeWithRegistryRules(stmt any, dialect DialectInfo) []Diagnostic {
	return a.Analyze(stmt, dialect)
}

// AnalyzeMultiple runs analysis on multiple statements.
func (a *Analyzer) AnalyzeMultiple(stmts []any, dialect DialectInfo) []Diagnostic {
	var diagnostics []Diagnostic
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
