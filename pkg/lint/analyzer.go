package lint

// RuleProvider provides lint rules. Implemented by dialect.Dialect.
type RuleProvider interface {
	DialectInfo
	LintRules() []RuleDef
}

// Analyzer runs lint rules against parsed SQL.
type Analyzer struct {
	config          *Config
	useRegistry     bool   // If true, also use rules from global registry
	registryDialect string // Filter registry rules by dialect (empty = all)
}

// NewAnalyzer creates a new analyzer with optional configuration.
func NewAnalyzer(config *Config) *Analyzer {
	if config == nil {
		config = NewConfig()
	}
	return &Analyzer{config: config}
}

// NewAnalyzerWithRegistry creates an analyzer that uses both dialect rules
// and global registry rules.
func NewAnalyzerWithRegistry(config *Config, dialect string) *Analyzer {
	if config == nil {
		config = NewConfig()
	}
	return &Analyzer{
		config:          config,
		useRegistry:     true,
		registryDialect: dialect,
	}
}

// Analyze runs all rules from the dialect against the statement.
// The stmt parameter should be *parser.SelectStmt.
func (a *Analyzer) Analyze(stmt any, provider RuleProvider) []Diagnostic {
	if stmt == nil {
		return nil
	}

	var diagnostics []Diagnostic

	// Get rules from provider (dialect)
	rules := provider.LintRules()

	// Optionally add rules from global registry
	if a.useRegistry {
		var registryRules []RuleDef
		if a.registryDialect != "" {
			registryRules = GetByDialect(a.registryDialect)
		} else {
			registryRules = GetAll()
		}
		rules = append(rules, registryRules...)
	}

	for _, rule := range rules {
		// Skip disabled rules
		if a.config.IsDisabled(rule.ID) {
			continue
		}

		// Check dialect filter
		if len(rule.Dialects) > 0 && !containsDialect(rule.Dialects, provider.GetName()) {
			continue
		}

		// Get rule-specific options
		opts := a.config.GetRuleOptions(rule.ID)

		// Run the rule with options
		diags := rule.Check(stmt, provider, opts)

		// Apply severity overrides
		for i := range diags {
			diags[i].Severity = a.config.GetSeverity(rule.ID, diags[i].Severity)
		}

		diagnostics = append(diagnostics, diags...)
	}

	return diagnostics
}

// AnalyzeWithRegistryRules runs all registered rules against the statement.
// This method uses only the global registry, not dialect-specific rules.
func (a *Analyzer) AnalyzeWithRegistryRules(stmt any, dialect DialectInfo) []Diagnostic {
	if stmt == nil {
		return nil
	}

	var diagnostics []Diagnostic
	dialectName := ""
	if dialect != nil {
		dialectName = dialect.GetName()
	}

	var rules []RuleDef
	if dialectName != "" {
		rules = GetByDialect(dialectName)
	} else {
		rules = GetAll()
	}

	for _, rule := range rules {
		// Skip disabled rules
		if a.config.IsDisabled(rule.ID) {
			continue
		}

		// Get rule-specific options
		opts := a.config.GetRuleOptions(rule.ID)

		// Run the rule with options
		diags := rule.Check(stmt, dialect, opts)

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

func containsDialect(dialects []string, name string) bool {
	for _, d := range dialects {
		if d == name {
			return true
		}
	}
	return false
}
