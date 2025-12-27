package project

import (
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/lint"
)

// Analyzer runs project-level lint rules against the project context.
type Analyzer struct {
	config        *AnalyzerConfig
	disabledRules map[string]bool
}

// AnalyzerConfig holds configuration for the project analyzer.
type AnalyzerConfig struct {
	// DisabledRules contains rule IDs to skip
	DisabledRules map[string]bool

	// SeverityOverrides changes the default severity of rules
	SeverityOverrides map[string]core.Severity

	// ProjectHealth contains thresholds and settings
	ProjectHealth lint.ProjectHealthConfig
}

// NewAnalyzerConfig creates a default configuration.
func NewAnalyzerConfig() *AnalyzerConfig {
	return &AnalyzerConfig{
		DisabledRules:     make(map[string]bool),
		SeverityOverrides: make(map[string]core.Severity),
		ProjectHealth:     lint.DefaultProjectHealthConfig(),
	}
}

// NewAnalyzer creates a new project analyzer with optional configuration.
func NewAnalyzer(config *AnalyzerConfig) *Analyzer {
	if config == nil {
		config = NewAnalyzerConfig()
	}
	return &Analyzer{
		config:        config,
		disabledRules: config.DisabledRules,
	}
}

// Analyze runs all registered project rules against the context.
func (a *Analyzer) Analyze(ctx *Context) []Diagnostic {
	if ctx == nil {
		return nil
	}

	var diagnostics []Diagnostic
	rules := GetAll()

	for _, rule := range rules {
		// Skip disabled rules
		if a.isDisabled(rule.ID) {
			continue
		}

		// Run the rule
		diags := rule.Check(ctx)

		// Apply severity overrides
		for i := range diags {
			diags[i].Severity = a.getSeverity(rule.ID, diags[i].Severity)
		}

		diagnostics = append(diagnostics, diags...)
	}

	return diagnostics
}

// AnalyzeProject implements lint.ProjectProvider.
func (a *Analyzer) AnalyzeProject(ctx lint.ProjectContext) []lint.Diagnostic {
	// Convert lint.ProjectContext to our internal Context
	projectCtx, ok := ctx.(*Context)
	if !ok {
		return nil
	}

	diags := a.Analyze(projectCtx)

	// Convert to lint.Diagnostic (Severity is now the same type)
	result := make([]lint.Diagnostic, len(diags))
	for i, d := range diags {
		result[i] = lint.Diagnostic{
			RuleID:   d.RuleID,
			Severity: d.Severity, // No conversion needed - same type
			Message:  d.Message,
		}
	}
	return result
}

// Name implements lint.Provider.
func (a *Analyzer) Name() string {
	return "project-health"
}

func (a *Analyzer) isDisabled(ruleID string) bool {
	return a.disabledRules[ruleID]
}

func (a *Analyzer) getSeverity(ruleID string, defaultSev core.Severity) core.Severity {
	if a.config != nil {
		if sev, ok := a.config.SeverityOverrides[ruleID]; ok {
			return sev
		}
	}
	return defaultSev
}

// Disable disables a rule by ID.
func (a *Analyzer) Disable(ruleID string) {
	a.disabledRules[ruleID] = true
}

// Enable enables a previously disabled rule.
func (a *Analyzer) Enable(ruleID string) {
	delete(a.disabledRules, ruleID)
}
