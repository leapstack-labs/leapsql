package project

import (
	"sync"

	"github.com/leapstack-labs/leapsql/pkg/lint"
)

// globalRegistry is the single global registry for project lint rules.
var globalRegistry = &Registry{
	rules: make(map[string]RuleDef),
}

// Registry stores registered project lint rules for discovery.
type Registry struct {
	mu    sync.RWMutex
	rules map[string]RuleDef // keyed by ID
}

// RuleDef is a project-level rule definition.
type RuleDef struct {
	ID          string        // Unique identifier, e.g., "PM01"
	Name        string        // Human-readable name, e.g., "root-models"
	Group       string        // Category: "modeling", "structure", "lineage"
	Description string        // Human-readable description
	Severity    lint.Severity // Default severity (uses unified lint.Severity)
	Check       Check         // The check function
	ConfigKeys  []string      // Configuration keys this rule accepts

	// Documentation fields for richer rule documentation
	Rationale   string // Why this rule exists, what problems it prevents
	BadExample  string // Code showing the anti-pattern
	GoodExample string // Code showing the correct pattern
	Fix         string // How to fix violations (when not obvious)
}

// Check is the function signature for project-level rule checks.
type Check func(ctx *Context) []Diagnostic

// Diagnostic represents a project-level lint finding.
type Diagnostic struct {
	RuleID   string
	Severity lint.Severity
	Message  string
	Model    string // Model path that triggered this diagnostic
	FilePath string // File path for LSP integration

	// Remediation metadata
	DocumentationURL string // URL to rule documentation
	ImpactScore      int    // 0-100, used for health score weighting
	AutoFixable      bool   // true if fixes can be auto-applied
}

// Register adds a rule to the global registry.
// Call this from init() functions in rule packages.
func Register(rule RuleDef) {
	globalRegistry.mu.Lock()
	defer globalRegistry.mu.Unlock()
	globalRegistry.rules[rule.ID] = rule

	// Also register in the unified registry
	lint.RegisterProjectRule(WrapRuleDef(rule))
}

// GetAll returns all registered rules.
func GetAll() []RuleDef {
	globalRegistry.mu.RLock()
	defer globalRegistry.mu.RUnlock()

	rules := make([]RuleDef, 0, len(globalRegistry.rules))
	for _, rule := range globalRegistry.rules {
		rules = append(rules, rule)
	}
	return rules
}

// GetByID returns a rule by its ID.
func GetByID(id string) (RuleDef, bool) {
	globalRegistry.mu.RLock()
	defer globalRegistry.mu.RUnlock()
	rule, ok := globalRegistry.rules[id]
	return rule, ok
}

// GetByGroup returns all rules in a specific group.
func GetByGroup(group string) []RuleDef {
	globalRegistry.mu.RLock()
	defer globalRegistry.mu.RUnlock()

	var rules []RuleDef
	for _, rule := range globalRegistry.rules {
		if rule.Group == group {
			rules = append(rules, rule)
		}
	}
	return rules
}

// Count returns the number of registered rules.
func Count() int {
	globalRegistry.mu.RLock()
	defer globalRegistry.mu.RUnlock()
	return len(globalRegistry.rules)
}

// Clear removes all registered rules. Used for testing.
func Clear() {
	globalRegistry.mu.Lock()
	defer globalRegistry.mu.Unlock()
	globalRegistry.rules = make(map[string]RuleDef)
}

// wrappedProjectRule wraps a RuleDef to implement lint.ProjectRule.
type wrappedProjectRule struct {
	def RuleDef
}

// WrapRuleDef wraps a project RuleDef to implement lint.ProjectRule.
func WrapRuleDef(def RuleDef) lint.ProjectRule {
	return &wrappedProjectRule{def: def}
}

func (w *wrappedProjectRule) ID() string                     { return w.def.ID }
func (w *wrappedProjectRule) Name() string                   { return w.def.Name }
func (w *wrappedProjectRule) Group() string                  { return w.def.Group }
func (w *wrappedProjectRule) Description() string            { return w.def.Description }
func (w *wrappedProjectRule) DefaultSeverity() lint.Severity { return w.def.Severity }
func (w *wrappedProjectRule) ConfigKeys() []string           { return w.def.ConfigKeys }

// Documentation methods
func (w *wrappedProjectRule) Rationale() string   { return w.def.Rationale }
func (w *wrappedProjectRule) BadExample() string  { return w.def.BadExample }
func (w *wrappedProjectRule) GoodExample() string { return w.def.GoodExample }
func (w *wrappedProjectRule) Fix() string         { return w.def.Fix }

func (w *wrappedProjectRule) CheckProject(ctx lint.ProjectContext) []lint.Diagnostic {
	// Convert lint.ProjectContext to our internal Context
	projectCtx, ok := ctx.(*Context)
	if !ok {
		return nil
	}

	diags := w.def.Check(projectCtx)

	// Convert to lint.Diagnostic
	result := make([]lint.Diagnostic, len(diags))
	for i, d := range diags {
		result[i] = lint.Diagnostic{
			RuleID:           d.RuleID,
			Severity:         d.Severity,
			Message:          d.Message,
			DocumentationURL: d.DocumentationURL,
			ImpactScore:      d.ImpactScore,
			AutoFixable:      d.AutoFixable,
		}
	}
	return result
}
