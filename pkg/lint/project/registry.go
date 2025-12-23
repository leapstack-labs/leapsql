package project

import "sync"

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
	ID          string   // Unique identifier, e.g., "PM01"
	Name        string   // Human-readable name, e.g., "root-models"
	Group       string   // Category: "modeling", "structure", "lineage"
	Description string   // Human-readable description
	Severity    Severity // Default severity
	Check       Check    // The check function
	ConfigKeys  []string // Configuration keys this rule accepts
}

// Severity indicates the importance of a diagnostic.
type Severity int

// Severity levels for diagnostics.
const (
	SeverityError Severity = iota
	SeverityWarning
	SeverityInfo
	SeverityHint
)

// Check is the function signature for project-level rule checks.
type Check func(ctx *Context) []Diagnostic

// Diagnostic represents a project-level lint finding.
type Diagnostic struct {
	RuleID   string
	Severity Severity
	Message  string
	Model    string // Model path that triggered this diagnostic
	FilePath string // File path for LSP integration
}

// Register adds a rule to the global registry.
// Call this from init() functions in rule packages.
func Register(rule RuleDef) {
	globalRegistry.mu.Lock()
	defer globalRegistry.mu.Unlock()
	globalRegistry.rules[rule.ID] = rule
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
