package lint

import "sync"

// globalRegistry is the single global registry for all lint rules.
var globalRegistry = &Registry{
	rules: make(map[string]RuleDef),
}

// Registry stores registered lint rules for discovery.
type Registry struct {
	mu    sync.RWMutex
	rules map[string]RuleDef // keyed by ID
}

// Register adds a rule to the global registry.
// Call this from init() functions in rule packages.
func Register(rule RuleDef) {
	globalRegistry.mu.Lock()
	defer globalRegistry.mu.Unlock()
	globalRegistry.rules[rule.ID] = rule

	// Also register in the unified registry
	RegisterSQLRule(WrapRuleDef(rule))
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

// GetByDialect returns rules applicable to a specific dialect.
// Rules with empty/nil Dialects field are included (they apply to all dialects).
func GetByDialect(dialectName string) []RuleDef {
	globalRegistry.mu.RLock()
	defer globalRegistry.mu.RUnlock()

	var rules []RuleDef
	for _, rule := range globalRegistry.rules {
		if len(rule.Dialects) == 0 {
			// Rule applies to all dialects
			rules = append(rules, rule)
			continue
		}
		for _, d := range rule.Dialects {
			if d == dialectName {
				rules = append(rules, rule)
				break
			}
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
