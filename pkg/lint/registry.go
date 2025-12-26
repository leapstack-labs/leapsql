package lint

import "sync"

// registry is the single global registry for all lint rules.
var registry = &Registry{
	sqlRules:     make(map[string]SQLRule),
	projectRules: make(map[string]ProjectRule),
}

// Registry stores all registered lint rules (both SQL and project).
type Registry struct {
	mu           sync.RWMutex
	sqlRules     map[string]SQLRule
	projectRules map[string]ProjectRule
}

// =============================================================================
// SQL Rule Registration
// =============================================================================

// RegisterSQLRule adds an SQL rule to the registry.
// Call this from init() functions in rule packages.
func RegisterSQLRule(rule SQLRule) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	registry.sqlRules[rule.ID()] = rule
}

// GetAllSQLRules returns all registered SQL rules.
func GetAllSQLRules() []SQLRule {
	registry.mu.RLock()
	defer registry.mu.RUnlock()

	rules := make([]SQLRule, 0, len(registry.sqlRules))
	for _, rule := range registry.sqlRules {
		rules = append(rules, rule)
	}
	return rules
}

// GetSQLRuleByID returns an SQL rule by its ID.
func GetSQLRuleByID(id string) (SQLRule, bool) {
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	rule, ok := registry.sqlRules[id]
	return rule, ok
}

// GetSQLRulesByDialect returns SQL rules applicable to a specific dialect.
// Rules with empty/nil Dialects are included (they apply to all dialects).
func GetSQLRulesByDialect(dialectName string) []SQLRule {
	registry.mu.RLock()
	defer registry.mu.RUnlock()

	var rules []SQLRule
	for _, rule := range registry.sqlRules {
		dialects := rule.Dialects()
		if len(dialects) == 0 {
			// Rule applies to all dialects
			rules = append(rules, rule)
			continue
		}
		for _, d := range dialects {
			if d == dialectName {
				rules = append(rules, rule)
				break
			}
		}
	}
	return rules
}

// GetSQLRulesByGroup returns SQL rules in a specific group.
func GetSQLRulesByGroup(group string) []SQLRule {
	registry.mu.RLock()
	defer registry.mu.RUnlock()

	var rules []SQLRule
	for _, rule := range registry.sqlRules {
		if rule.Group() == group {
			rules = append(rules, rule)
		}
	}
	return rules
}

// CountSQLRules returns the number of registered SQL rules.
func CountSQLRules() int {
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	return len(registry.sqlRules)
}

// =============================================================================
// Project Rule Registration
// =============================================================================

// RegisterProjectRule adds a project rule to the registry.
// Call this from init() functions in rule packages.
func RegisterProjectRule(rule ProjectRule) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	registry.projectRules[rule.ID()] = rule
}

// GetAllProjectRules returns all registered project rules.
func GetAllProjectRules() []ProjectRule {
	registry.mu.RLock()
	defer registry.mu.RUnlock()

	rules := make([]ProjectRule, 0, len(registry.projectRules))
	for _, rule := range registry.projectRules {
		rules = append(rules, rule)
	}
	return rules
}

// GetProjectRuleByID returns a project rule by its ID.
func GetProjectRuleByID(id string) (ProjectRule, bool) {
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	rule, ok := registry.projectRules[id]
	return rule, ok
}

// GetProjectRulesByGroup returns project rules in a specific group.
func GetProjectRulesByGroup(group string) []ProjectRule {
	registry.mu.RLock()
	defer registry.mu.RUnlock()

	var rules []ProjectRule
	for _, rule := range registry.projectRules {
		if rule.Group() == group {
			rules = append(rules, rule)
		}
	}
	return rules
}

// CountProjectRules returns the number of registered project rules.
func CountProjectRules() int {
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	return len(registry.projectRules)
}

// =============================================================================
// Unified Access
// =============================================================================

// GetRuleByID returns any rule by its ID, checking both SQL and project rules.
func GetRuleByID(id string) (Rule, bool) {
	registry.mu.RLock()
	defer registry.mu.RUnlock()

	if rule, ok := registry.sqlRules[id]; ok {
		return rule, true
	}
	if rule, ok := registry.projectRules[id]; ok {
		return rule, true
	}
	return nil, false
}

// AllRules returns metadata for all registered rules (both SQL and project).
func AllRules() []RuleInfo {
	registry.mu.RLock()
	defer registry.mu.RUnlock()

	rules := make([]RuleInfo, 0, len(registry.sqlRules)+len(registry.projectRules))
	for _, rule := range registry.sqlRules {
		rules = append(rules, GetRuleInfo(rule))
	}
	for _, rule := range registry.projectRules {
		rules = append(rules, GetRuleInfo(rule))
	}
	return rules
}

// =============================================================================
// Testing Utilities
// =============================================================================

// Clear removes all rules from the registry. Used for testing.
func Clear() {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	registry.sqlRules = make(map[string]SQLRule)
	registry.projectRules = make(map[string]ProjectRule)
}
