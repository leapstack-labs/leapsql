package lint

import "sync"

// unifiedRegistry stores all rules (both SQL and project) for unified access.
var unifiedRegistry = &UnifiedRegistry{
	sqlRules:     make(map[string]SQLRule),
	projectRules: make(map[string]ProjectRule),
}

// UnifiedRegistry provides unified access to all rules.
type UnifiedRegistry struct {
	mu           sync.RWMutex
	sqlRules     map[string]SQLRule
	projectRules map[string]ProjectRule
}

// RegisterSQLRule adds an SQL rule to the unified registry.
func RegisterSQLRule(rule SQLRule) {
	unifiedRegistry.mu.Lock()
	defer unifiedRegistry.mu.Unlock()
	unifiedRegistry.sqlRules[rule.ID()] = rule
}

// RegisterProjectRule adds a project rule to the unified registry.
func RegisterProjectRule(rule ProjectRule) {
	unifiedRegistry.mu.Lock()
	defer unifiedRegistry.mu.Unlock()
	unifiedRegistry.projectRules[rule.ID()] = rule
}

// GetAllSQLRules returns all registered SQL rules.
func GetAllSQLRules() []SQLRule {
	unifiedRegistry.mu.RLock()
	defer unifiedRegistry.mu.RUnlock()

	rules := make([]SQLRule, 0, len(unifiedRegistry.sqlRules))
	for _, rule := range unifiedRegistry.sqlRules {
		rules = append(rules, rule)
	}
	return rules
}

// GetAllProjectRules returns all registered project rules.
func GetAllProjectRules() []ProjectRule {
	unifiedRegistry.mu.RLock()
	defer unifiedRegistry.mu.RUnlock()

	rules := make([]ProjectRule, 0, len(unifiedRegistry.projectRules))
	for _, rule := range unifiedRegistry.projectRules {
		rules = append(rules, rule)
	}
	return rules
}

// GetSQLRuleByID returns an SQL rule by its ID.
func GetSQLRuleByID(id string) (SQLRule, bool) {
	unifiedRegistry.mu.RLock()
	defer unifiedRegistry.mu.RUnlock()
	rule, ok := unifiedRegistry.sqlRules[id]
	return rule, ok
}

// GetProjectRuleByID returns a project rule by its ID.
func GetProjectRuleByID(id string) (ProjectRule, bool) {
	unifiedRegistry.mu.RLock()
	defer unifiedRegistry.mu.RUnlock()
	rule, ok := unifiedRegistry.projectRules[id]
	return rule, ok
}

// GetRuleByID returns any rule by its ID, checking both registries.
func GetRuleByID(id string) (Rule, bool) {
	unifiedRegistry.mu.RLock()
	defer unifiedRegistry.mu.RUnlock()

	if rule, ok := unifiedRegistry.sqlRules[id]; ok {
		return rule, true
	}
	if rule, ok := unifiedRegistry.projectRules[id]; ok {
		return rule, true
	}
	return nil, false
}

// AllRules returns metadata for all registered rules.
func AllRules() []RuleInfo {
	unifiedRegistry.mu.RLock()
	defer unifiedRegistry.mu.RUnlock()

	rules := make([]RuleInfo, 0, len(unifiedRegistry.sqlRules)+len(unifiedRegistry.projectRules))
	for _, rule := range unifiedRegistry.sqlRules {
		rules = append(rules, GetRuleInfo(rule))
	}
	for _, rule := range unifiedRegistry.projectRules {
		rules = append(rules, GetRuleInfo(rule))
	}
	return rules
}

// GetSQLRulesByDialect returns SQL rules applicable to a specific dialect.
// Rules with empty/nil Dialects are included (they apply to all dialects).
func GetSQLRulesByDialect(dialectName string) []SQLRule {
	unifiedRegistry.mu.RLock()
	defer unifiedRegistry.mu.RUnlock()

	var rules []SQLRule
	for _, rule := range unifiedRegistry.sqlRules {
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
	unifiedRegistry.mu.RLock()
	defer unifiedRegistry.mu.RUnlock()

	var rules []SQLRule
	for _, rule := range unifiedRegistry.sqlRules {
		if rule.Group() == group {
			rules = append(rules, rule)
		}
	}
	return rules
}

// GetProjectRulesByGroup returns project rules in a specific group.
func GetProjectRulesByGroup(group string) []ProjectRule {
	unifiedRegistry.mu.RLock()
	defer unifiedRegistry.mu.RUnlock()

	var rules []ProjectRule
	for _, rule := range unifiedRegistry.projectRules {
		if rule.Group() == group {
			rules = append(rules, rule)
		}
	}
	return rules
}

// CountSQLRules returns the number of registered SQL rules.
func CountSQLRules() int {
	unifiedRegistry.mu.RLock()
	defer unifiedRegistry.mu.RUnlock()
	return len(unifiedRegistry.sqlRules)
}

// CountProjectRules returns the number of registered project rules.
func CountProjectRules() int {
	unifiedRegistry.mu.RLock()
	defer unifiedRegistry.mu.RUnlock()
	return len(unifiedRegistry.projectRules)
}

// ClearUnified removes all rules from the unified registry. Used for testing.
func ClearUnified() {
	unifiedRegistry.mu.Lock()
	defer unifiedRegistry.mu.Unlock()
	unifiedRegistry.sqlRules = make(map[string]SQLRule)
	unifiedRegistry.projectRules = make(map[string]ProjectRule)
}
