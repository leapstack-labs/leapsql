package lint

// Config controls which rules are enabled and their severity.
type Config struct {
	// DisabledRules contains rule IDs to skip
	DisabledRules map[string]bool

	// SeverityOverrides changes the default severity of rules
	SeverityOverrides map[string]Severity
}

// NewConfig creates a default configuration with all rules enabled.
func NewConfig() *Config {
	return &Config{
		DisabledRules:     make(map[string]bool),
		SeverityOverrides: make(map[string]Severity),
	}
}

// IsDisabled returns true if the rule should be skipped.
func (c *Config) IsDisabled(ruleID string) bool {
	if c == nil {
		return false
	}
	return c.DisabledRules[ruleID]
}

// GetSeverity returns the severity for a rule, applying any override.
func (c *Config) GetSeverity(ruleID string, defaultSeverity Severity) Severity {
	if c != nil {
		if sev, ok := c.SeverityOverrides[ruleID]; ok {
			return sev
		}
	}
	return defaultSeverity
}

// Disable disables a rule by ID.
func (c *Config) Disable(ruleID string) *Config {
	c.DisabledRules[ruleID] = true
	return c
}

// SetSeverity overrides the severity for a rule.
func (c *Config) SetSeverity(ruleID string, severity Severity) *Config {
	c.SeverityOverrides[ruleID] = severity
	return c
}
