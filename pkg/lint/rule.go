package lint

// Rule is the base interface all lint rules implement.
// This provides a unified interface for both SQL-level and project-level rules.
type Rule interface {
	// ID returns the unique identifier, e.g., "AM01" or "PM01"
	ID() string

	// Name returns the human-readable name, e.g., "ambiguous.distinct"
	Name() string

	// Group returns the category, e.g., "ambiguous", "structure", "modeling"
	Group() string

	// Description returns a human-readable description
	Description() string

	// DefaultSeverity returns the default severity for this rule
	DefaultSeverity() Severity

	// ConfigKeys returns configuration keys this rule accepts
	ConfigKeys() []string
}

// SQLRule analyzes individual SQL statements.
// Implemented by rules that check SQL syntax and structure.
type SQLRule interface {
	Rule

	// CheckSQL analyzes a statement and returns diagnostics.
	// The stmt parameter is *parser.SelectStmt passed as any to avoid import cycles.
	// The opts parameter contains rule-specific options from configuration.
	CheckSQL(stmt any, dialect DialectInfo, opts map[string]any) []Diagnostic

	// Dialects returns dialect restrictions; nil/empty means all dialects.
	Dialects() []string
}

// ProjectRule analyzes project-level concerns.
// Implemented by rules that check DAG structure, naming conventions, etc.
type ProjectRule interface {
	Rule

	// CheckProject analyzes the project context and returns diagnostics.
	CheckProject(ctx ProjectContext) []Diagnostic
}

// RuleInfo provides metadata about a rule for documentation/tooling.
type RuleInfo struct {
	ID              string   `json:"id"`
	Name            string   `json:"name"`
	Group           string   `json:"group"`
	Description     string   `json:"description"`
	DefaultSeverity Severity `json:"default_severity"`
	ConfigKeys      []string `json:"config_keys,omitempty"`
	Dialects        []string `json:"dialects,omitempty"` // Only for SQL rules
	Type            string   `json:"type"`               // "sql" or "project"
}

// GetRuleInfo extracts metadata from a Rule for documentation/tooling.
func GetRuleInfo(r Rule) RuleInfo {
	info := RuleInfo{
		ID:              r.ID(),
		Name:            r.Name(),
		Group:           r.Group(),
		Description:     r.Description(),
		DefaultSeverity: r.DefaultSeverity(),
		ConfigKeys:      r.ConfigKeys(),
	}

	// Check if it's an SQL rule with dialect restrictions
	if sqlRule, ok := r.(SQLRule); ok {
		info.Dialects = sqlRule.Dialects()
		info.Type = "sql"
	} else if _, ok := r.(ProjectRule); ok {
		info.Type = "project"
	}

	return info
}

// wrappedRuleDef wraps a legacy RuleDef to implement SQLRule.
// This allows gradual migration of existing rules to the new interface.
type wrappedRuleDef struct {
	def RuleDef
}

// WrapRuleDef wraps a legacy RuleDef to implement SQLRule.
// This allows gradual migration of existing rules.
func WrapRuleDef(def RuleDef) SQLRule {
	return &wrappedRuleDef{def: def}
}

func (w *wrappedRuleDef) ID() string                { return w.def.ID }
func (w *wrappedRuleDef) Name() string              { return w.def.Name }
func (w *wrappedRuleDef) Group() string             { return w.def.Group }
func (w *wrappedRuleDef) Description() string       { return w.def.Description }
func (w *wrappedRuleDef) DefaultSeverity() Severity { return w.def.Severity }
func (w *wrappedRuleDef) ConfigKeys() []string      { return w.def.ConfigKeys }
func (w *wrappedRuleDef) Dialects() []string        { return w.def.Dialects }

func (w *wrappedRuleDef) CheckSQL(stmt any, dialect DialectInfo, opts map[string]any) []Diagnostic {
	return w.def.Check(stmt, dialect, opts)
}

// Unwrap returns the underlying RuleDef if this rule is a wrapped RuleDef.
func (w *wrappedRuleDef) Unwrap() RuleDef {
	return w.def
}
