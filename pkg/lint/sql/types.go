package sql

import (
	"github.com/leapstack-labs/leapsql/pkg/lint"
)

// RuleDef is a data-driven SQL rule definition.
// Rules are stateless - all context comes via the Check function parameters.
// The Check function receives an `any` type that should be *parser.SelectStmt.
// This avoids import cycles between lint -> parser -> dialect -> lint.
type RuleDef struct {
	ID          string        // Unique identifier, e.g., "AM01" or "ansi/select-star"
	Name        string        // Human-readable name, e.g., "ambiguous.distinct"
	Group       string        // Category, e.g., "ambiguous", "structure", "convention"
	Description string        // Human-readable description
	Severity    lint.Severity // Default severity
	Check       CheckFunc     // The check function
	ConfigKeys  []string      // Configuration keys this rule accepts (for rule-specific options)
	Dialects    []string      // Restrict to specific dialects; nil/empty means all dialects

	// Documentation fields for richer rule documentation
	Rationale   string // Why this rule exists, what problems it prevents
	BadExample  string // Code showing the anti-pattern
	GoodExample string // Code showing the correct pattern
	Fix         string // How to fix violations (when not obvious)
}

// CheckFunc analyzes a statement and returns diagnostics.
// The stmt parameter is *parser.SelectStmt passed as any to avoid import cycles.
// The opts parameter contains rule-specific options from configuration.
type CheckFunc func(stmt any, dialect lint.DialectInfo, opts map[string]any) []lint.Diagnostic

// wrappedRuleDef wraps a legacy RuleDef to implement lint.SQLRule.
// This allows gradual migration of existing rules to the new interface.
type wrappedRuleDef struct {
	def RuleDef
}

// WrapRuleDef wraps a RuleDef to implement lint.SQLRule.
func WrapRuleDef(def RuleDef) lint.SQLRule {
	return &wrappedRuleDef{def: def}
}

func (w *wrappedRuleDef) ID() string                     { return w.def.ID }
func (w *wrappedRuleDef) Name() string                   { return w.def.Name }
func (w *wrappedRuleDef) Group() string                  { return w.def.Group }
func (w *wrappedRuleDef) Description() string            { return w.def.Description }
func (w *wrappedRuleDef) DefaultSeverity() lint.Severity { return w.def.Severity }
func (w *wrappedRuleDef) ConfigKeys() []string           { return w.def.ConfigKeys }
func (w *wrappedRuleDef) Dialects() []string             { return w.def.Dialects }

// Documentation methods
func (w *wrappedRuleDef) Rationale() string   { return w.def.Rationale }
func (w *wrappedRuleDef) BadExample() string  { return w.def.BadExample }
func (w *wrappedRuleDef) GoodExample() string { return w.def.GoodExample }
func (w *wrappedRuleDef) Fix() string         { return w.def.Fix }

func (w *wrappedRuleDef) CheckSQL(stmt any, dialect lint.DialectInfo, opts map[string]any) []lint.Diagnostic {
	return w.def.Check(stmt, dialect, opts)
}

// Unwrap returns the underlying RuleDef if this rule is a wrapped RuleDef.
func (w *wrappedRuleDef) Unwrap() RuleDef {
	return w.def
}

// Register adds a rule to the registry.
// Call this from init() functions in rule packages.
func Register(rule RuleDef) {
	lint.RegisterSQLRule(WrapRuleDef(rule))
}
