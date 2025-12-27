package lint

import (
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/token"
)

// =============================================================================
// Dialect Interface
// =============================================================================

// DialectInfo is a minimal interface to avoid importing the full dialect package.
// Implemented by dialect.Dialect.
type DialectInfo interface {
	GetName() string
	IsClauseToken(t token.TokenType) bool
	NormalizeName(name string) string
	IsWindow(name string) bool
}

// =============================================================================
// Rule Definitions
// =============================================================================

// RuleDef is a data-driven rule definition used by dialects for dialect-specific rules.
// Rules are stateless - all context comes via the Check function parameters.
// The Check function receives an `any` type that should be *parser.SelectStmt.
// This avoids import cycles between lint -> parser -> dialect -> lint.
type RuleDef struct {
	ID          string        // Unique identifier, e.g., "AM01" or "ansi/select-star"
	Name        string        // Human-readable name, e.g., "ambiguous.distinct"
	Group       string        // Category, e.g., "ambiguous", "structure", "convention"
	Description string        // Human-readable description
	Severity    core.Severity // Default severity
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
type CheckFunc func(stmt any, dialect DialectInfo, opts map[string]any) []Diagnostic

// =============================================================================
// Diagnostics
// =============================================================================

// Diagnostic represents a lint finding.
type Diagnostic struct {
	RuleID   string
	Severity core.Severity
	Message  string
	Pos      token.Position
	EndPos   token.Position // Optional: end of the problematic range
	Fixes    []Fix          // Optional: suggested fixes (for LSP code actions)

	// Remediation metadata
	DocumentationURL string        // URL to rule documentation, e.g., "https://leapsql.dev/docs/rules/AM01"
	ImpactScore      int           // 0-100, used for health score weighting
	AutoFixable      bool          // true if Fixes can be auto-applied
	RelatedInfo      []RelatedInfo // Additional locations/context
}

// RelatedInfo provides additional context for a diagnostic.
type RelatedInfo struct {
	FilePath string
	Pos      token.Position
	Message  string
}

// Fix represents a suggested code fix.
type Fix struct {
	Description string
	TextEdits   []TextEdit
}

// TextEdit represents a text replacement.
type TextEdit struct {
	Pos     token.Position
	EndPos  token.Position
	NewText string
}

// =============================================================================
// Rule Interfaces
// =============================================================================

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
	DefaultSeverity() core.Severity

	// ConfigKeys returns configuration keys this rule accepts
	ConfigKeys() []string

	// Documentation methods for richer rule documentation
	Rationale() string   // Why this rule exists, what problems it prevents
	BadExample() string  // Code showing the anti-pattern
	GoodExample() string // Code showing the correct pattern
	Fix() string         // How to fix violations (when not obvious)
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

// GetRuleInfo extracts metadata from a Rule for documentation/tooling.
func GetRuleInfo(r Rule) core.RuleInfo {
	info := core.RuleInfo{
		ID:              r.ID(),
		Name:            r.Name(),
		Group:           r.Group(),
		Description:     r.Description(),
		DefaultSeverity: r.DefaultSeverity(),
		ConfigKeys:      r.ConfigKeys(),
		Rationale:       r.Rationale(),
		BadExample:      r.BadExample(),
		GoodExample:     r.GoodExample(),
		Fix:             r.Fix(),
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

// =============================================================================
// Wrapped RuleDef
// =============================================================================

// wrappedRuleDef wraps a RuleDef to implement SQLRule.
type wrappedRuleDef struct {
	def RuleDef
}

// WrapRuleDef wraps a RuleDef to implement SQLRule interface.
func WrapRuleDef(def RuleDef) SQLRule {
	return &wrappedRuleDef{def: def}
}

func (w *wrappedRuleDef) ID() string                     { return w.def.ID }
func (w *wrappedRuleDef) Name() string                   { return w.def.Name }
func (w *wrappedRuleDef) Group() string                  { return w.def.Group }
func (w *wrappedRuleDef) Description() string            { return w.def.Description }
func (w *wrappedRuleDef) DefaultSeverity() core.Severity { return w.def.Severity }
func (w *wrappedRuleDef) ConfigKeys() []string           { return w.def.ConfigKeys }
func (w *wrappedRuleDef) Dialects() []string             { return w.def.Dialects }

// Documentation methods
func (w *wrappedRuleDef) Rationale() string   { return w.def.Rationale }
func (w *wrappedRuleDef) BadExample() string  { return w.def.BadExample }
func (w *wrappedRuleDef) GoodExample() string { return w.def.GoodExample }
func (w *wrappedRuleDef) Fix() string         { return w.def.Fix }

func (w *wrappedRuleDef) CheckSQL(stmt any, dialect DialectInfo, opts map[string]any) []Diagnostic {
	return w.def.Check(stmt, dialect, opts)
}

// Unwrap returns the underlying RuleDef.
func (w *wrappedRuleDef) Unwrap() RuleDef {
	return w.def
}

// =============================================================================
// Provider Interfaces
// =============================================================================

// Provider is the base interface for all lint providers.
type Provider interface {
	Name() string
}

// SQLProvider analyzes individual SQL statements.
// Implemented by the SQL analyzer for statement-level linting.
type SQLProvider interface {
	Provider
	AnalyzeStatement(stmt any, dialect DialectInfo) []Diagnostic
}

// ProjectProvider analyzes project-level concerns.
// Implemented by the project health analyzer for DAG/architecture linting.
type ProjectProvider interface {
	Provider
	// AnalyzeProject runs project-level rules and returns diagnostics.
	// The context parameter provides all project data needed for analysis.
	AnalyzeProject(ctx ProjectContext) []Diagnostic
}

// =============================================================================
// Project Context
// =============================================================================

// ProjectContext provides access to project data for project-level rules.
// This is an interface to avoid import cycles between lint and project packages.
type ProjectContext interface {
	// GetModels returns all models indexed by path.
	GetModels() map[string]ModelInfo

	// GetParents returns upstream model paths for a given model.
	GetParents(modelPath string) []string

	// GetChildren returns downstream model paths for a given model.
	GetChildren(modelPath string) []string

	// GetConfig returns the project health configuration.
	GetConfig() ProjectHealthConfig
}

// ModelInfo represents a model for project-level analysis.
// This mirrors the data needed from parser.ModelConfig without importing it.
type ModelInfo struct {
	Path         string            // Model path (e.g., "staging.customers")
	Name         string            // Model name (e.g., "stg_customers")
	FilePath     string            // Absolute path to .sql file
	Type         core.ModelType    // Inferred or explicit model type
	Sources      []string          // Table references (deps)
	Columns      []core.ColumnInfo // Column-level lineage
	Materialized string            // table, view, incremental
	Tags         []string          // Metadata tags
	Meta         map[string]any    // Custom metadata
}

// ProjectHealthConfig holds configurable thresholds for project health rules.
type ProjectHealthConfig struct {
	ModelFanoutThreshold        int // PM04: default 3
	TooManyJoinsThreshold       int // PM05: default 7
	PassthroughColumnThreshold  int // PL01: default 20
	StarlarkComplexityThreshold int // PT01: default 10
}

// DefaultProjectHealthConfig returns the default configuration.
func DefaultProjectHealthConfig() ProjectHealthConfig {
	return ProjectHealthConfig{
		ModelFanoutThreshold:        3,
		TooManyJoinsThreshold:       7,
		PassthroughColumnThreshold:  20,
		StarlarkComplexityThreshold: 10,
	}
}
