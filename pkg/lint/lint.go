// Package lint provides data-driven SQL linting that integrates with the dialect architecture.
// Rules are defined alongside dialects and inherited via Extends(), following the same patterns
// used for clause handlers and operators.
//
// The package defines types that are used across the system. Rule implementations and the
// Analyzer live in separate packages to avoid import cycles.
package lint

import (
	"github.com/leapstack-labs/leapsql/pkg/token"
)

// Severity indicates the importance of a diagnostic.
type Severity int

// Severity levels for diagnostics.
const (
	// SeverityError indicates a critical issue that should be fixed.
	SeverityError Severity = iota
	// SeverityWarning indicates a potential issue that should be reviewed.
	SeverityWarning
	// SeverityInfo indicates informational feedback.
	SeverityInfo
	// SeverityHint indicates a suggestion for improvement.
	SeverityHint
)

// String returns the string representation of the severity.
func (s Severity) String() string {
	switch s {
	case SeverityError:
		return "error"
	case SeverityWarning:
		return "warning"
	case SeverityInfo:
		return "info"
	case SeverityHint:
		return "hint"
	default:
		return "unknown"
	}
}

// DialectInfo is a minimal interface to avoid importing the full dialect package.
// Implemented by dialect.Dialect.
type DialectInfo interface {
	GetName() string
	IsClauseToken(t token.TokenType) bool
	NormalizeName(name string) string
	IsWindow(name string) bool
}

// RuleDef is a data-driven rule definition.
// Rules are stateless - all context comes via the Check function parameters.
// The Check function receives an `any` type that should be *parser.SelectStmt.
// This avoids import cycles between lint -> parser -> dialect -> lint.
type RuleDef struct {
	ID          string    // Unique identifier, e.g., "AM01" or "ansi/select-star"
	Name        string    // Human-readable name, e.g., "ambiguous.distinct"
	Group       string    // Category, e.g., "ambiguous", "structure", "convention"
	Description string    // Human-readable description
	Severity    Severity  // Default severity
	Check       CheckFunc // The check function
	ConfigKeys  []string  // Configuration keys this rule accepts (for rule-specific options)
	Dialects    []string  // Restrict to specific dialects; nil/empty means all dialects
}

// CheckFunc analyzes a statement and returns diagnostics.
// The stmt parameter is *parser.SelectStmt passed as any to avoid import cycles.
// The opts parameter contains rule-specific options from configuration.
type CheckFunc func(stmt any, dialect DialectInfo, opts map[string]any) []Diagnostic

// Diagnostic represents a lint finding.
type Diagnostic struct {
	RuleID   string
	Severity Severity
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
