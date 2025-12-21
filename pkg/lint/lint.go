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
	ID          string    // Unique identifier, e.g., "ansi/fetch-limit-conflict"
	Description string    // Human-readable description
	Severity    Severity  // Default severity
	Check       CheckFunc // The check function
}

// CheckFunc analyzes a statement and returns diagnostics.
// The stmt parameter is *parser.SelectStmt passed as any to avoid import cycles.
type CheckFunc func(stmt any, dialect DialectInfo) []Diagnostic

// Diagnostic represents a lint finding.
type Diagnostic struct {
	RuleID   string
	Severity Severity
	Message  string
	Pos      token.Position
	EndPos   token.Position // Optional: end of the problematic range
	Fixes    []Fix          // Optional: suggested fixes (for future LSP code actions)
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
