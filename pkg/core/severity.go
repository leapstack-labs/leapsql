package core

import "strings"

// =============================================================================
// Severity
// =============================================================================

// Severity indicates the importance of a lint diagnostic.
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

// ParseSeverity converts a string to a Severity value.
// Returns the severity and true if valid, or SeverityWarning and false if invalid.
func ParseSeverity(s string) (Severity, bool) {
	switch strings.ToLower(s) {
	case "error":
		return SeverityError, true
	case "warning":
		return SeverityWarning, true
	case "info":
		return SeverityInfo, true
	case "hint":
		return SeverityHint, true
	default:
		return SeverityWarning, false
	}
}

// =============================================================================
// RuleInfo
// =============================================================================

// RuleInfo provides metadata about a lint rule for documentation/tooling.
// This is a DTO (Data Transfer Object) - it carries data without behavior.
type RuleInfo struct {
	ID              string   `json:"id"`
	Name            string   `json:"name"`
	Group           string   `json:"group"`
	Description     string   `json:"description"`
	DefaultSeverity Severity `json:"default_severity"`
	ConfigKeys      []string `json:"config_keys,omitempty"`
	Dialects        []string `json:"dialects,omitempty"` // Only for SQL rules
	Type            string   `json:"type"`               // "sql" or "project"

	// Documentation fields
	Rationale   string `json:"rationale,omitempty"`
	BadExample  string `json:"bad_example,omitempty"`
	GoodExample string `json:"good_example,omitempty"`
	Fix         string `json:"fix,omitempty"`
}
