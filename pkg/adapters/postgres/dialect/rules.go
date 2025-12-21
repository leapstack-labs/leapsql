// Package dialect provides the PostgreSQL SQL dialect definition.
// This file contains PostgreSQL-specific lint rules.
package dialect

import "github.com/leapstack-labs/leapsql/pkg/lint"

// PostgreSQL-specific rules can be added here.
// For now, PostgreSQL inherits all ANSI rules via Extends().

// AllRules contains all PostgreSQL-specific lint rules.
// Note: ANSI rules are inherited automatically via Extends().
var AllRules = []lint.RuleDef{
	// Add PostgreSQL-specific rules here as needed
	// For example:
	// - Rules for PostgreSQL-specific syntax
	// - Rules for PostgreSQL anti-patterns
}
