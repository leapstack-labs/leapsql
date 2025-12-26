// Package ansi provides the base ANSI SQL dialect with standard clause sequences,
// handlers, and operator precedence.
//
// This dialect serves as the foundation for all other SQL dialects. It uses the
// toolbox composition pattern with explicit clause definitions from pkg/dialect.
package ansi

import (
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/dialect"
)

func init() {
	dialect.Register(ANSI)
}

// ANSI is the base ANSI SQL dialect.
// It defines standard clause sequence, handlers, and operator precedence
// using explicit composition from the dialect toolbox.
var ANSI = dialect.NewDialect("ansi").
	// Configuration
	Identifiers(`"`, `"`, `""`, core.NormLowercase).
	PlaceholderStyle(core.PlaceholderQuestion).
	// Clause Sequence - explicit from toolbox
	Clauses(dialect.StandardSelectClauses...).
	// Operators - standard ANSI operators
	Operators(dialect.ANSIOperators).
	// Join Types - standard ANSI join types
	JoinTypes(dialect.ANSIJoinTypes).
	// Lint rules (defined in rules.go)
	LintRulesAdd(AllRules...).
	Build()
