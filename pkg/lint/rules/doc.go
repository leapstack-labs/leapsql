// Package rules provides SQLFluff-style lint rule implementations for LeapSQL.
//
// Rules are organized by category following SQLFluff's naming conventions:
//   - ambiguous: Rules detecting ambiguous SQL constructs (AM01-AM09)
//   - structure: Rules about query structure and style (ST01-ST12)
//   - convention: Rules about SQL conventions (CV01-CV12)
//   - aliasing: Rules about table and column aliasing (AL03-AL09)
//   - references: Rules about column qualification (RF02-RF03)
//
// To register all rules with the global lint registry, import this package
// with a blank identifier:
//
//	import _ "github.com/leapstack-labs/leapsql/pkg/lint/rules"
//
// Individual rule categories can also be imported:
//
//	import _ "github.com/leapstack-labs/leapsql/pkg/lint/rules/ambiguous"
//	import _ "github.com/leapstack-labs/leapsql/pkg/lint/rules/structure"
package rules
