// Package rules contains all SQL lint rules for statement-level analysis.
//
// Rules are organized by prefix to indicate their category:
//
//   - al_*.go: Aliasing rules (table and column alias conventions)
//   - am_*.go: Ambiguous rules (potentially confusing constructs)
//   - cv_*.go: Convention rules (style and formatting preferences)
//   - rf_*.go: References rules (column and table reference patterns)
//   - st_*.go: Structure rules (query structure and organization)
//
// Import this package to register all SQL rules:
//
//	import _ "github.com/leapstack-labs/leapsql/pkg/lint/sql/rules"
package rules
