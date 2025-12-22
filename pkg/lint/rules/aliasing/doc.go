// Package aliasing provides lint rules for SQL aliasing conventions.
// These rules follow SQLFluff's AL (Aliasing) rule category.
//
// Rules in this package:
//   - AL03: Expression columns should have aliases
//   - AL04: Unique table aliases
//   - AL05: Unused table alias
//   - AL06: Alias length constraints
//   - AL07: Forbidden alias patterns
//   - AL08: Unique column aliases
//   - AL09: Table aliased to its own name
package aliasing
