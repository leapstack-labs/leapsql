// Package ambiguous provides lint rules for detecting ambiguous SQL constructs.
// These rules follow SQLFluff's AM (Ambiguous) rule category.
//
// Rules in this package:
//   - AM01: DISTINCT used with GROUP BY (redundant)
//   - AM02: UNION vs UNION DISTINCT ambiguity
//   - AM03: ORDER BY with ambiguous column in set operations
//   - AM04: Column count mismatch in set operations
//   - AM05: Implicit cross join (comma syntax)
//   - AM06: Ambiguous column references
//   - AM07: Set operation column count mismatch
//   - AM08: Join condition references wrong tables
//   - AM09: ORDER BY/LIMIT with set operations
package ambiguous
