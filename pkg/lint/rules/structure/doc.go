// Package structure provides lint rules for SQL query structure.
// These rules follow SQLFluff's ST (Structure) rule category.
//
// Rules in this package:
//   - ST01: Redundant ELSE NULL in CASE expressions
//   - ST02: Simple CASE vs searched CASE
//   - ST03: Unused CTE definition
//   - ST04: Nested CASE statements
//   - ST06: SELECT column ordering (wildcards last)
//   - ST07: Prefer USING over ON for equality joins
//   - ST08: DISTINCT vs GROUP BY
//   - ST09: Join condition column order
package structure
