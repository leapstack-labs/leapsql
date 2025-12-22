// Package convention provides lint rules for SQL conventions.
// These rules follow SQLFluff's CV (Convention) rule category.
//
// Rules in this package:
//   - CV01: Prefer != over <> for not equal
//   - CV02: Prefer COALESCE over IFNULL/NVL
//   - CV04: COUNT(*) vs COUNT(1) consistency
//   - CV05: Use IS NULL instead of = NULL
//   - CV08: Prefer LEFT JOIN over RIGHT JOIN
//   - CV09: Block dangerous keywords (DELETE, DROP, TRUNCATE)
//   - CV11: Consistent casting style (CAST vs ::)
package convention
