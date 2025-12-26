// Package rules contains all SQL lint rules.
// Import this package to register all SQL rules with the unified registry.
//
// Rules are automatically registered via init() functions when this package is imported:
//
//	import _ "github.com/leapstack-labs/leapsql/pkg/lint/sql/rules"
//
// Rule Categories:
//   - AL (Aliasing): Rules about alias usage and naming
//   - AM (Ambiguous): Rules about ambiguous SQL constructs
//   - CV (Convention): Rules about SQL coding conventions
//   - RF (References): Rules about column and table references
//   - ST (Structure): Rules about SQL query structure
package rules

// All rules are registered via init() functions in their respective files.
// This file exists to provide package documentation and ensure the package
// can be imported for its side effects (rule registration).
//
// Importing this package will register all the following rules:
//
// Aliasing rules:
//   - AL03: Expression Alias - Expressions should be aliased
//   - AL04: Unique Table - Table aliases must be unique
//   - AL05: Unused Alias - Aliases must be used if defined
//   - AL06: Alias Length - Alias length requirements
//   - AL07: Forbid Alias - Forbid certain alias patterns
//   - AL08: Unique Column - Column aliases must be unique
//   - AL09: Self Alias - Table should not alias to itself
//
// Ambiguous rules:
//   - AM01: Distinct - DISTINCT with GROUP BY is redundant
//   - AM02: Union - UNION vs UNION ALL
//   - AM03: Order By - ORDER BY usage
//   - AM04: Column Count - Star in subquery context
//   - AM05: Join - JOIN without condition
//   - AM06: Column Refs - Ambiguous column references
//   - AM08: Join Condition - Missing join condition
//   - AM09: Order By Limit - ORDER BY without LIMIT
//
// Convention rules:
//   - CV01: Not Equal - Prefer != over <>
//   - CV02: Coalesce - Prefer COALESCE over IFNULL/NVL
//   - CV04: Count Rows - Prefer COUNT(*) over COUNT(1)
//   - CV05: Is Null - Use IS NULL instead of = NULL
//   - CV08: Left Join - Prefer LEFT JOIN over RIGHT JOIN
//   - CV09: Blocked Words - Block dangerous SQL keywords
//
// References rules:
//   - RF02: Qualification - Qualify columns in multi-table queries
//   - RF03: Consistent - Consistent column qualification style
//
// Structure rules:
//   - ST01: Else Null - ELSE NULL is redundant
//   - ST02: Simple Case - Simplify searched CASE to simple CASE
//   - ST03: Unused CTE - CTE is defined but never used
//   - ST04: Nested Case - Avoid nested CASE expressions
//   - ST06: Column Order - Wildcards should appear last
//   - ST07: Using - Prefer USING for same-named column joins
//   - ST08: Distinct - Consider GROUP BY instead of DISTINCT
//   - ST09: Join Condition Order - Left table first in join conditions
//   - ST10: Constant Expression - Unnecessary constant expressions
