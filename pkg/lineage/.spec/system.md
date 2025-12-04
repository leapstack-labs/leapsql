# System Constraints & Philosophy

## Design Goals

We are building a **column-level lineage library for Go**.

1. **Target Dialect:** DuckDB (ANSI compliant) first.
2. **Parser Type:** Recursive Descent.
3. **Philosophy:** Opinionated. We enforce best practices by rejecting complex DML patterns in favor of clean transformation models.

## STRICT Constraints (DO NOT VIOLATE)

The parser MUST return an error if it encounters the following patterns. Do not attempt to parse them.

| Constraint                      | Error Message to User                                                                  |
| :------------------------------ | :------------------------------------------------------------------------------------- |
| **Scalar Subqueries in SELECT** | "Scalar subqueries in SELECT columns are not supported. Rewrite using a CTE and JOIN." |
| **Top-level DML**               | "Models must start with SELECT or WITH."                                               |

## Allowed Patterns

- **Recursive CTEs:** Must be supported (topological sort required for resolution).
- **Unions/Intersects:** Allowed.
- **Derived Tables:** Allowed in FROM clauses.

## Dialect Configuration (DuckDB)

- **Identifiers:** Double quotes (`"col"`). Case-insensitive.
- **Strings:** Single quotes (`'val'`). Escaped by doubling (`''`).
- **Concat:** `||` operator.
