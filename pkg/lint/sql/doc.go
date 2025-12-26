// Package sql provides SQL statement-level linting rules and analysis.
//
// This package contains:
//   - SQLRule interface and RuleDef type for defining rules
//   - Analyzer for running SQL rules against parsed statements
//   - AST utilities for rule implementation
//
// Rules are registered via init() functions and stored in the unified registry (pkg/lint).
// The analyzer retrieves rules from the registry and filters by dialect at runtime.
//
// # Example Usage
//
//	import (
//	    "github.com/leapstack-labs/leapsql/pkg/lint"
//	    "github.com/leapstack-labs/leapsql/pkg/lint/sql"
//	    _ "github.com/leapstack-labs/leapsql/pkg/lint/sql/rules" // register all rules
//	)
//
//	analyzer := sql.NewAnalyzer(lint.NewConfig(), "duckdb")
//	diagnostics := analyzer.Analyze(stmt, dialect)
package sql
