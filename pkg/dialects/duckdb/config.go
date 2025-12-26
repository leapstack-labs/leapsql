// Package duckdb provides the DuckDB SQL dialect definition.
// This package is pure Go with no database driver dependencies.
package duckdb

import "github.com/leapstack-labs/leapsql/pkg/core"

// Config is the DuckDB dialect configuration.
// This is pure data - accessible by both Adapter and Parser.
// The Builder reads feature flags and auto-wires standard capabilities.
var Config = &core.DialectConfig{
	Name:          "duckdb",
	DefaultSchema: "main",
	Placeholder:   core.PlaceholderQuestion,
	Identifiers: core.IdentifierConfig{
		Quote:         `"`,
		QuoteEnd:      `"`,
		Escape:        `""`,
		Normalization: core.NormCaseInsensitive,
	},

	// Framework Features (auto-wired by Builder)
	SupportsQualify:       true,
	SupportsIlike:         true,
	SupportsCastOperator:  true,
	SupportsSemiAntiJoins: true,
	SupportsGroupByAll:    true,
	SupportsOrderByAll:    true,
	SupportsReturning:     true,

	// Function classifications (populated from generated files and metadata)
	Aggregates:     duckDBAggregates,
	Generators:     duckDBGenerators,
	Windows:        duckDBWindows,
	TableFunctions: duckDBTableFunctions,
	Keywords:       duckDBCompletionKeywords,
	DataTypes:      duckDBTypes,
}
