// Package snowflake provides the Snowflake SQL dialect definition.
// This package is pure Go with no database driver dependencies.
package snowflake

import "github.com/leapstack-labs/leapsql/pkg/core"

// Config is the Snowflake SQL dialect configuration.
// This is pure data - accessible by both Adapter and Parser.
// The Builder reads feature flags and auto-wires standard capabilities.
var Config = &core.DialectConfig{
	Name:          "snowflake",
	DefaultSchema: "PUBLIC",
	Placeholder:   core.PlaceholderQuestion,
	Identifiers: core.IdentifierConfig{
		Quote:         `"`,
		QuoteEnd:      `"`,
		Escape:        `""`,
		Normalization: core.NormUppercase, // Snowflake normalizes to uppercase
	},

	// Framework Features (auto-wired by Builder)
	SupportsQualify:      true,
	SupportsIlike:        true,
	SupportsCastOperator: true, // :: operator

	// Snowflake does NOT support these:
	// - GROUP BY ALL
	// - ORDER BY ALL
	// - SEMI/ANTI joins (as keywords - they use different syntax)

	// Function classifications (populated from generated files)
	Aggregates:     snowflakeAggregates,
	Generators:     snowflakeGenerators,
	Windows:        snowflakeWindows,
	TableFunctions: snowflakeTableFunctions,
	Keywords:       snowflakeCompletionKeywords,
	DataTypes:      snowflakeTypes,
}
