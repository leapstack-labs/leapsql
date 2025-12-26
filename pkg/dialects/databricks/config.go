// Package databricks provides the Databricks SQL dialect definition.
// This package is pure Go with no database driver dependencies.
package databricks

import "github.com/leapstack-labs/leapsql/pkg/core"

// Config is the Databricks SQL dialect configuration.
// This is pure data - accessible by both Adapter and Parser.
// The Builder reads feature flags and auto-wires standard capabilities.
var Config = &core.DialectConfig{
	Name:          "databricks",
	DefaultSchema: "default",
	Placeholder:   core.PlaceholderQuestion,
	Identifiers: core.IdentifierConfig{
		Quote:         "`",
		QuoteEnd:      "`",
		Escape:        "``",
		Normalization: core.NormCaseInsensitive,
	},

	// Framework Features (auto-wired by Builder)
	SupportsQualify:       true,
	SupportsIlike:         true,
	SupportsCastOperator:  true,
	SupportsSemiAntiJoins: true,
	// Databricks does NOT support these:
	// - GROUP BY ALL
	// - ORDER BY ALL
	// - RETURNING clause

	// Function classifications (populated from generated files and metadata)
	Aggregates:     databricksAggregates,
	Generators:     databricksGenerators,
	Windows:        databricksWindows,
	TableFunctions: databricksTableFunctions,
	Keywords:       databricksCompletionKeywords,
	DataTypes:      databricksTypes,
}
