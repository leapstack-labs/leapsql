// Package postgres provides the PostgreSQL SQL dialect definition.
// This package is pure Go with no database driver dependencies.
package postgres

import "github.com/leapstack-labs/leapsql/pkg/core"

// Config is the PostgreSQL dialect configuration.
// This is pure data - accessible by both Adapter and Parser.
// The Builder reads feature flags and auto-wires standard capabilities.
var Config = &core.DialectConfig{
	Name:          "postgres",
	DefaultSchema: "public",
	Placeholder:   core.PlaceholderDollar,
	Identifiers: core.IdentifierConfig{
		Quote:         `"`,
		QuoteEnd:      `"`,
		Escape:        `""`,
		Normalization: core.NormLowercase, // Postgres normalizes unquoted to lowercase
	},

	// Framework Features (auto-wired by Builder)
	SupportsIlike:        true,
	SupportsCastOperator: true,
	SupportsReturning:    true,
	// PostgreSQL does NOT support these:
	// - QUALIFY (window filtering clause)
	// - GROUP BY ALL
	// - ORDER BY ALL
	// - SEMI/ANTI joins

	// Function classifications
	Aggregates: []string{
		// Standard aggregates
		"SUM", "COUNT", "AVG", "MIN", "MAX",
		"STDDEV", "STDDEV_POP", "STDDEV_SAMP",
		"VARIANCE", "VAR_POP", "VAR_SAMP",
		// PostgreSQL specific
		"ARRAY_AGG", "STRING_AGG",
		"JSONB_AGG", "JSONB_OBJECT_AGG", "JSON_AGG", "JSON_OBJECT_AGG",
		"BOOL_AND", "BOOL_OR", "EVERY",
		"BIT_AND", "BIT_OR", "BIT_XOR",
		"CORR", "COVAR_POP", "COVAR_SAMP",
		"REGR_AVGX", "REGR_AVGY", "REGR_COUNT", "REGR_INTERCEPT",
		"REGR_R2", "REGR_SLOPE", "REGR_SXX", "REGR_SXY", "REGR_SYY",
		"PERCENTILE_CONT", "PERCENTILE_DISC",
		"MODE",
		"XMLAGG",
	},
	Generators: []string{
		// Date/time generators
		"CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME",
		"NOW", "LOCALTIME", "LOCALTIMESTAMP",
		"STATEMENT_TIMESTAMP", "TRANSACTION_TIMESTAMP", "CLOCK_TIMESTAMP",
		// Value generators
		"GEN_RANDOM_UUID",
		"RANDOM", "SETSEED",
		// Constants
		"PI",
		// System functions
		"CURRENT_SCHEMA", "CURRENT_SCHEMAS",
		"CURRENT_DATABASE", "CURRENT_CATALOG",
		"CURRENT_USER", "CURRENT_ROLE", "SESSION_USER", "USER",
		"VERSION",
		"INET_CLIENT_ADDR", "INET_CLIENT_PORT",
		"INET_SERVER_ADDR", "INET_SERVER_PORT",
		"PG_BACKEND_PID", "PG_BLOCKING_PIDS",
		"TXID_CURRENT", "TXID_CURRENT_IF_ASSIGNED",
	},
	Windows: []string{
		// Ranking functions
		"ROW_NUMBER", "RANK", "DENSE_RANK", "NTILE",
		"PERCENT_RANK", "CUME_DIST",
		// Value functions
		"LAG", "LEAD", "FIRST_VALUE", "LAST_VALUE", "NTH_VALUE",
	},
}
