// Package dialect provides the PostgreSQL SQL dialect definition.
// This package is lightweight and has no database driver dependencies,
// making it suitable for use in the LSP and other tools that need
// dialect information without the overhead of database connections.
package dialect

import (
	"github.com/leapstack-labs/leapsql/pkg/dialect"
)

func init() {
	dialect.Register(Postgres)
}

// postgresReservedWords contains common PostgreSQL reserved words.
// This is a manually maintained list of frequently problematic identifiers.
// For a complete list, use pg_get_keywords() at runtime.
var postgresReservedWords = []string{
	"user", "order", "group", "table", "select", "from", "where", "index",
	"all", "and", "any", "array", "as", "asc", "asymmetric", "authorization",
	"between", "binary", "both", "case", "cast", "check", "collate", "column",
	"constraint", "create", "cross", "current_catalog", "current_date",
	"current_role", "current_schema", "current_time", "current_timestamp",
	"current_user", "default", "deferrable", "desc", "distinct", "do", "else",
	"end", "except", "false", "fetch", "for", "foreign", "freeze", "full",
	"grant", "having", "ilike", "in", "initially", "inner", "intersect",
	"into", "is", "isnull", "join", "lateral", "leading", "left", "like",
	"limit", "localtime", "localtimestamp", "natural", "not", "notnull",
	"null", "offset", "on", "only", "or", "outer", "overlaps", "placing",
	"primary", "references", "returning", "right", "session_user", "similar",
	"some", "symmetric", "then", "to", "trailing", "true", "union", "unique",
	"using", "variadic", "verbose", "when", "window", "with",
}

// Postgres is the PostgreSQL dialect configuration.
var Postgres = dialect.NewDialect("postgres").
	Identifiers(`"`, `"`, `""`, dialect.NormLowercase). // Postgres normalizes unquoted identifiers to lowercase
	Operators(true, false).                             // || is concat, CONCAT does NOT coalesce NULL
	DefaultSchema("public").
	PlaceholderStyle(dialect.PlaceholderDollar).
	Aggregates(
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
	).
	Generators(
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
	).
	Windows(
		// Ranking functions
		"ROW_NUMBER", "RANK", "DENSE_RANK", "NTILE",
		"PERCENT_RANK", "CUME_DIST",
		// Value functions
		"LAG", "LEAD", "FIRST_VALUE", "LAST_VALUE", "NTH_VALUE",
	).
	WithReservedWords(postgresReservedWords...).
	Build()
