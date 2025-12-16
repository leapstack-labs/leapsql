package sql

// PostgreSQL dialect definition.

func init() {
	RegisterDialect(Postgres)
}

// Postgres is the PostgreSQL dialect configuration.
var Postgres = NewDialect("postgres").
	Identifiers(`"`, `"`, `""`, NormLowercase). // Postgres normalizes unquoted identifiers to lowercase
	Operators(true, false).                     // || is concat, CONCAT does NOT coalesce NULL
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
	Aliases(map[string]string{
		// NULL handling
		"IFNULL": "COALESCE",
		"NVL":    "COALESCE",
		// String functions
		"SUBSTR":      "SUBSTRING",
		"LEN":         "LENGTH",
		"CHAR_LENGTH": "LENGTH",
		"UCASE":       "UPPER",
		"LCASE":       "LOWER",
	}).
	Build()
