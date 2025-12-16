// Package dialect provides the DuckDB SQL dialect definition.
// This package is lightweight and has no database driver dependencies,
// making it suitable for use in the LSP and other tools that need
// dialect information without the overhead of database connections.
package dialect

import (
	"github.com/leapstack-labs/leapsql/pkg/dialect"
)

func init() {
	dialect.Register(DuckDB)
}

// DuckDB is the DuckDB dialect configuration.
var DuckDB = dialect.NewDialect("duckdb").
	Identifiers(`"`, `"`, `""`, dialect.NormCaseInsensitive).
	Operators(true, true). // || is concat, CONCAT coalesces NULL
	Aggregates(
		// Standard aggregates
		"SUM", "COUNT", "AVG", "MIN", "MAX",
		"STDDEV", "STDDEV_POP", "STDDEV_SAMP",
		"VARIANCE", "VAR_POP", "VAR_SAMP",
		// DuckDB specific
		"LIST", "ARRAY_AGG", "STRING_AGG", "GROUP_CONCAT",
		"FIRST", "LAST", "ANY_VALUE", "ARBITRARY",
		"MEDIAN", "MODE", "QUANTILE", "QUANTILE_CONT", "QUANTILE_DISC",
		"APPROX_COUNT_DISTINCT", "APPROX_QUANTILE",
		"HISTOGRAM", "ENTROPY", "KURTOSIS", "SKEWNESS",
		"BIT_AND", "BIT_OR", "BIT_XOR", "BOOL_AND", "BOOL_OR",
		"CORR", "COVAR_POP", "COVAR_SAMP", "REGR_AVGX", "REGR_AVGY",
		"REGR_COUNT", "REGR_INTERCEPT", "REGR_R2", "REGR_SLOPE",
		"REGR_SXX", "REGR_SXY", "REGR_SYY",
		"PRODUCT", "FSUM", "FAVG",
		// Statistical
		"MAD", "RESERVOIR_QUANTILE",
	).
	Generators(
		// Date/time generators
		"CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME",
		"NOW", "TODAY",
		"LOCALTIME", "LOCALTIMESTAMP",
		// Value generators
		"UUID", "GEN_RANDOM_UUID",
		"RANDOM", "SETSEED",
		// Constants
		"PI", "E",
		// System functions
		"CURRENT_SCHEMA", "CURRENT_DATABASE", "CURRENT_CATALOG",
		"VERSION",
	).
	Windows(
		// Ranking functions
		"ROW_NUMBER", "RANK", "DENSE_RANK", "NTILE", "PERCENT_RANK", "CUME_DIST",
		// Value functions
		"LAG", "LEAD", "FIRST_VALUE", "LAST_VALUE", "NTH_VALUE",
	).
	Aliases(map[string]string{
		// NULL handling
		"IFNULL": "COALESCE",
		"NVL":    "COALESCE",
		"NVL2":   "IF",
		// String functions
		"SUBSTR":      "SUBSTRING",
		"LEN":         "LENGTH",
		"CHAR_LENGTH": "LENGTH",
		"UCASE":       "UPPER",
		"LCASE":       "LOWER",
		// Aggregate aliases
		"COLLECT_LIST": "LIST",
		"COLLECT_SET":  "LIST",
		// Array
		"ARRAY_LENGTH": "LEN",
	}).
	Build()
