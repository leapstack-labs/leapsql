// Package duckdb provides the DuckDB SQL dialect implementation.
//
// This file contains static metadata for DuckDB functions that are not
// exposed via duckdb_functions() or require manual documentation.
package duckdb

import "github.com/leapstack-labs/leapsql/pkg/dialect"

// duckDBWindows contains window function names.
// DuckDB doesn't expose window function metadata via duckdb_functions(),
// so we maintain this list manually.
var duckDBWindows = []string{
	"cume_dist",
	"dense_rank",
	"first_value",
	"lag",
	"last_value",
	"lead",
	"nth_value",
	"ntile",
	"percent_rank",
	"rank",
	"row_number",
}

// duckDBWindowDocs provides documentation for window functions.
var duckDBWindowDocs = map[string]dialect.FunctionDoc{
	"row_number": {
		Description: "Sequential row number starting from 1",
		Signatures:  []string{"row_number() OVER(...) -> BIGINT"},
		ReturnType:  "BIGINT",
	},
	"rank": {
		Description: "Rank with gaps for ties",
		Signatures:  []string{"rank() OVER(...) -> BIGINT"},
		ReturnType:  "BIGINT",
	},
	"dense_rank": {
		Description: "Rank without gaps for ties",
		Signatures:  []string{"dense_rank() OVER(...) -> BIGINT"},
		ReturnType:  "BIGINT",
	},
	"ntile": {
		Description: "Divide rows into n buckets",
		Signatures:  []string{"ntile(n INTEGER) OVER(...) -> BIGINT"},
		ReturnType:  "BIGINT",
	},
	"lag": {
		Description: "Value from previous row",
		Signatures:  []string{"lag(expr ANY, offset INTEGER, default ANY) OVER(...) -> ANY"},
		ReturnType:  "ANY",
	},
	"lead": {
		Description: "Value from following row",
		Signatures:  []string{"lead(expr ANY, offset INTEGER, default ANY) OVER(...) -> ANY"},
		ReturnType:  "ANY",
	},
	"first_value": {
		Description: "First value in window frame",
		Signatures:  []string{"first_value(expr ANY) OVER(...) -> ANY"},
		ReturnType:  "ANY",
	},
	"last_value": {
		Description: "Last value in window frame",
		Signatures:  []string{"last_value(expr ANY) OVER(...) -> ANY"},
		ReturnType:  "ANY",
	},
	"nth_value": {
		Description: "Nth value in window frame",
		Signatures:  []string{"nth_value(expr ANY, n INTEGER) OVER(...) -> ANY"},
		ReturnType:  "ANY",
	},
	"percent_rank": {
		Description: "Relative rank as percentage (0-1)",
		Signatures:  []string{"percent_rank() OVER(...) -> DOUBLE"},
		ReturnType:  "DOUBLE",
	},
	"cume_dist": {
		Description: "Cumulative distribution (0-1)",
		Signatures:  []string{"cume_dist() OVER(...) -> DOUBLE"},
		ReturnType:  "DOUBLE",
	},
}

// duckDBGenerators contains generator function names.
// These produce values without input columns.
var duckDBGenerators = []string{
	"current_catalog",
	"current_database",
	"current_date",
	"current_schema",
	"current_time",
	"current_timestamp",
	"e",
	"gen_random_uuid",
	"localtime",
	"localtimestamp",
	"now",
	"pi",
	"random",
	"today",
	"uuid",
	"version",
}
