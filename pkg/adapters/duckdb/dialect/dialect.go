// Package dialect provides the DuckDB SQL dialect definition.
// This package is lightweight and has no database driver dependencies,
// making it suitable for use in the LSP and other tools that need
// dialect information without the overhead of database connections.
package dialect

import (
	"github.com/leapstack-labs/leapsql/pkg/dialect"
)

//go:generate go run ../../../../scripts/gendialect -dialect=duckdb -out=functions_gen.go

func init() {
	dialect.Register(DuckDB)
}

// DuckDB is the DuckDB dialect configuration.
var DuckDB = dialect.NewDialect("duckdb").
	Identifiers(`"`, `"`, `""`, dialect.NormCaseInsensitive).
	Operators(true, true). // || is concat, CONCAT coalesces NULL
	Aggregates(duckDBAggregates...).
	Generators(duckDBGenerators...).
	Windows(duckDBWindows...).
	TableFunctions(duckDBTableFunctions...).
	WithDocs(duckDBFunctionDocs).
	WithDocs(duckDBWindowDocs).
	Build()
