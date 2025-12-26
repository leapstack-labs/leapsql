// Package duckdb provides a DuckDB database adapter for LeapSQL.
//
// This file registers the DuckDB adapter with the adapter registry.
// Import this package with a blank identifier to register the adapter:
//
//	import _ "github.com/leapstack-labs/leapsql/pkg/adapters/duckdb"
package duckdb

import (
	"log/slog"

	"github.com/leapstack-labs/leapsql/pkg/adapter"

	// Import dialect to ensure it's registered
	_ "github.com/leapstack-labs/leapsql/pkg/dialects/duckdb"
)

func init() {
	adapter.Register("duckdb", func(logger *slog.Logger) adapter.Adapter { return New(logger) })
}
