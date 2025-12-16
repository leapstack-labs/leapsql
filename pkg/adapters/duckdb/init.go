// Package duckdb provides a DuckDB database adapter for LeapSQL.
//
// This file registers the DuckDB adapter with the adapter registry.
// Import this package with a blank identifier to register the adapter:
//
//	import _ "github.com/leapstack-labs/leapsql/pkg/adapters/duckdb"
package duckdb

import (
	"github.com/leapstack-labs/leapsql/pkg/adapter"

	// Import dialect to ensure it's registered
	_ "github.com/leapstack-labs/leapsql/pkg/adapters/duckdb/dialect"
)

func init() {
	adapter.Register("duckdb", func() adapter.Adapter { return New() })
}
