// Package postgres provides a PostgreSQL database adapter for LeapSQL.
//
// This file registers the PostgreSQL adapter with the adapter registry.
// Import this package with a blank identifier to register the adapter:
//
//	import _ "github.com/leapstack-labs/leapsql/pkg/adapters/postgres"
package postgres

import (
	"log/slog"

	"github.com/leapstack-labs/leapsql/pkg/adapter"

	// Import dialect to ensure it's registered
	_ "github.com/leapstack-labs/leapsql/pkg/dialects/postgres"
)

func init() {
	adapter.Register("postgres", func(logger *slog.Logger) adapter.Adapter { return New(logger) })
}
