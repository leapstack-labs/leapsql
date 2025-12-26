// Package databricks provides the Databricks SQL adapter for LeapSQL.
// This package registers the Databricks dialect on import.
package databricks

import (
	// Import dialect subpackage to register the Databricks dialect.
	_ "github.com/leapstack-labs/leapsql/pkg/dialects/databricks"
)
