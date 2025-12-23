// Package projectrules registers all project health lint rules.
// Import this package to register all project-level rules with the global registry.
package projectrules

import (
	// Blank imports trigger init() functions that register rules with the global registry.
	_ "github.com/leapstack-labs/leapsql/pkg/lint/project/rules/lineage"   // registers PL* rules
	_ "github.com/leapstack-labs/leapsql/pkg/lint/project/rules/modeling"  // registers PM* rules
	_ "github.com/leapstack-labs/leapsql/pkg/lint/project/rules/structure" // registers PS* rules
)
