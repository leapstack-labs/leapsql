package rules

// Import all rule subpackages to register them with the global registry.
// This file triggers all init() functions in the rule packages.
import (
	// Import rule categories - each registers its rules via init()
	_ "github.com/leapstack-labs/leapsql/pkg/lint/rules/aliasing"
	_ "github.com/leapstack-labs/leapsql/pkg/lint/rules/ambiguous"
	_ "github.com/leapstack-labs/leapsql/pkg/lint/rules/convention"
	_ "github.com/leapstack-labs/leapsql/pkg/lint/rules/references"
	_ "github.com/leapstack-labs/leapsql/pkg/lint/rules/structure"
)
