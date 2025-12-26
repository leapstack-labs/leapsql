// Package dialect provides SQL dialect configuration and function classification.
//
// This file contains join type definitions that form the "toolbox" of
// reusable join configurations. These can be composed into any dialect.
package dialect

import (
	"github.com/leapstack-labs/leapsql/pkg/token"
)

// Standard ANSI SQL join type values.
// These are defined as untyped string constants for easy use.
const (
	JoinInner = "INNER"
	JoinLeft  = "LEFT"
	JoinRight = "RIGHT"
	JoinFull  = "FULL"
	JoinCross = "CROSS"
)

// ANSIJoinTypes contains standard SQL join types.
var ANSIJoinTypes = []JoinTypeDef{
	{
		Token:       token.INNER,
		Type:        JoinInner,
		RequiresOn:  true,
		AllowsUsing: true,
	},
	{
		Token:         token.LEFT,
		Type:          JoinLeft,
		OptionalToken: token.OUTER,
		RequiresOn:    true,
		AllowsUsing:   true,
	},
	{
		Token:         token.RIGHT,
		Type:          JoinRight,
		OptionalToken: token.OUTER,
		RequiresOn:    true,
		AllowsUsing:   true,
	},
	{
		Token:         token.FULL,
		Type:          JoinFull,
		OptionalToken: token.OUTER,
		RequiresOn:    true,
		AllowsUsing:   true,
	},
	{
		Token:       token.CROSS,
		Type:        JoinCross,
		RequiresOn:  false,
		AllowsUsing: false,
	},
}
