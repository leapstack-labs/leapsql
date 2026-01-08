// Package dialect provides SQL dialect configuration and function classification.
//
// This file contains join type definitions that form the "toolbox" of
// reusable join configurations. These can be composed into any dialect.
package dialect

import (
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/token"
)

// ANSIJoinTypes contains standard SQL join types.
var ANSIJoinTypes = []core.JoinTypeDef{
	{
		Token:       token.INNER,
		Type:        core.JoinInner,
		RequiresOn:  true,
		AllowsUsing: true,
	},
	{
		Token:         token.LEFT,
		Type:          core.JoinLeft,
		OptionalToken: token.OUTER,
		RequiresOn:    true,
		AllowsUsing:   true,
	},
	{
		Token:         token.RIGHT,
		Type:          core.JoinRight,
		OptionalToken: token.OUTER,
		RequiresOn:    true,
		AllowsUsing:   true,
	},
	{
		Token:         token.FULL,
		Type:          core.JoinFull,
		OptionalToken: token.OUTER,
		RequiresOn:    true,
		AllowsUsing:   true,
	},
	{
		Token:       token.CROSS,
		Type:        core.JoinCross,
		RequiresOn:  false,
		AllowsUsing: false,
	},
}
