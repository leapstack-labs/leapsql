// Package dialect provides SQL dialect configuration and function classification.
//
// This file contains operator definitions that form the "toolbox" of
// reusable operator configurations. These can be composed into any dialect.
package dialect

import (
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/token"
)

// ANSIOperators contains standard SQL operators with their precedence.
var ANSIOperators = []core.OperatorDef{
	// Logical operators (lowest precedence)
	{Token: token.OR, Precedence: core.PrecedenceOr},
	{Token: token.AND, Precedence: core.PrecedenceAnd},

	// Comparison operators
	{Token: token.EQ, Precedence: core.PrecedenceComparison},
	{Token: token.NE, Precedence: core.PrecedenceComparison},
	{Token: token.LT, Precedence: core.PrecedenceComparison},
	{Token: token.GT, Precedence: core.PrecedenceComparison},
	{Token: token.LE, Precedence: core.PrecedenceComparison},
	{Token: token.GE, Precedence: core.PrecedenceComparison},
	{Token: token.LIKE, Precedence: core.PrecedenceComparison},
	{Token: token.IN, Precedence: core.PrecedenceComparison},
	{Token: token.BETWEEN, Precedence: core.PrecedenceComparison},
	{Token: token.IS, Precedence: core.PrecedenceComparison},

	// Arithmetic operators
	{Token: token.PLUS, Precedence: core.PrecedenceAddition},
	{Token: token.MINUS, Precedence: core.PrecedenceAddition},
	{Token: token.DPIPE, Precedence: core.PrecedenceAddition}, // || string concatenation

	// Multiplicative operators (highest precedence for binary ops)
	{Token: token.STAR, Precedence: core.PrecedenceMultiply},
	{Token: token.SLASH, Precedence: core.PrecedenceMultiply},
	{Token: token.MOD, Precedence: core.PrecedenceMultiply},
}
