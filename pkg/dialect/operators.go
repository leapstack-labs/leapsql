// Package dialect provides SQL dialect configuration and function classification.
//
// This file contains operator definitions that form the "toolbox" of
// reusable operator configurations. These can be composed into any dialect.
package dialect

import (
	"github.com/leapstack-labs/leapsql/pkg/spi"
	"github.com/leapstack-labs/leapsql/pkg/token"
)

// OperatorDef defines an infix operator with precedence.
// Symbol is optional - if provided, it's registered with the lexer.
type OperatorDef struct {
	Token      token.TokenType  // The token type for this operator
	Symbol     string           // Optional symbol for lexer registration (e.g. "::")
	Precedence int              // Operator precedence level
	Handler    spi.InfixHandler // Optional custom handler (nil for standard binary ops)
}

// ANSIOperators contains standard SQL operators with their precedence.
var ANSIOperators = []OperatorDef{
	// Logical operators (lowest precedence)
	{Token: token.OR, Precedence: spi.PrecedenceOr},
	{Token: token.AND, Precedence: spi.PrecedenceAnd},

	// Comparison operators
	{Token: token.EQ, Precedence: spi.PrecedenceComparison},
	{Token: token.NE, Precedence: spi.PrecedenceComparison},
	{Token: token.LT, Precedence: spi.PrecedenceComparison},
	{Token: token.GT, Precedence: spi.PrecedenceComparison},
	{Token: token.LE, Precedence: spi.PrecedenceComparison},
	{Token: token.GE, Precedence: spi.PrecedenceComparison},
	{Token: token.LIKE, Precedence: spi.PrecedenceComparison},
	{Token: token.IN, Precedence: spi.PrecedenceComparison},
	{Token: token.BETWEEN, Precedence: spi.PrecedenceComparison},
	{Token: token.IS, Precedence: spi.PrecedenceComparison},

	// Arithmetic operators
	{Token: token.PLUS, Precedence: spi.PrecedenceAddition},
	{Token: token.MINUS, Precedence: spi.PrecedenceAddition},
	{Token: token.DPIPE, Precedence: spi.PrecedenceAddition}, // || string concatenation

	// Multiplicative operators (highest precedence for binary ops)
	{Token: token.STAR, Precedence: spi.PrecedenceMultiply},
	{Token: token.SLASH, Precedence: spi.PrecedenceMultiply},
	{Token: token.MOD, Precedence: spi.PrecedenceMultiply},
}
