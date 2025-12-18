// Package spi provides Service Provider Interface types for dialect
// clause handlers to interact with the parser without circular dependencies.
package spi

import "github.com/leapstack-labs/leapsql/pkg/token"

// ParserOps exposes parser operations to dialect clause handlers.
// This interface allows dialect-specific code to interact with the parser
// without creating circular dependencies.
type ParserOps interface {
	// Token access
	Token() token.Token
	Peek() token.Token

	// Consumption
	Match(t token.TokenType) bool
	Expect(t token.TokenType) error
	NextToken()
	Check(t token.TokenType) bool

	// Sub-parsers
	ParseExpression() (Expr, error)
	ParseExpressionList() ([]Expr, error)
	ParseOrderByList() ([]OrderByItem, error)
	ParseIdentifier() (string, error)

	// Error handling
	AddError(msg string)
	Position() token.Position
}

// ClauseHandler parses a dialect-specific clause.
// Called AFTER the clause keyword has been consumed.
// Returns the parsed node or an error.
type ClauseHandler func(p ParserOps) (Node, error)

// InfixHandler parses a dialect-specific infix operator.
// Called AFTER the operator has been consumed.
// left is the already-parsed left operand.
type InfixHandler func(p ParserOps, left Expr) (Expr, error)

// PrefixHandler parses a dialect-specific prefix operator.
// Called AFTER the operator has been consumed.
type PrefixHandler func(p ParserOps) (Expr, error)

// Node is the parsed result (opaque to avoid circular deps).
type Node interface{}

// Expr is an expression node.
type Expr interface{}

// OrderByItem represents an ORDER BY item.
type OrderByItem interface{}

// Precedence constants for operator precedence parsing.
const (
	PrecedenceNone       = 0
	PrecedenceOr         = 1
	PrecedenceAnd        = 2
	PrecedenceNot        = 3
	PrecedenceComparison = 4 // =, <>, <, >, <=, >=, LIKE, ILIKE, IN, BETWEEN
	PrecedenceAddition   = 5 // +, -, ||
	PrecedenceMultiply   = 6 // *, /, %
	PrecedenceUnary      = 7 // -, +, NOT
	PrecedencePostfix    = 8 // ::, [], ()
)
