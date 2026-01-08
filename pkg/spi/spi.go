// Package spi provides Service Provider Interface types for dialect
// clause handlers to interact with the parser without circular dependencies.
package spi

import (
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/token"
)

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
	ParseExpression() (core.Expr, error)
	ParseExpressionList() ([]core.Expr, error)
	ParseOrderByList() ([]core.OrderByItem, error)
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
type InfixHandler func(p ParserOps, left core.Expr) (core.Expr, error)

// PrefixHandler parses a dialect-specific prefix operator.
// Called AFTER the operator has been consumed.
type PrefixHandler func(p ParserOps) (core.Expr, error)

// StarModifierHandler parses a dialect-specific star modifier (e.g., EXCLUDE, REPLACE, RENAME).
// Called AFTER the modifier keyword has been consumed.
// Returns the parsed modifier or an error.
type StarModifierHandler func(p ParserOps) (core.StarModifier, error)

// FromItemHandler parses a dialect-specific FROM clause item (e.g., PIVOT, UNPIVOT).
// Called AFTER the keyword has been consumed.
// The sourceTable parameter is the already-parsed left-hand table.
type FromItemHandler func(p ParserOps, sourceTable core.TableRef) (core.TableRef, error)

// Node is the parsed result (opaque to allow returning slices and markers).
// Handlers can return core.Node implementations, slices, or marker types.
type Node interface{}

// GroupByAllMarker is an interface to identify GROUP BY ALL markers from dialect handlers.
// Implement this interface in dialect-specific marker types.
type GroupByAllMarker interface {
	IsGroupByAll() bool
}

// OrderByAllMarker is an interface to identify ORDER BY ALL markers from dialect handlers.
// Implement this interface in dialect-specific marker types.
type OrderByAllMarker interface {
	IsOrderByAll() bool
	IsDesc() bool
}
