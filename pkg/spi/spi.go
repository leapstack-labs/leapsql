// Package spi provides Service Provider Interface types for dialect
// clause handlers to interact with the parser without circular dependencies.
package spi

import (
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/token"
)

// Re-export core precedence constants for backward compatibility
const (
	PrecedenceNone       = core.PrecedenceNone
	PrecedenceOr         = core.PrecedenceOr
	PrecedenceAnd        = core.PrecedenceAnd
	PrecedenceNot        = core.PrecedenceNot
	PrecedenceComparison = core.PrecedenceComparison
	PrecedenceAddition   = core.PrecedenceAddition
	PrecedenceMultiply   = core.PrecedenceMultiply
	PrecedenceUnary      = core.PrecedenceUnary
	PrecedencePostfix    = core.PrecedencePostfix
)

// Re-export ClauseSlot constants
const (
	SlotWhere      = core.SlotWhere
	SlotGroupBy    = core.SlotGroupBy
	SlotHaving     = core.SlotHaving
	SlotWindow     = core.SlotWindow
	SlotOrderBy    = core.SlotOrderBy
	SlotLimit      = core.SlotLimit
	SlotOffset     = core.SlotOffset
	SlotQualify    = core.SlotQualify
	SlotFetch      = core.SlotFetch
	SlotExtensions = core.SlotExtensions
)

// ClauseSlot type alias for backward compatibility
type ClauseSlot = core.ClauseSlot

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

// StarModifierHandler parses a dialect-specific star modifier (e.g., EXCLUDE, REPLACE, RENAME).
// Called AFTER the modifier keyword has been consumed.
// Returns the parsed modifier or an error.
type StarModifierHandler func(p ParserOps) (StarModifier, error)

// FromItemHandler parses a dialect-specific FROM clause item (e.g., PIVOT, UNPIVOT).
// Called AFTER the keyword has been consumed.
// The sourceTable parameter is the already-parsed left-hand table.
type FromItemHandler func(p ParserOps, sourceTable TableRef) (TableRef, error)

// TableRef is a table reference node.
// This is an alias to core.TableRef for use in handler signatures.
type TableRef = core.TableRef

// StarModifier is the parsed result for star expression modifiers.
// This is an alias to core.StarModifier for use in handler signatures.
type StarModifier = core.StarModifier

// Node is the parsed result (opaque to allow returning slices and markers).
// Handlers can return core.Node implementations, slices, or marker types.
type Node interface{}

// Expr is an expression node.
// This is an alias to core.Expr for use in handler signatures.
type Expr = core.Expr

// OrderByItem represents an ORDER BY item.
// This is an alias to core.OrderByItem for use in handler signatures.
type OrderByItem = core.OrderByItem

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
