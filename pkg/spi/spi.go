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

// TableRef is a table reference node (opaque to avoid circular deps).
type TableRef interface{}

// StarModifier is the parsed result for star expression modifiers.
// This is an alias to core.StarModifier for use in handler signatures.
type StarModifier = core.StarModifier

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

// ClauseSlot specifies where a parsed clause result should be stored in SelectCore.
// This enables dialects to declaratively specify storage locations for their clauses.
type ClauseSlot int

// ClauseSlot constants define where parsed clause results are stored in SelectCore.
const (
	SlotWhere ClauseSlot = iota
	SlotGroupBy
	SlotHaving
	SlotWindow
	SlotOrderBy
	SlotLimit
	SlotOffset
	SlotQualify
	SlotFetch      // FETCH FIRST/NEXT clause
	SlotExtensions // Default for custom/dialect-specific clauses
)

// String returns the slot name for debugging.
func (s ClauseSlot) String() string {
	switch s {
	case SlotWhere:
		return "WHERE"
	case SlotGroupBy:
		return "GROUP BY"
	case SlotHaving:
		return "HAVING"
	case SlotWindow:
		return "WINDOW"
	case SlotOrderBy:
		return "ORDER BY"
	case SlotLimit:
		return "LIMIT"
	case SlotOffset:
		return "OFFSET"
	case SlotQualify:
		return "QUALIFY"
	case SlotFetch:
		return "FETCH"
	case SlotExtensions:
		return "EXTENSIONS"
	default:
		return "UNKNOWN"
	}
}
