package core

import "github.com/leapstack-labs/leapsql/pkg/token"

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

// ClauseDef bundles clause parsing logic with storage destination.
// Handler is stored as 'any' to avoid import cycles with pkg/spi.
// Consumers cast to spi.ClauseHandler when invoking.
type ClauseDef struct {
	Token    token.TokenType
	Handler  any // spi.ClauseHandler - cast at call site
	Slot     ClauseSlot
	Keywords []string
	Inline   bool
}

// OperatorDef defines an infix operator with precedence.
// Handler is stored as 'any' to avoid import cycles.
type OperatorDef struct {
	Token      token.TokenType
	Symbol     string
	Precedence int
	Handler    any // spi.InfixHandler - cast at call site
}
