// Package ansi provides the base ANSI SQL dialect with standard clause sequences,
// handlers, and operator precedence.
//
// This dialect serves as the foundation for all other SQL dialects. Dialects like
// DuckDB or PostgreSQL can extend ANSI and add/override specific behaviors.
package ansi

import (
	"github.com/leapstack-labs/leapsql/pkg/dialect"
	"github.com/leapstack-labs/leapsql/pkg/spi"
	"github.com/leapstack-labs/leapsql/pkg/token"
)

func init() {
	dialect.Register(ANSI)
}

// ANSI is the base ANSI SQL dialect.
// It defines standard clause sequence, handlers, and operator precedence.
var ANSI = dialect.NewDialect("ansi").
	// Clause parsing order
	ClauseSequence(
		token.WHERE,
		token.GROUP, // GROUP BY
		token.HAVING,
		token.WINDOW, // Named window definitions
		token.ORDER,  // ORDER BY
		token.LIMIT,
		token.OFFSET,
	).
	// Standard clause handlers with slot assignments
	ClauseHandler(token.WHERE, parseWhere, spi.SlotWhere).
	ClauseHandler(token.GROUP, parseGroupBy, spi.SlotGroupBy, dialect.WithKeywords("GROUP", "BY")).
	ClauseHandler(token.HAVING, parseHaving, spi.SlotHaving).
	ClauseHandler(token.WINDOW, parseWindow, spi.SlotWindow).
	ClauseHandler(token.ORDER, parseOrderBy, spi.SlotOrderBy, dialect.WithKeywords("ORDER", "BY")).
	ClauseHandler(token.LIMIT, parseLimit, spi.SlotLimit, dialect.WithInline()).
	ClauseHandler(token.OFFSET, parseOffset, spi.SlotOffset, dialect.WithInline()).
	// Standard operator precedence
	AddInfix(token.OR, spi.PrecedenceOr).
	AddInfix(token.AND, spi.PrecedenceAnd).
	AddInfix(token.EQ, spi.PrecedenceComparison).
	AddInfix(token.NE, spi.PrecedenceComparison).
	AddInfix(token.LT, spi.PrecedenceComparison).
	AddInfix(token.GT, spi.PrecedenceComparison).
	AddInfix(token.LE, spi.PrecedenceComparison).
	AddInfix(token.GE, spi.PrecedenceComparison).
	AddInfix(token.LIKE, spi.PrecedenceComparison).
	AddInfix(token.IN, spi.PrecedenceComparison).
	AddInfix(token.BETWEEN, spi.PrecedenceComparison).
	AddInfix(token.IS, spi.PrecedenceComparison).
	AddInfix(token.PLUS, spi.PrecedenceAddition).
	AddInfix(token.MINUS, spi.PrecedenceAddition).
	AddInfix(token.DPIPE, spi.PrecedenceAddition).
	AddInfix(token.STAR, spi.PrecedenceMultiply).
	AddInfix(token.SLASH, spi.PrecedenceMultiply).
	AddInfix(token.PERCENT, spi.PrecedenceMultiply).
	// Lint rules
	LintRulesAdd(AllRules...).
	// Config
	Identifiers(`"`, `"`, `""`, dialect.NormLowercase).
	PlaceholderStyle(dialect.PlaceholderQuestion).
	Build()

// ---------- Clause Handlers ----------
// These are placeholder implementations that delegate to the parser.
// The parser will call these with the ParserOps interface to access
// the actual parsing methods.

// parseWhere handles the WHERE clause.
// The WHERE keyword has already been consumed.
func parseWhere(p spi.ParserOps) (spi.Node, error) {
	return p.ParseExpression()
}

// parseGroupBy handles the GROUP BY clause.
// The GROUP keyword has already been consumed.
func parseGroupBy(p spi.ParserOps) (spi.Node, error) {
	// Expect BY keyword
	if err := p.Expect(token.BY); err != nil {
		return nil, err
	}
	return p.ParseExpressionList()
}

// parseHaving handles the HAVING clause.
// The HAVING keyword has already been consumed.
func parseHaving(p spi.ParserOps) (spi.Node, error) {
	return p.ParseExpression()
}

// parseWindow handles named window definitions.
// The WINDOW keyword has already been consumed.
func parseWindow(_ spi.ParserOps) (spi.Node, error) {
	// Window definitions are complex - for now return nil
	// The parser will handle this specially
	return nil, nil
}

// parseOrderBy handles the ORDER BY clause.
// The ORDER keyword has already been consumed.
func parseOrderBy(p spi.ParserOps) (spi.Node, error) {
	// Expect BY keyword
	if err := p.Expect(token.BY); err != nil {
		return nil, err
	}
	return p.ParseOrderByList()
}

// parseLimit handles the LIMIT clause.
// The LIMIT keyword has already been consumed.
func parseLimit(p spi.ParserOps) (spi.Node, error) {
	return p.ParseExpression()
}

// parseOffset handles the OFFSET clause.
// The OFFSET keyword has already been consumed.
func parseOffset(p spi.ParserOps) (spi.Node, error) {
	return p.ParseExpression()
}
