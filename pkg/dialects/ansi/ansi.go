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
		token.FETCH, // FETCH FIRST/NEXT
	).
	// Standard clause handlers with slot assignments
	ClauseHandler(token.WHERE, parseWhere, spi.SlotWhere).
	ClauseHandler(token.GROUP, parseGroupBy, spi.SlotGroupBy, dialect.WithKeywords("GROUP", "BY")).
	ClauseHandler(token.HAVING, parseHaving, spi.SlotHaving).
	ClauseHandler(token.WINDOW, parseWindow, spi.SlotWindow).
	ClauseHandler(token.ORDER, parseOrderBy, spi.SlotOrderBy, dialect.WithKeywords("ORDER", "BY")).
	ClauseHandler(token.LIMIT, parseLimit, spi.SlotLimit, dialect.WithInline()).
	ClauseHandler(token.OFFSET, parseOffset, spi.SlotOffset, dialect.WithInline()).
	ClauseHandler(token.FETCH, parseFetch, spi.SlotFetch).
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
	AddInfix(token.MOD, spi.PrecedenceMultiply).
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

// FetchClause represents FETCH FIRST/NEXT n ROWS ONLY/WITH TIES (SQL:2008).
// This is defined here to avoid import cycles with the parser package.
type FetchClause struct {
	First    bool     // true = FIRST, false = NEXT (semantically identical)
	Count    spi.Expr // Number of rows (nil = 1 row implied)
	Percent  bool     // FETCH FIRST n PERCENT ROWS
	WithTies bool     // true = WITH TIES, false = ONLY
}

// GetFirst implements FetchClauseData.
func (f *FetchClause) GetFirst() bool { return f.First }

// GetCount implements FetchClauseData.
func (f *FetchClause) GetCount() spi.Expr { return f.Count }

// GetPercent implements FetchClauseData.
func (f *FetchClause) GetPercent() bool { return f.Percent }

// GetWithTies implements FetchClauseData.
func (f *FetchClause) GetWithTies() bool { return f.WithTies }

// parseFetch handles the FETCH clause.
// The FETCH keyword has already been consumed.
func parseFetch(p spi.ParserOps) (spi.Node, error) {
	fetch := &FetchClause{}

	// FIRST or NEXT (semantically identical)
	switch {
	case p.Match(token.FIRST):
		fetch.First = true
	case p.Match(token.NEXT):
		fetch.First = false
	default:
		p.AddError("expected FIRST or NEXT after FETCH")
		return fetch, nil
	}

	// Optional count expression (if not directly ROW/ROWS)
	if !p.Check(token.ROW) && !p.Check(token.ROWS) {
		expr, err := p.ParseExpression()
		if err != nil {
			p.AddError(err.Error())
			return fetch, nil
		}
		fetch.Count = expr

		// Optional PERCENT keyword
		if p.Match(token.PERCENT) {
			fetch.Percent = true
		}
	}

	// ROW or ROWS (both valid, singular or plural)
	if !p.Match(token.ROW) && !p.Match(token.ROWS) {
		p.AddError("expected ROW or ROWS in FETCH clause")
	}

	// ONLY or WITH TIES
	switch {
	case p.Match(token.ONLY):
		fetch.WithTies = false
	case p.Match(token.WITH):
		if !p.Match(token.TIES) {
			p.AddError("expected TIES after WITH")
		}
		fetch.WithTies = true
	default:
		p.AddError("expected ONLY or WITH TIES")
	}

	return fetch, nil
}
