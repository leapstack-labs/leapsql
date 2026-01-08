// Package dialect provides SQL dialect configuration and function classification.
//
// This file contains stateless clause handlers that form the "toolbox" of
// reusable parsing logic. These handlers are pure functions that accept
// spi.ParserOps and return spi.Node.
package dialect

import (
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/spi"
	"github.com/leapstack-labs/leapsql/pkg/token"
)

// ---------- Standard Clause Handlers ----------
// These are stateless functions that can be composed into any dialect.
// The leading keyword has already been consumed when these are called.

// ParseWhere handles the standard WHERE clause.
// The WHERE keyword has already been consumed.
func ParseWhere(p spi.ParserOps) (spi.Node, error) {
	return p.ParseExpression()
}

// ParseGroupBy handles the standard GROUP BY clause.
// The GROUP keyword has already been consumed.
func ParseGroupBy(p spi.ParserOps) (spi.Node, error) {
	if err := p.Expect(token.BY); err != nil {
		return nil, err
	}
	return p.ParseExpressionList()
}

// ParseHaving handles the standard HAVING clause.
// The HAVING keyword has already been consumed.
func ParseHaving(p spi.ParserOps) (spi.Node, error) {
	return p.ParseExpression()
}

// ParseWindow handles named window definitions.
// The WINDOW keyword has already been consumed.
func ParseWindow(_ spi.ParserOps) (spi.Node, error) {
	// Window definitions are complex - the parser handles this specially
	return nil, nil
}

// ParseOrderBy handles the standard ORDER BY clause.
// The ORDER keyword has already been consumed.
func ParseOrderBy(p spi.ParserOps) (spi.Node, error) {
	if err := p.Expect(token.BY); err != nil {
		return nil, err
	}
	return p.ParseOrderByList()
}

// ParseLimit handles the standard LIMIT clause.
// The LIMIT keyword has already been consumed.
func ParseLimit(p spi.ParserOps) (spi.Node, error) {
	return p.ParseExpression()
}

// ParseOffset handles the standard OFFSET clause.
// The OFFSET keyword has already been consumed.
func ParseOffset(p spi.ParserOps) (spi.Node, error) {
	return p.ParseExpression()
}

// FetchClause represents FETCH FIRST/NEXT n ROWS ONLY/WITH TIES (SQL:2008).
// This is defined here to avoid import cycles with the parser package.
type FetchClause struct {
	First    bool      // true = FIRST, false = NEXT (semantically identical)
	Count    core.Expr // Number of rows (nil = 1 row implied)
	Percent  bool      // FETCH FIRST n PERCENT ROWS
	WithTies bool      // true = WITH TIES, false = ONLY
}

// GetFirst implements FetchClauseData.
func (f *FetchClause) GetFirst() bool { return f.First }

// GetCount implements FetchClauseData.
func (f *FetchClause) GetCount() core.Expr { return f.Count }

// GetPercent implements FetchClauseData.
func (f *FetchClause) GetPercent() bool { return f.Percent }

// GetWithTies implements FetchClauseData.
func (f *FetchClause) GetWithTies() bool { return f.WithTies }

// ParseFetch handles the FETCH FIRST/NEXT clause (SQL:2008).
// The FETCH keyword has already been consumed.
func ParseFetch(p spi.ParserOps) (spi.Node, error) {
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

// ParseQualify handles the QUALIFY clause (DuckDB, Databricks, etc.).
// The QUALIFY keyword has already been consumed.
func ParseQualify(p spi.ParserOps) (spi.Node, error) {
	return p.ParseExpression()
}

// --- GROUP BY ALL / ORDER BY ALL support ---

// groupByAllMarker implements spi.GroupByAllMarker.
type groupByAllMarker struct{}

func (g *groupByAllMarker) IsGroupByAll() bool { return true }

// ParseGroupByWithAll handles GROUP BY with optional ALL keyword.
// The GROUP keyword has already been consumed.
func ParseGroupByWithAll(p spi.ParserOps) (spi.Node, error) {
	if err := p.Expect(token.BY); err != nil {
		return nil, err
	}
	if p.Match(token.ALL) {
		return &groupByAllMarker{}, nil
	}
	return p.ParseExpressionList()
}

// orderByAllMarker implements spi.OrderByAllMarker.
type orderByAllMarker struct {
	desc bool
}

func (o *orderByAllMarker) IsOrderByAll() bool { return true }
func (o *orderByAllMarker) IsDesc() bool       { return o.desc }

// ParseOrderByWithAll handles ORDER BY with optional ALL keyword.
// The ORDER keyword has already been consumed.
func ParseOrderByWithAll(p spi.ParserOps) (spi.Node, error) {
	if err := p.Expect(token.BY); err != nil {
		return nil, err
	}
	if p.Match(token.ALL) {
		desc := false
		if p.Match(token.DESC) {
			desc = true
		} else {
			p.Match(token.ASC) // consume optional ASC
		}
		return &orderByAllMarker{desc: desc}, nil
	}
	return p.ParseOrderByList()
}
