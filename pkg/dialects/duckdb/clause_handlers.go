package duckdb

import (
	"github.com/leapstack-labs/leapsql/pkg/spi"
	"github.com/leapstack-labs/leapsql/pkg/token"
)

// GroupByAllMarker indicates GROUP BY ALL was parsed.
// This is stored in the slot and converted to GroupByAll=true in the parser.
type GroupByAllMarker struct{}

// IsGroupByAll implements spi.GroupByAllMarker.
func (*GroupByAllMarker) IsGroupByAll() bool { return true }

// OrderByAllMarker indicates ORDER BY ALL was parsed.
// Desc indicates whether DESC was specified (ORDER BY ALL DESC).
type OrderByAllMarker struct {
	Desc bool
}

// IsOrderByAll implements spi.OrderByAllMarker.
func (*OrderByAllMarker) IsOrderByAll() bool { return true }

// IsDesc implements spi.OrderByAllMarker.
func (m *OrderByAllMarker) IsDesc() bool { return m.Desc }

// parseGroupByWithAll handles the GROUP BY clause with DuckDB's ALL support.
// The GROUP keyword has already been consumed.
func parseGroupByWithAll(p spi.ParserOps) (spi.Node, error) {
	// Expect BY keyword
	if err := p.Expect(token.BY); err != nil {
		return nil, err
	}

	// Check for GROUP BY ALL (DuckDB extension)
	if p.Match(token.ALL) {
		return &GroupByAllMarker{}, nil
	}

	// Standard GROUP BY expression list
	return p.ParseExpressionList()
}

// parseOrderByWithAll handles the ORDER BY clause with DuckDB's ALL support.
// The ORDER keyword has already been consumed.
func parseOrderByWithAll(p spi.ParserOps) (spi.Node, error) {
	// Expect BY keyword
	if err := p.Expect(token.BY); err != nil {
		return nil, err
	}

	// Check for ORDER BY ALL (DuckDB extension)
	if p.Match(token.ALL) {
		marker := &OrderByAllMarker{}

		// Optional direction
		if p.Match(token.DESC) {
			marker.Desc = true
		} else {
			p.Match(token.ASC) // consume optional ASC
		}

		return marker, nil
	}

	// Standard ORDER BY list
	return p.ParseOrderByList()
}
