package duckdb

import (
	"fmt"

	"github.com/leapstack-labs/leapsql/pkg/parser"
	"github.com/leapstack-labs/leapsql/pkg/spi"
	"github.com/leapstack-labs/leapsql/pkg/token"
)

// parsePivot handles PIVOT (aggregates FOR column IN (values)).
// The PIVOT keyword has already been consumed.
func parsePivot(p spi.ParserOps, source spi.TableRef) (spi.TableRef, error) {
	pivot := &parser.PivotTable{
		Source: source.(parser.TableRef),
	}

	if err := p.Expect(token.LPAREN); err != nil {
		return nil, fmt.Errorf("PIVOT: %w", err)
	}

	// Parse aggregates (at least one required)
	for {
		agg, err := parsePivotAggregate(p)
		if err != nil {
			return nil, err
		}
		pivot.Aggregates = append(pivot.Aggregates, agg)

		// Check for comma (more aggregates) or FOR (end of aggregates)
		if p.Check(TokenFor) {
			break
		}
		if !p.Match(token.COMMA) {
			break
		}
	}

	// FOR column
	if err := p.Expect(TokenFor); err != nil {
		return nil, fmt.Errorf("PIVOT: expected FOR: %w", err)
	}

	colName, err := p.ParseIdentifier()
	if err != nil {
		return nil, fmt.Errorf("PIVOT: expected column name after FOR: %w", err)
	}
	pivot.ForColumn = colName

	// IN (values) or IN *
	if err := p.Expect(token.IN); err != nil {
		return nil, fmt.Errorf("PIVOT: expected IN: %w", err)
	}

	if p.Match(token.STAR) {
		pivot.InStar = true
	} else {
		if err := p.Expect(token.LPAREN); err != nil {
			return nil, fmt.Errorf("PIVOT IN: expected ( or *: %w", err)
		}

		// Parse IN values
		for {
			val, err := parsePivotInValue(p)
			if err != nil {
				return nil, err
			}
			pivot.InValues = append(pivot.InValues, val)

			if !p.Match(token.COMMA) {
				break
			}
		}

		if err := p.Expect(token.RPAREN); err != nil {
			return nil, fmt.Errorf("PIVOT IN: expected ): %w", err)
		}
	}

	if err := p.Expect(token.RPAREN); err != nil {
		return nil, fmt.Errorf("PIVOT: expected closing ): %w", err)
	}

	// Optional alias
	if p.Match(token.AS) {
		name, err := p.ParseIdentifier()
		if err != nil {
			return nil, fmt.Errorf("PIVOT alias: %w", err)
		}
		pivot.Alias = name
	} else if p.Check(token.IDENT) {
		// Alias without AS
		tok := p.Token()
		pivot.Alias = tok.Literal
		p.NextToken()
	}

	return pivot, nil
}

// parsePivotAggregate parses an aggregate function in PIVOT.
func parsePivotAggregate(p spi.ParserOps) (parser.PivotAggregate, error) {
	agg := parser.PivotAggregate{}

	// Parse the aggregate function call
	expr, err := p.ParseExpression()
	if err != nil {
		return agg, fmt.Errorf("PIVOT aggregate: %w", err)
	}

	fn, ok := expr.(*parser.FuncCall)
	if !ok {
		return agg, fmt.Errorf("PIVOT: expected aggregate function, got %T", expr)
	}
	agg.Func = fn

	// Optional alias
	if p.Match(token.AS) {
		name, err := p.ParseIdentifier()
		if err != nil {
			return agg, fmt.Errorf("PIVOT aggregate alias: %w", err)
		}
		agg.Alias = name
	}

	return agg, nil
}

// parsePivotInValue parses a value in PIVOT ... IN (...).
func parsePivotInValue(p spi.ParserOps) (parser.PivotInValue, error) {
	val := parser.PivotInValue{}

	// Parse value (could be literal, identifier, or expression)
	expr, err := p.ParseExpression()
	if err != nil {
		return val, fmt.Errorf("PIVOT IN value: %w", err)
	}
	val.Value = expr.(parser.Expr)

	// Optional alias
	if p.Match(token.AS) {
		name, err := p.ParseIdentifier()
		if err != nil {
			return val, fmt.Errorf("PIVOT IN value alias: %w", err)
		}
		val.Alias = name
	}

	return val, nil
}

// parseUnpivot handles UNPIVOT (value FOR name IN (columns)).
// The UNPIVOT keyword has already been consumed.
func parseUnpivot(p spi.ParserOps, source spi.TableRef) (spi.TableRef, error) {
	unpivot := &parser.UnpivotTable{
		Source: source.(parser.TableRef),
	}

	if err := p.Expect(token.LPAREN); err != nil {
		return nil, fmt.Errorf("UNPIVOT: %w", err)
	}

	// Parse value column(s)
	// Can be single: value
	// Or multiple: (value1, value2)
	if p.Match(token.LPAREN) {
		// Multiple value columns
		for {
			name, err := p.ParseIdentifier()
			if err != nil {
				return nil, fmt.Errorf("UNPIVOT value columns: %w", err)
			}
			unpivot.ValueColumns = append(unpivot.ValueColumns, name)

			if !p.Match(token.COMMA) {
				break
			}
		}
		if err := p.Expect(token.RPAREN); err != nil {
			return nil, fmt.Errorf("UNPIVOT value columns: expected ): %w", err)
		}
	} else {
		// Single value column
		name, err := p.ParseIdentifier()
		if err != nil {
			return nil, fmt.Errorf("UNPIVOT: expected value column name: %w", err)
		}
		unpivot.ValueColumns = []string{name}
	}

	// FOR name_column
	if err := p.Expect(TokenFor); err != nil {
		return nil, fmt.Errorf("UNPIVOT: expected FOR: %w", err)
	}

	nameCol, err := p.ParseIdentifier()
	if err != nil {
		return nil, fmt.Errorf("UNPIVOT: expected name column after FOR: %w", err)
	}
	unpivot.NameColumn = nameCol

	// IN (columns)
	if err := p.Expect(token.IN); err != nil {
		return nil, fmt.Errorf("UNPIVOT: expected IN: %w", err)
	}
	if err := p.Expect(token.LPAREN); err != nil {
		return nil, fmt.Errorf("UNPIVOT IN: expected (: %w", err)
	}

	// Parse IN column groups
	for {
		group, err := parseUnpivotInGroup(p, len(unpivot.ValueColumns))
		if err != nil {
			return nil, err
		}
		unpivot.InColumns = append(unpivot.InColumns, group)

		if !p.Match(token.COMMA) {
			break
		}
	}

	if err := p.Expect(token.RPAREN); err != nil {
		return nil, fmt.Errorf("UNPIVOT IN: expected ): %w", err)
	}

	if err := p.Expect(token.RPAREN); err != nil {
		return nil, fmt.Errorf("UNPIVOT: expected closing ): %w", err)
	}

	// Optional alias
	if p.Match(token.AS) {
		name, err := p.ParseIdentifier()
		if err != nil {
			return nil, fmt.Errorf("UNPIVOT alias: %w", err)
		}
		unpivot.Alias = name
	} else if p.Check(token.IDENT) {
		tok := p.Token()
		unpivot.Alias = tok.Literal
		p.NextToken()
	}

	return unpivot, nil
}

// parseUnpivotInGroup parses a column group in UNPIVOT ... IN (...).
func parseUnpivotInGroup(p spi.ParserOps, expectedCols int) (parser.UnpivotInGroup, error) {
	group := parser.UnpivotInGroup{}

	if expectedCols > 1 && p.Check(token.LPAREN) {
		// Multiple columns: (col1, col2)
		p.Match(token.LPAREN)
		for {
			name, err := p.ParseIdentifier()
			if err != nil {
				return group, fmt.Errorf("UNPIVOT IN columns: %w", err)
			}
			group.Columns = append(group.Columns, name)

			if !p.Match(token.COMMA) {
				break
			}
		}
		if err := p.Expect(token.RPAREN); err != nil {
			return group, fmt.Errorf("UNPIVOT IN columns: expected ): %w", err)
		}
	} else {
		// Single column
		name, err := p.ParseIdentifier()
		if err != nil {
			return group, fmt.Errorf("UNPIVOT IN: expected column name: %w", err)
		}
		group.Columns = []string{name}
	}

	// Optional alias (AS 'label' or AS identifier)
	if p.Match(token.AS) {
		tok := p.Token()
		if tok.Type == token.STRING {
			group.Alias = tok.Literal
			p.NextToken()
		} else {
			name, err := p.ParseIdentifier()
			if err != nil {
				return group, fmt.Errorf("UNPIVOT IN alias: %w", err)
			}
			group.Alias = name
		}
	}

	return group, nil
}
