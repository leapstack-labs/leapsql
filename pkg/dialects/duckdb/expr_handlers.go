package duckdb

import (
	"fmt"

	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/spi"
	"github.com/leapstack-labs/leapsql/pkg/token"
)

// parseListLiteral handles [expr, expr, ...].
// The opening [ has already been consumed.
func parseListLiteral(p spi.ParserOps) (spi.Expr, error) {
	list := &core.ListLiteral{}

	if !p.Check(token.RBRACKET) {
		for {
			elem, err := p.ParseExpression()
			if err != nil {
				return nil, fmt.Errorf("list literal: %w", err)
			}
			list.Elements = append(list.Elements, elem)

			if !p.Match(token.COMMA) {
				break
			}
		}
	}

	if err := p.Expect(token.RBRACKET); err != nil {
		return nil, fmt.Errorf("list literal: expected ]: %w", err)
	}

	return list, nil
}

// parseStructLiteral handles {'key': value, ...}.
// The opening { has already been consumed.
func parseStructLiteral(p spi.ParserOps) (spi.Expr, error) {
	s := &core.StructLiteral{}

	if !p.Check(token.RBRACE) {
		for {
			// Key can be identifier or string
			var key string
			tok := p.Token()
			switch tok.Type {
			case token.IDENT:
				key = tok.Literal
				p.NextToken()
			case token.STRING:
				key = tok.Literal
				p.NextToken()
			default:
				return nil, fmt.Errorf("struct literal: expected identifier or string as key, got %s", tok.Type)
			}

			if err := p.Expect(token.COLON); err != nil {
				return nil, fmt.Errorf("struct literal: expected colon after key: %w", err)
			}

			value, err := p.ParseExpression()
			if err != nil {
				return nil, fmt.Errorf("struct literal: %w", err)
			}

			s.Fields = append(s.Fields, core.StructField{
				Key:   key,
				Value: value,
			})

			if !p.Match(token.COMMA) {
				break
			}
		}
	}

	if err := p.Expect(token.RBRACE); err != nil {
		return nil, fmt.Errorf("struct literal: expected }: %w", err)
	}

	return s, nil
}

// parseIndexOrSlice handles expr[index] or expr[start:end].
// This is an infix handler - left is the expression being indexed.
// The opening [ has already been consumed.
func parseIndexOrSlice(p spi.ParserOps, left spi.Expr) (spi.Expr, error) {
	idx := &core.IndexExpr{
		Expr: left,
	}

	// Check for empty slice [:end]
	if p.Check(token.COLON) {
		idx.IsSlice = true
		p.NextToken() // consume :

		if !p.Check(token.RBRACKET) {
			end, err := p.ParseExpression()
			if err != nil {
				return nil, fmt.Errorf("array slice: %w", err)
			}
			idx.Stop = end
		}
	} else if !p.Check(token.RBRACKET) {
		// Parse start/index
		start, err := p.ParseExpression()
		if err != nil {
			return nil, fmt.Errorf("array index: %w", err)
		}

		if p.Match(token.COLON) {
			// It's a slice
			idx.IsSlice = true
			idx.Start = start

			if !p.Check(token.RBRACKET) {
				end, err := p.ParseExpression()
				if err != nil {
					return nil, fmt.Errorf("array slice: %w", err)
				}
				idx.Stop = end
			}
		} else {
			// Simple index
			idx.Index = start
		}
	}

	if err := p.Expect(token.RBRACKET); err != nil {
		return nil, fmt.Errorf("array index: expected ]: %w", err)
	}

	return idx, nil
}

// parseLambdaBody handles -> expr after lambda params.
// The -> has already been consumed.
// left is the parameter(s) - either ColumnRef or ParenExpr containing params.
func parseLambdaBody(p spi.ParserOps, left spi.Expr) (spi.Expr, error) {
	lambda := &core.LambdaExpr{}

	// Extract parameter names from left
	params, err := extractLambdaParams(left)
	if err != nil {
		return nil, err
	}
	lambda.Params = params

	// Parse body - use low precedence to capture the full expression
	body, err := p.ParseExpression()
	if err != nil {
		return nil, fmt.Errorf("lambda body: %w", err)
	}
	lambda.Body = body

	return lambda, nil
}

// extractLambdaParams extracts parameter names from a lambda parameter expression.
// Uses type assertions on the underlying AST types via interface checks.
func extractLambdaParams(expr spi.Expr) ([]string, error) {
	// Use interface checks to extract parameter info without importing parser
	type columnRef interface {
		GetTable() string
		GetColumn() string
	}
	type parenExpr interface {
		GetExpr() spi.Expr
	}
	type binaryExpr interface {
		GetLeft() spi.Expr
		GetRight() spi.Expr
		GetOp() token.TokenType
	}

	switch e := expr.(type) {
	case columnRef:
		// Single parameter: x -> expr
		if e.GetTable() != "" {
			return nil, fmt.Errorf("invalid lambda parameter: qualified name not allowed")
		}
		return []string{e.GetColumn()}, nil

	case parenExpr:
		// Parenthesized - could be (x) or need to handle (x, y) differently
		return extractLambdaParams(e.GetExpr())

	case binaryExpr:
		// For (x, y), the comma might create a binary expression
		if e.GetOp() == token.COMMA {
			leftParams, err := extractLambdaParams(e.GetLeft())
			if err != nil {
				return nil, err
			}
			rightParams, err := extractLambdaParams(e.GetRight())
			if err != nil {
				return nil, err
			}
			return append(leftParams, rightParams...), nil
		}
		return nil, fmt.Errorf("invalid lambda parameter: unexpected binary expression")

	default:
		return nil, fmt.Errorf("invalid lambda parameter: expected identifier, got %T", expr)
	}
}
