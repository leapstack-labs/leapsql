package duckdb

import (
	"fmt"

	"github.com/leapstack-labs/leapsql/pkg/parser"
	"github.com/leapstack-labs/leapsql/pkg/spi"
	"github.com/leapstack-labs/leapsql/pkg/token"
)

// parseListLiteral handles [expr, expr, ...].
// The opening [ has already been consumed.
func parseListLiteral(p spi.ParserOps) (spi.Expr, error) {
	list := &parser.ListLiteral{}

	if !p.Check(token.RBRACKET) {
		for {
			elem, err := p.ParseExpression()
			if err != nil {
				return nil, fmt.Errorf("list literal: %w", err)
			}
			list.Elements = append(list.Elements, elem.(parser.Expr))

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
	s := &parser.StructLiteral{}

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

			s.Fields = append(s.Fields, parser.StructField{
				Key:   key,
				Value: value.(parser.Expr),
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
	idx := &parser.IndexExpr{
		Expr: left.(parser.Expr),
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
			idx.End = end.(parser.Expr)
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
			idx.Start = start.(parser.Expr)

			if !p.Check(token.RBRACKET) {
				end, err := p.ParseExpression()
				if err != nil {
					return nil, fmt.Errorf("array slice: %w", err)
				}
				idx.End = end.(parser.Expr)
			}
		} else {
			// Simple index
			idx.Index = start.(parser.Expr)
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
	lambda := &parser.LambdaExpr{}

	// Extract parameter names from left
	params, err := extractLambdaParams(left.(parser.Expr))
	if err != nil {
		return nil, err
	}
	lambda.Params = params

	// Parse body - use low precedence to capture the full expression
	body, err := p.ParseExpression()
	if err != nil {
		return nil, fmt.Errorf("lambda body: %w", err)
	}
	lambda.Body = body.(parser.Expr)

	return lambda, nil
}

// extractLambdaParams extracts parameter names from a lambda parameter expression.
func extractLambdaParams(expr parser.Expr) ([]string, error) {
	switch e := expr.(type) {
	case *parser.ColumnRef:
		// Single parameter: x -> expr
		if e.Table != "" {
			return nil, fmt.Errorf("invalid lambda parameter: qualified name not allowed")
		}
		return []string{e.Column}, nil

	case *parser.ParenExpr:
		// Parenthesized - could be (x) or need to handle (x, y) differently
		// In our parser, (x, y) would be parsed as x comma y which isn't a standard expr
		// Let's handle the single param case first
		return extractLambdaParams(e.Expr)

	case *parser.BinaryExpr:
		// For (x, y), the comma might create a binary expression depending on parsing
		if e.Op == token.COMMA {
			leftParams, err := extractLambdaParams(e.Left)
			if err != nil {
				return nil, err
			}
			rightParams, err := extractLambdaParams(e.Right)
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
