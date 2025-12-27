package parser

import (
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/spi"
	"github.com/leapstack-labs/leapsql/pkg/token"
)

// Expression precedence parsing using Pratt parser with dialect-aware precedence.
//
// Precedence levels (from spi package):
//
//	PrecedenceNone       = 0
//	PrecedenceOr         = 1
//	PrecedenceAnd        = 2
//	PrecedenceNot        = 3
//	PrecedenceComparison = 4  (=, !=, <, >, <=, >=, IS, IN, BETWEEN, LIKE, ILIKE)
//	PrecedenceAddition   = 5  (+, -, ||)
//	PrecedenceMultiply   = 6  (*, /, %)
//	PrecedenceUnary      = 7  (-, +, NOT)
//	PrecedencePostfix    = 8  (::, [], ())
//
// The parser uses dialect.Precedence() to look up operator precedence dynamically,
// allowing dialects to add custom operators (like ILIKE for Postgres/DuckDB or :: for DuckDB).
// When no dialect is set, it falls back to default ANSI precedence.

// parseExpression parses an expression using precedence climbing.
func (p *Parser) parseExpression() core.Expr {
	return p.parseExpressionWithPrecedence(spi.PrecedenceNone + 1)
}

// parseExpressionWithPrecedence implements Pratt parsing with dialect-aware precedence.
func (p *Parser) parseExpressionWithPrecedence(minPrecedence int) core.Expr {
	// Parse prefix (unary operators and primary expressions)
	left := p.parsePrefixExpr()
	if left == nil {
		return nil
	}

	// Parse infix operators while their precedence is >= minPrecedence
	for {
		prec := p.getInfixPrecedence()
		if prec < minPrecedence {
			break
		}

		left = p.parseInfixExpr(left, prec)
		if left == nil {
			break
		}
	}

	return left
}

// parsePrefixExpr parses prefix expressions (unary operators and primary expressions).
func (p *Parser) parsePrefixExpr() core.Expr {
	switch p.token.Type {
	case TOKEN_NOT:
		p.nextToken()
		expr := p.parseExpressionWithPrecedence(spi.PrecedenceNot)
		return &core.UnaryExpr{Op: token.NOT, Expr: expr}

	case TOKEN_MINUS:
		p.nextToken()
		expr := p.parseExpressionWithPrecedence(spi.PrecedenceUnary)
		return &core.UnaryExpr{Op: token.MINUS, Expr: expr}

	case TOKEN_PLUS:
		p.nextToken()
		expr := p.parseExpressionWithPrecedence(spi.PrecedenceUnary)
		return &core.UnaryExpr{Op: token.PLUS, Expr: expr}

	default:
		return p.parsePrimary()
	}
}

// getInfixPrecedence returns the precedence of the current token as an infix operator.
// Returns 0 if the token is not an infix operator.
func (p *Parser) getInfixPrecedence() int {
	// Check dialect precedence first (includes ANSI operators if dialect has them)
	if p.dialect != nil {
		if prec := p.dialect.Precedence(p.token.Type); prec > 0 {
			return prec
		}
	}

	// Fall back to default ANSI precedence (for permissive parsing without dialect)
	return p.defaultPrecedence(p.token.Type)
}

// defaultPrecedence returns the default ANSI precedence for an operator.
// Used when no dialect is set (permissive parsing mode).
func (p *Parser) defaultPrecedence(t TokenType) int {
	switch t {
	case TOKEN_OR:
		return spi.PrecedenceOr
	case TOKEN_AND:
		return spi.PrecedenceAnd
	case TOKEN_EQ, TOKEN_NE, TOKEN_LT, TOKEN_GT, TOKEN_LE, TOKEN_GE:
		return spi.PrecedenceComparison
	case TOKEN_IS, TOKEN_IN, TOKEN_BETWEEN, TOKEN_LIKE:
		return spi.PrecedenceComparison
	case TOKEN_PLUS, TOKEN_MINUS, TOKEN_DPIPE:
		return spi.PrecedenceAddition
	case TOKEN_STAR, TOKEN_SLASH, TOKEN_MOD:
		return spi.PrecedenceMultiply
	case TOKEN_NOT:
		// NOT as infix (for NOT IN, NOT LIKE, etc.) - handled specially
		return spi.PrecedenceComparison
	default:
		// Check dynamically registered ILIKE
		if ilike := tokenIlike(); ilike != TOKEN_ILLEGAL && t == ilike {
			return spi.PrecedenceComparison
		}
		return spi.PrecedenceNone
	}
}

// parseInfixExpr parses an infix expression given the left operand and current precedence.
func (p *Parser) parseInfixExpr(left core.Expr, prec int) core.Expr {
	// Handle special infix operators first
	switch p.token.Type {
	case TOKEN_NOT:
		// NOT IN, NOT BETWEEN, NOT LIKE, NOT ILIKE
		return p.parseNotInfixExpr(left)

	case TOKEN_IS:
		return p.parseIsExpr(left)

	case TOKEN_IN:
		p.nextToken()
		return p.parseInExpr(left, false)

	case TOKEN_BETWEEN:
		p.nextToken()
		return p.parseBetweenExpr(left, false)

	case TOKEN_LIKE:
		op := p.token.Type
		p.nextToken()
		return p.parseLikeExpr(left, false, op)
	}

	// Check for ILIKE (dialect-specific token - compare by name since dialects may register their own)
	if p.token.Type.String() == "ILIKE" {
		op := p.token.Type
		p.nextToken()
		return p.parseLikeExpr(left, false, op)
	}

	// Check for custom infix handler (dialect-specific operators like ::)
	if p.dialect != nil {
		if handler := p.dialect.InfixHandler(p.token.Type); handler != nil {
			op := p.token
			p.nextToken()
			result, err := handler(p, left)
			if err != nil {
				p.addError(err.Error())
				return left
			}
			if result != nil {
				// result is already Expr type (spi.Expr = core.Expr = Expr)
				return result
			}
			// If handler returned nil, fall through to standard handling
			// This can happen for operators that need standard binary handling
			return &core.BinaryExpr{Left: left, Op: op.Type, Right: p.parseExpressionWithPrecedence(prec + 1)}
		}
	}

	// Standard binary operators
	op := p.token
	p.nextToken()

	// Parse right operand with higher precedence (left-associative)
	right := p.parseExpressionWithPrecedence(prec + 1)

	return &core.BinaryExpr{Left: left, Op: op.Type, Right: right}
}

// parseNotInfixExpr handles NOT as an infix modifier (NOT IN, NOT BETWEEN, NOT LIKE).
func (p *Parser) parseNotInfixExpr(left core.Expr) core.Expr {
	p.nextToken() // consume NOT

	switch p.token.Type {
	case TOKEN_IN:
		p.nextToken()
		return p.parseInExpr(left, true)

	case TOKEN_BETWEEN:
		p.nextToken()
		return p.parseBetweenExpr(left, true)

	case TOKEN_LIKE:
		op := p.token.Type
		p.nextToken()
		return p.parseLikeExpr(left, true, op)

	default:
		// Check for ILIKE (dialect-specific token - compare by name)
		if p.token.Type.String() == "ILIKE" {
			op := p.token.Type
			p.nextToken()
			return p.parseLikeExpr(left, true, op)
		}

		// NOT without a recognized following keyword - treat as error
		p.addError("expected IN, BETWEEN, LIKE, or ILIKE after NOT")
		return left
	}
}

// parseIsExpr parses IS [NOT] NULL / IS [NOT] TRUE / IS [NOT] FALSE.
func (p *Parser) parseIsExpr(left core.Expr) core.Expr {
	p.nextToken() // consume IS

	isNot := p.match(TOKEN_NOT)

	switch p.token.Type {
	case TOKEN_NULL:
		p.nextToken()
		return &core.IsNullExpr{Expr: left, Not: isNot}

	case TOKEN_TRUE:
		p.nextToken()
		return &core.IsBoolExpr{Expr: left, Not: isNot, Value: true}

	case TOKEN_FALSE:
		p.nextToken()
		return &core.IsBoolExpr{Expr: left, Not: isNot, Value: false}

	default:
		p.addError("expected NULL, TRUE, or FALSE after IS")
		return left
	}
}

// parseInExpr parses an IN expression.
func (p *Parser) parseInExpr(left core.Expr, not bool) core.Expr {
	p.expect(TOKEN_LPAREN)
	in := &core.InExpr{Expr: left, Not: not}

	// Check if it's a subquery
	if p.check(TOKEN_SELECT) || p.check(TOKEN_WITH) {
		in.Query = p.parseStatement()
	} else {
		// List of values
		in.Values = p.parseExpressionList()
	}

	p.expect(TOKEN_RPAREN)
	return in
}

// parseBetweenExpr parses a BETWEEN expression.
func (p *Parser) parseBetweenExpr(left core.Expr, not bool) core.Expr {
	between := &core.BetweenExpr{Expr: left, Not: not}
	// Parse low bound at addition precedence to avoid capturing AND
	between.Low = p.parseExpressionWithPrecedence(spi.PrecedenceAddition)
	p.expect(TOKEN_AND)
	// Parse high bound at addition precedence
	between.High = p.parseExpressionWithPrecedence(spi.PrecedenceAddition)
	return between
}

// parseLikeExpr parses a LIKE/ILIKE expression.
func (p *Parser) parseLikeExpr(left core.Expr, not bool, op token.TokenType) core.Expr {
	like := &core.LikeExpr{Expr: left, Not: not, Op: op}
	// Parse pattern at addition precedence
	like.Pattern = p.parseExpressionWithPrecedence(spi.PrecedenceAddition)
	return like
}
