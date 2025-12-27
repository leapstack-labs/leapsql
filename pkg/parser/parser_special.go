package parser

import (
	"github.com/leapstack-labs/leapsql/pkg/core"
)

// Special expression parsing: CASE, CAST, EXISTS, parenthesized expressions, subqueries.
//
// Grammar:
//
//	case_expr     → CASE [expr] (WHEN expr THEN expr)+ [ELSE expr] END
//	cast_expr     → CAST "(" expr AS type_name ")"
//	exists_expr   → [NOT] EXISTS "(" statement ")"
//	paren_expr    → "(" expression ")" | "(" statement ")"  -- subquery if SELECT/WITH
//	type_name     → identifier ["(" number ["," number] ")"]

// parseCaseExpr parses a CASE expression.
func (p *Parser) parseCaseExpr() core.Expr {
	p.expect(TOKEN_CASE)
	caseExpr := &core.CaseExpr{}

	// Simple CASE: CASE expr WHEN ...
	if !p.check(TOKEN_WHEN) {
		caseExpr.Operand = p.parseExpression()
	}

	// WHEN clauses
	for p.match(TOKEN_WHEN) {
		when := core.WhenClause{}
		when.Condition = p.parseExpression()
		p.expect(TOKEN_THEN)
		when.Result = p.parseExpression()
		caseExpr.Whens = append(caseExpr.Whens, when)
	}

	// ELSE clause
	if p.match(TOKEN_ELSE) {
		caseExpr.Else = p.parseExpression()
	}

	p.expect(TOKEN_END)
	return caseExpr
}

// parseCastExpr parses a CAST expression.
func (p *Parser) parseCastExpr() core.Expr {
	p.expect(TOKEN_CAST)
	p.expect(TOKEN_LPAREN)

	cast := &core.CastExpr{}
	cast.Expr = p.parseExpression()

	p.expect(TOKEN_AS)

	// Parse type name (can be qualified with parameters like VARCHAR(255))
	cast.TypeName = p.parseTypeName()

	p.expect(TOKEN_RPAREN)
	return cast
}

// parseTypeName parses a type name with optional parameters.
func (p *Parser) parseTypeName() string {
	if !p.check(TOKEN_IDENT) {
		p.addError("expected type name")
		return ""
	}

	typeName := p.token.Literal
	p.nextToken()

	// Type parameters like VARCHAR(255) or DECIMAL(10, 2)
	if p.match(TOKEN_LPAREN) {
		typeName += "("
		for {
			if p.check(TOKEN_NUMBER) {
				typeName += p.token.Literal
				p.nextToken()
			} else if p.check(TOKEN_IDENT) {
				typeName += p.token.Literal
				p.nextToken()
			}

			if !p.match(TOKEN_COMMA) {
				break
			}
			typeName += ", "
		}
		p.expect(TOKEN_RPAREN)
		typeName += ")"
	}

	return typeName
}

// parseParenExpr parses a parenthesized expression, subquery, or lambda parameter list.
func (p *Parser) parseParenExpr() core.Expr {
	p.expect(TOKEN_LPAREN)

	// Check if this is a subquery
	if p.check(TOKEN_SELECT) || p.check(TOKEN_WITH) {
		// Subquery expression (scalar subquery in SELECT, or in WHERE/HAVING for IN/EXISTS)
		subquery := &core.SubqueryExpr{Select: p.parseStatement()}
		p.expect(TOKEN_RPAREN)
		return subquery
	}

	// Parse the first expression
	expr := p.parseExpression()

	// Check for comma-separated list (for lambda parameters like (x, y) -> ...)
	if p.check(TOKEN_COMMA) {
		// This might be a lambda parameter list - parse all comma-separated identifiers
		// and wrap in a binary comma expression for the lambda handler to extract
		for p.match(TOKEN_COMMA) {
			right := p.parseExpression()
			expr = &core.BinaryExpr{Left: expr, Op: TOKEN_COMMA, Right: right}
		}
	}

	p.expect(TOKEN_RPAREN)
	return &core.ParenExpr{Expr: expr}
}

// parseExistsExpr parses an EXISTS expression.
func (p *Parser) parseExistsExpr(not bool) core.Expr {
	// Consume EXISTS keyword
	p.nextToken()

	p.expect(TOKEN_LPAREN)
	exists := &core.ExistsExpr{Not: not, Select: p.parseStatement()}
	p.expect(TOKEN_RPAREN)

	return exists
}
