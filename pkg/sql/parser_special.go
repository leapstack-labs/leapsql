package sql

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
func (p *Parser) parseCaseExpr() Expr {
	p.expect(TOKEN_CASE)
	caseExpr := &CaseExpr{}

	// Simple CASE: CASE expr WHEN ...
	if !p.check(TOKEN_WHEN) {
		caseExpr.Operand = p.parseExpression()
	}

	// WHEN clauses
	for p.match(TOKEN_WHEN) {
		when := WhenClause{}
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
func (p *Parser) parseCastExpr() Expr {
	p.expect(TOKEN_CAST)
	p.expect(TOKEN_LPAREN)

	cast := &CastExpr{}
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

// parseParenExpr parses a parenthesized expression or subquery.
func (p *Parser) parseParenExpr() Expr {
	p.expect(TOKEN_LPAREN)

	// Check if this is a subquery
	if p.check(TOKEN_SELECT) || p.check(TOKEN_WITH) {
		// Disallow scalar subqueries in SELECT list
		if p.inSelectList {
			p.addError(ErrScalarSubquery)
			// Skip to matching paren
			depth := 1
			for depth > 0 && p.token.Type != TOKEN_EOF {
				switch p.token.Type {
				case TOKEN_LPAREN:
					depth++
				case TOKEN_RPAREN:
					depth--
				}
				p.nextToken()
			}
			return nil
		}

		// Subquery expression (allowed in WHERE/HAVING context for IN/EXISTS)
		subquery := &SubqueryExpr{Select: p.parseStatement()}
		p.expect(TOKEN_RPAREN)
		return subquery
	}

	// Regular parenthesized expression
	expr := p.parseExpression()
	p.expect(TOKEN_RPAREN)
	return &ParenExpr{Expr: expr}
}

// parseExistsExpr parses an EXISTS expression.
func (p *Parser) parseExistsExpr(not bool) Expr {
	// Consume EXISTS keyword
	p.nextToken()

	p.expect(TOKEN_LPAREN)
	exists := &ExistsExpr{Not: not, Select: p.parseStatement()}
	p.expect(TOKEN_RPAREN)

	return exists
}
