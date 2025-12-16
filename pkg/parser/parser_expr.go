package parser

// Expression precedence parsing: OR, AND, NOT, comparisons, arithmetic operators.
//
// Precedence (lowest to highest):
//
//  1. OR
//  2. AND
//  3. NOT
//  4. Comparisons: =, !=, <, >, <=, >=, IS [NOT] NULL, IN, BETWEEN, LIKE, ILIKE
//  5. Addition: +, -, ||
//  6. Multiplication: *, /, %
//  7. Unary: -, +
//  8. Primary: literals, column refs, function calls, parenthesized expressions
//
// Grammar:
//
//	expression    → or_expr
//	or_expr       → and_expr (OR and_expr)*
//	and_expr      → not_expr (AND not_expr)*
//	not_expr      → NOT not_expr | comparison
//	comparison    → addition ([NOT] (IN | BETWEEN | LIKE | ILIKE) ... | IS [NOT] NULL | cmp_op addition)?
//	addition      → multiplication (("+"|"-"|"||") multiplication)*
//	multiplication→ unary (("*"|"/"|"%") unary)*
//	unary         → ("-"|"+") unary | primary

// parseExpression parses an expression.
func (p *Parser) parseExpression() Expr {
	return p.parseOrExpr()
}

// parseOrExpr parses OR expressions.
func (p *Parser) parseOrExpr() Expr {
	left := p.parseAndExpr()

	for p.match(TOKEN_OR) {
		right := p.parseAndExpr()
		left = &BinaryExpr{Left: left, Op: "OR", Right: right}
	}

	return left
}

// parseAndExpr parses AND expressions.
func (p *Parser) parseAndExpr() Expr {
	left := p.parseNotExpr()

	for p.match(TOKEN_AND) {
		right := p.parseNotExpr()
		left = &BinaryExpr{Left: left, Op: "AND", Right: right}
	}

	return left
}

// parseNotExpr parses NOT expressions.
func (p *Parser) parseNotExpr() Expr {
	if p.match(TOKEN_NOT) {
		expr := p.parseNotExpr()
		return &UnaryExpr{Op: "NOT", Expr: expr}
	}
	return p.parseComparison()
}

// parseComparison parses comparison expressions.
func (p *Parser) parseComparison() Expr {
	left := p.parseAddition()

	// Check for special comparison operators
	var not bool
	if p.match(TOKEN_NOT) {
		not = true
	}

	switch {
	case p.match(TOKEN_IN):
		return p.parseInExpr(left, not)

	case p.match(TOKEN_BETWEEN):
		return p.parseBetweenExpr(left, not)

	case p.match(TOKEN_LIKE):
		return p.parseLikeExpr(left, not, false)

	case p.match(TOKEN_ILIKE):
		return p.parseLikeExpr(left, not, true)
	}

	// If we consumed NOT but didn't find IN/BETWEEN/LIKE, it was for IS NOT NULL
	if not {
		// Put NOT back conceptually - actually this path shouldn't happen
		// because NOT IS would be parsed differently
		return &UnaryExpr{Op: "NOT", Expr: left}
	}

	// IS NULL / IS NOT NULL
	if p.match(TOKEN_IS) {
		isNot := p.match(TOKEN_NOT)
		if p.match(TOKEN_NULL) {
			return &IsNullExpr{Expr: left, Not: isNot}
		}
		// IS TRUE / IS FALSE could be handled here
		p.addError("expected NULL after IS")
	}

	// Standard comparison operators
	switch p.token.Type {
	case TOKEN_EQ:
		p.nextToken()
		return &BinaryExpr{Left: left, Op: "=", Right: p.parseAddition()}
	case TOKEN_NE:
		p.nextToken()
		return &BinaryExpr{Left: left, Op: "!=", Right: p.parseAddition()}
	case TOKEN_LT:
		p.nextToken()
		return &BinaryExpr{Left: left, Op: "<", Right: p.parseAddition()}
	case TOKEN_GT:
		p.nextToken()
		return &BinaryExpr{Left: left, Op: ">", Right: p.parseAddition()}
	case TOKEN_LE:
		p.nextToken()
		return &BinaryExpr{Left: left, Op: "<=", Right: p.parseAddition()}
	case TOKEN_GE:
		p.nextToken()
		return &BinaryExpr{Left: left, Op: ">=", Right: p.parseAddition()}
	}

	return left
}

// parseInExpr parses an IN expression.
func (p *Parser) parseInExpr(left Expr, not bool) Expr {
	p.expect(TOKEN_LPAREN)
	in := &InExpr{Expr: left, Not: not}

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
func (p *Parser) parseBetweenExpr(left Expr, not bool) Expr {
	between := &BetweenExpr{Expr: left, Not: not}
	between.Low = p.parseAddition()
	p.expect(TOKEN_AND)
	between.High = p.parseAddition()
	return between
}

// parseLikeExpr parses a LIKE expression.
func (p *Parser) parseLikeExpr(left Expr, not bool, ilike bool) Expr {
	like := &LikeExpr{Expr: left, Not: not, ILike: ilike}
	like.Pattern = p.parseAddition()
	return like
}

// parseAddition parses addition/subtraction/concatenation expressions.
func (p *Parser) parseAddition() Expr {
	left := p.parseMultiplication()

	for {
		switch p.token.Type {
		case TOKEN_PLUS:
			p.nextToken()
			left = &BinaryExpr{Left: left, Op: "+", Right: p.parseMultiplication()}
		case TOKEN_MINUS:
			p.nextToken()
			left = &BinaryExpr{Left: left, Op: "-", Right: p.parseMultiplication()}
		case TOKEN_DPIPE:
			p.nextToken()
			left = &BinaryExpr{Left: left, Op: "||", Right: p.parseMultiplication()}
		default:
			return left
		}
	}
}

// parseMultiplication parses multiplication/division/modulo expressions.
func (p *Parser) parseMultiplication() Expr {
	left := p.parseUnary()

	for {
		switch p.token.Type {
		case TOKEN_STAR:
			p.nextToken()
			left = &BinaryExpr{Left: left, Op: "*", Right: p.parseUnary()}
		case TOKEN_SLASH:
			p.nextToken()
			left = &BinaryExpr{Left: left, Op: "/", Right: p.parseUnary()}
		case TOKEN_PERCENT:
			p.nextToken()
			left = &BinaryExpr{Left: left, Op: "%", Right: p.parseUnary()}
		default:
			return left
		}
	}
}

// parseUnary parses unary expressions.
func (p *Parser) parseUnary() Expr {
	switch p.token.Type {
	case TOKEN_MINUS:
		p.nextToken()
		return &UnaryExpr{Op: "-", Expr: p.parseUnary()}
	case TOKEN_PLUS:
		p.nextToken()
		return &UnaryExpr{Op: "+", Expr: p.parseUnary()}
	}
	return p.parsePrimary()
}
