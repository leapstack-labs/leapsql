package sql

// Statement parsing: WITH clause, CTEs, SELECT body, SELECT list, ORDER BY.
//
// Grammar:
//
//	statement     → [WITH cte_list] select_body
//	cte_list      → cte ("," cte)*
//	cte           → identifier AS "(" statement ")"
//	select_body   → select_core [(UNION|INTERSECT|EXCEPT) [ALL|DISTINCT] select_body]
//	select_core   → SELECT [DISTINCT|ALL] select_list
//	                [FROM from_clause]
//	                [WHERE expr]
//	                [GROUP BY expr_list]
//	                [HAVING expr]
//	                [QUALIFY expr]
//	                [ORDER BY order_list]
//	                [LIMIT expr [OFFSET expr]]
//	select_list   → select_item ("," select_item)*
//	select_item   → "*" | table "." "*" | expr [AS identifier]
//	order_list    → order_item ("," order_item)*
//	order_item    → expr [ASC|DESC] [NULLS FIRST|LAST]

// parseStatement parses a complete SQL statement.
func (p *Parser) parseStatement() *SelectStmt {
	stmt := &SelectStmt{}

	// Optional WITH clause
	if p.check(TOKEN_WITH) {
		stmt.With = p.parseWithClause()
	}

	// Required SELECT body
	stmt.Body = p.parseSelectBody()

	return stmt
}

// parseWithClause parses a WITH clause with CTEs.
func (p *Parser) parseWithClause() *WithClause {
	p.expect(TOKEN_WITH)
	with := &WithClause{}

	// Optional RECURSIVE
	if p.match(TOKEN_RECURSIVE) {
		with.Recursive = true
	}

	// Parse CTE list
	for {
		cte := p.parseCTE()
		with.CTEs = append(with.CTEs, cte)

		if !p.match(TOKEN_COMMA) {
			break
		}
	}

	return with
}

// parseCTE parses a single CTE.
func (p *Parser) parseCTE() *CTE {
	cte := &CTE{}

	// CTE name
	if !p.check(TOKEN_IDENT) {
		p.addError("expected CTE name")
		return cte
	}
	cte.Name = p.token.Literal
	p.nextToken()

	// AS
	p.expect(TOKEN_AS)

	// ( SelectStatement )
	p.expect(TOKEN_LPAREN)
	cte.Select = p.parseStatement()
	p.expect(TOKEN_RPAREN)

	return cte
}

// parseSelectBody parses a SELECT body with possible set operations.
func (p *Parser) parseSelectBody() *SelectBody {
	body := &SelectBody{}
	body.Left = p.parseSelectCore()

	// Check for set operations
	if p.check(TOKEN_UNION) || p.check(TOKEN_INTERSECT) || p.check(TOKEN_EXCEPT) {
		switch p.token.Type {
		case TOKEN_UNION:
			p.nextToken()
			if p.match(TOKEN_ALL) {
				body.Op = SetOpUnionAll
				body.All = true
			} else {
				body.Op = SetOpUnion
				p.match(TOKEN_DISTINCT) // optional
			}
		case TOKEN_INTERSECT:
			p.nextToken()
			body.Op = SetOpIntersect
			p.match(TOKEN_ALL) // optional
		case TOKEN_EXCEPT:
			p.nextToken()
			body.Op = SetOpExcept
			p.match(TOKEN_ALL) // optional
		}

		// Parse the right side (recursively for chained operations)
		body.Right = p.parseSelectBody()
	}

	return body
}

// parseSelectCore parses a single SELECT clause.
func (p *Parser) parseSelectCore() *SelectCore {
	p.expect(TOKEN_SELECT)
	core := &SelectCore{}

	// DISTINCT / ALL
	if p.match(TOKEN_DISTINCT) {
		core.Distinct = true
	} else {
		p.match(TOKEN_ALL) // optional, consume if present
	}

	// SELECT list
	p.inSelectList = true
	core.Columns = p.parseSelectList()
	p.inSelectList = false

	// FROM clause (required for our use case)
	if p.match(TOKEN_FROM) {
		core.From = p.parseFromClause()
	}

	// WHERE clause
	if p.match(TOKEN_WHERE) {
		core.Where = p.parseExpression()
	}

	// GROUP BY clause
	if p.match(TOKEN_GROUP) {
		p.expect(TOKEN_BY)
		core.GroupBy = p.parseExpressionList()
	}

	// HAVING clause
	if p.match(TOKEN_HAVING) {
		core.Having = p.parseExpression()
	}

	// QUALIFY clause (DuckDB/Snowflake)
	if p.match(TOKEN_QUALIFY) {
		core.Qualify = p.parseExpression()
	}

	// ORDER BY clause
	if p.match(TOKEN_ORDER) {
		p.expect(TOKEN_BY)
		core.OrderBy = p.parseOrderByList()
	}

	// LIMIT clause
	if p.match(TOKEN_LIMIT) {
		core.Limit = p.parseExpression()

		// OFFSET clause
		if p.match(TOKEN_OFFSET) {
			core.Offset = p.parseExpression()
		}
	}

	return core
}

// parseSelectList parses the list of SELECT items.
func (p *Parser) parseSelectList() []SelectItem {
	var items []SelectItem

	for {
		item := p.parseSelectItem()
		items = append(items, item)

		if !p.match(TOKEN_COMMA) {
			break
		}
	}

	return items
}

// parseSelectItem parses a single SELECT item.
func (p *Parser) parseSelectItem() SelectItem {
	item := SelectItem{}

	// Check for * or table.*
	if p.check(TOKEN_STAR) {
		item.Star = true
		p.nextToken()
		return item
	}

	// Check for table.* pattern using 3-token lookahead (no rollback needed)
	if p.check(TOKEN_IDENT) && p.checkPeek(TOKEN_DOT) && p.checkPeek2(TOKEN_STAR) {
		tableName := p.token.Literal
		p.nextToken() // consume identifier
		p.nextToken() // consume DOT
		p.nextToken() // consume STAR
		item.TableStar = tableName
		return item
	}

	// Regular expression
	item.Expr = p.parseExpression()

	// Optional alias
	if p.match(TOKEN_AS) {
		if p.check(TOKEN_IDENT) {
			item.Alias = p.token.Literal
			p.nextToken()
		} else {
			p.addError("expected alias after AS")
		}
	} else if p.check(TOKEN_IDENT) && !p.isKeyword(p.token) {
		// Alias without AS
		item.Alias = p.token.Literal
		p.nextToken()
	}

	return item
}

// parseOrderByList parses a list of ORDER BY items.
func (p *Parser) parseOrderByList() []OrderByItem {
	var items []OrderByItem

	for {
		item := p.parseOrderByItem()
		items = append(items, item)

		if !p.match(TOKEN_COMMA) {
			break
		}
	}

	return items
}

// parseOrderByItem parses a single ORDER BY item.
func (p *Parser) parseOrderByItem() OrderByItem {
	item := OrderByItem{}
	item.Expr = p.parseExpression()

	// ASC / DESC
	if p.match(TOKEN_ASC) {
		item.Desc = false
	} else if p.match(TOKEN_DESC) {
		item.Desc = true
	}

	// NULLS FIRST / LAST
	if p.match(TOKEN_NULLS) {
		if p.match(TOKEN_FIRST) {
			b := true
			item.NullsFirst = &b
		} else if p.match(TOKEN_LAST) {
			b := false
			item.NullsFirst = &b
		}
	}

	return item
}

// parseExpressionList parses a comma-separated list of expressions.
func (p *Parser) parseExpressionList() []Expr {
	var exprs []Expr

	for {
		expr := p.parseExpression()
		exprs = append(exprs, expr)

		if !p.match(TOKEN_COMMA) {
			break
		}
	}

	return exprs
}
