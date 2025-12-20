package parser

import (
	"fmt"

	"github.com/leapstack-labs/leapsql/pkg/dialect"
	"github.com/leapstack-labs/leapsql/pkg/spi"
)

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
//	                [clauses based on dialect sequence]
//	select_list   → select_item ("," select_item)*
//	select_item   → "*" | table "." "*" | expr [AS identifier]
//	order_list    → order_item ("," order_item)*
//	order_item    → expr [ASC|DESC] [NULLS FIRST|LAST]
//
// The parser uses dialect.ClauseSequence() and dialect.ClauseHandler() to
// parse clauses in the correct order for the current dialect, and to
// reject unsupported clauses (like QUALIFY in Postgres).

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
	core.Columns = p.parseSelectList()

	// FROM clause (required for our use case)
	if p.match(TOKEN_FROM) {
		core.From = p.parseFromClause()
	}

	// Parse optional clauses using dialect-driven approach
	p.parseClauses(core)

	return core
}

// parseClauses parses optional clauses using the dialect's clause sequence and handlers.
func (p *Parser) parseClauses(core *SelectCore) {
	p.parseClausesWithDialect(core)
}

// parseClausesWithDialect parses clauses using dialect.ClauseDef() for both
// parsing logic and slot-based assignment. This is fully declarative -
// no hardcoded clause knowledge in the parser.
func (p *Parser) parseClausesWithDialect(core *SelectCore) {
	sequence := p.dialect.ClauseSequence()
	if sequence == nil {
		return // No clauses to parse for this dialect
	}

	for {
		matched := false

		// Try to match against any clause in the sequence
		for _, clauseType := range sequence {
			if p.check(clauseType) {
				def, ok := p.dialect.ClauseDef(clauseType)
				if !ok {
					p.addError(fmt.Sprintf("no definition for clause %s in dialect %s", clauseType, p.dialect.Name))
					p.nextToken()
					matched = true
					break
				}

				p.nextToken() // consume clause keyword

				result, err := def.Handler(p)
				if err != nil {
					p.addError(err.Error())
				}

				// Use slot-based assignment (declarative)
				p.assignToSlot(core, def.Slot, result)
				matched = true
				break
			}
		}

		// Check for unsupported clause (known globally but not in this dialect)
		if !matched {
			if name, isKnown := dialect.IsKnownClause(p.token.Type); isKnown {
				// Check if it's in this dialect's sequence
				inSequence := false
				for _, tok := range sequence {
					if tok == p.token.Type {
						inSequence = true
						break
					}
				}
				if !inSequence {
					p.addError(fmt.Sprintf("%s is not supported in %s dialect", name, p.dialect.Name))
					p.nextToken()
					continue
				}
			}
		}

		if !matched {
			break
		}
	}

	// Handle OFFSET which typically follows LIMIT
	if p.match(TOKEN_OFFSET) {
		core.Offset = p.parseExpression()
	}
}

// assignToSlot stores the parsed clause result in the appropriate SelectCore field.
// This uses the declarative ClauseSlot enum to determine where to store data.
func (p *Parser) assignToSlot(core *SelectCore, slot spi.ClauseSlot, result any) {
	if result == nil {
		return
	}

	switch slot {
	case spi.SlotWhere:
		if expr, ok := result.(Expr); ok {
			core.Where = expr
		}

	case spi.SlotGroupBy:
		switch v := result.(type) {
		case []Expr:
			core.GroupBy = v
		case []spi.Expr:
			exprs := make([]Expr, len(v))
			for i, e := range v {
				if expr, ok := e.(Expr); ok {
					exprs[i] = expr
				}
			}
			core.GroupBy = exprs
		case []any:
			exprs := make([]Expr, len(v))
			for i, e := range v {
				if expr, ok := e.(Expr); ok {
					exprs[i] = expr
				}
			}
			core.GroupBy = exprs
		}

	case spi.SlotHaving:
		if expr, ok := result.(Expr); ok {
			core.Having = expr
		}

	case spi.SlotWindow:
		// Window definitions are complex - for now skip
		// TODO: Add window definitions support

	case spi.SlotOrderBy:
		switch v := result.(type) {
		case []OrderByItem:
			core.OrderBy = v
		case []spi.OrderByItem:
			items := make([]OrderByItem, len(v))
			for i, item := range v {
				if obi, ok := item.(OrderByItem); ok {
					items[i] = obi
				}
			}
			core.OrderBy = items
		case []any:
			items := make([]OrderByItem, len(v))
			for i, item := range v {
				if obi, ok := item.(OrderByItem); ok {
					items[i] = obi
				}
			}
			core.OrderBy = items
		}

	case spi.SlotLimit:
		if expr, ok := result.(Expr); ok {
			core.Limit = expr
		}

	case spi.SlotOffset:
		if expr, ok := result.(Expr); ok {
			core.Offset = expr
		}

	case spi.SlotQualify:
		if expr, ok := result.(Expr); ok {
			core.Qualify = expr
		}

	case spi.SlotExtensions:
		if node, ok := result.(Node); ok {
			core.Extensions = append(core.Extensions, node)
		}
	}
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
