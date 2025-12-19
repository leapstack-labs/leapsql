package parser

import (
	"fmt"

	"github.com/leapstack-labs/leapsql/pkg/spi"
	"github.com/leapstack-labs/leapsql/pkg/token"
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
	p.inSelectList = true
	core.Columns = p.parseSelectList()
	p.inSelectList = false

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

// parseClausesWithDialect parses clauses using dialect.ClauseSequence() and ClauseHandler().
// This is the strict mode - only clauses in the dialect's sequence are accepted.
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
				handler := p.dialect.ClauseHandler(clauseType)
				if handler == nil {
					p.addError(fmt.Sprintf("no handler for clause %s in dialect %s", clauseType, p.dialect.Name))
					p.nextToken()
					matched = true
					break
				}

				p.nextToken() // consume clause keyword

				result, err := handler(p)
				if err != nil {
					p.addError(err.Error())
				}

				p.assignClauseResult(core, clauseType, result)
				matched = true
				break
			}
		}

		// Check for unsupported clause keyword (not in this dialect's sequence)
		if !matched && p.isUnknownClauseKeyword(sequence) {
			p.addError(fmt.Sprintf("%s is not supported in %s dialect", p.token.Literal, p.dialect.Name))
			p.nextToken()
			continue
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

// isUnknownClauseKeyword returns true if the current token is a clause keyword
// that's not in the provided sequence (i.e., unsupported by this dialect).
func (p *Parser) isUnknownClauseKeyword(sequence []token.TokenType) bool {
	// Check if current token is a known clause keyword
	knownClauseKeywords := []TokenType{
		TOKEN_WHERE, TOKEN_GROUP, TOKEN_HAVING, TOKEN_WINDOW,
		TOKEN_ORDER, TOKEN_LIMIT,
	}
	// Add QUALIFY if registered by a dialect
	if qualify := tokenQualify(); qualify != TOKEN_ILLEGAL {
		knownClauseKeywords = append(knownClauseKeywords, qualify)
	}

	for _, kw := range knownClauseKeywords {
		if p.check(kw) {
			// It's a known clause keyword - check if it's in the sequence
			for _, seqTok := range sequence {
				if seqTok == kw {
					return false // In sequence, not unknown
				}
			}
			return true // Known clause but not in this dialect's sequence
		}
	}

	return false
}

// assignClauseResult assigns the parsed clause result to the appropriate field in SelectCore.
func (p *Parser) assignClauseResult(core *SelectCore, clauseType token.TokenType, result any) {
	if result == nil {
		return
	}

	switch clauseType {
	case token.WHERE:
		if expr, ok := result.(Expr); ok {
			core.Where = expr
		}

	case token.GROUP:
		switch v := result.(type) {
		case []Expr:
			core.GroupBy = v
		case []spi.Expr:
			// Convert []spi.Expr to []Expr
			exprs := make([]Expr, len(v))
			for i, e := range v {
				if expr, ok := e.(Expr); ok {
					exprs[i] = expr
				}
			}
			core.GroupBy = exprs
		case []any:
			// Convert generic slice to []Expr
			exprs := make([]Expr, len(v))
			for i, e := range v {
				if expr, ok := e.(Expr); ok {
					exprs[i] = expr
				}
			}
			core.GroupBy = exprs
		}

	case token.HAVING:
		if expr, ok := result.(Expr); ok {
			core.Having = expr
		}

	case token.WINDOW:
		// Window definitions are complex - handled specially if needed
		// For now, we don't have full WINDOW clause support

	case token.ORDER:
		switch v := result.(type) {
		case []OrderByItem:
			core.OrderBy = v
		case []spi.OrderByItem:
			// Convert from spi types
			items := make([]OrderByItem, len(v))
			for i, item := range v {
				if obi, ok := item.(OrderByItem); ok {
					items[i] = obi
				}
			}
			core.OrderBy = items
		case []any:
			// Convert from generic slice
			items := make([]OrderByItem, len(v))
			for i, item := range v {
				if obi, ok := item.(OrderByItem); ok {
					items[i] = obi
				}
			}
			core.OrderBy = items
		}

	case token.LIMIT:
		if expr, ok := result.(Expr); ok {
			core.Limit = expr
		}

	default:
		// Check for QUALIFY (dynamic token) - compare by name since dialects may register their own
		if clauseType.String() == "QUALIFY" {
			if expr, ok := result.(Expr); ok {
				core.Qualify = expr
			}
		} else {
			// Unknown clause - add to extensions if it's a Node
			if node, ok := result.(Node); ok {
				core.Extensions = append(core.Extensions, node)
			}
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
