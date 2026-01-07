package parser

import (
	"fmt"
	"github.com/leapstack-labs/leapsql/pkg/core"

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
func (p *Parser) parseStatement() *core.SelectStmt {
	stmt := &core.SelectStmt{}

	// Optional WITH clause
	if p.check(TOKEN_WITH) {
		stmt.With = p.parseWithClause()
	}

	// Required SELECT body
	stmt.Body = p.parseSelectBody()

	return stmt
}

// parseWithClause parses a WITH clause with CTEs.
func (p *Parser) parseWithClause() *core.WithClause {
	p.expect(TOKEN_WITH)
	with := &core.WithClause{}

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
func (p *Parser) parseCTE() *core.CTE {
	cte := &core.CTE{}

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
func (p *Parser) parseSelectBody() *core.SelectBody {
	body := &core.SelectBody{}
	body.Left = p.parseSelectCore()

	// Check for set operations
	if p.check(TOKEN_UNION) || p.check(TOKEN_INTERSECT) || p.check(TOKEN_EXCEPT) {
		switch p.token.Type {
		case TOKEN_UNION:
			p.nextToken()
			if p.match(TOKEN_ALL) {
				body.Op = core.SetOpUnionAll
				body.All = true
			} else {
				body.Op = core.SetOpUnion
				p.match(TOKEN_DISTINCT) // optional
			}
		case TOKEN_INTERSECT:
			p.nextToken()
			body.Op = core.SetOpIntersect
			p.match(TOKEN_ALL) // optional
		case TOKEN_EXCEPT:
			p.nextToken()
			body.Op = core.SetOpExcept
			p.match(TOKEN_ALL) // optional
		}

		// DuckDB extension: BY NAME (match columns by name, not position)
		if p.check(TOKEN_BY) {
			p.nextToken() // consume BY
			if p.matchSoftKeyword(SoftKeywordName) {
				body.ByName = true
			} else {
				p.addError("expected NAME after BY in set operation")
			}
		}

		// Parse the right side (recursively for chained operations)
		body.Right = p.parseSelectBody()
	}

	return body
}

// parseSelectCore parses a single SELECT clause.
func (p *Parser) parseSelectCore() *core.SelectCore {
	p.expect(TOKEN_SELECT)
	sc := &core.SelectCore{}

	// DISTINCT / ALL
	if p.match(TOKEN_DISTINCT) {
		sc.Distinct = true
	} else {
		p.match(TOKEN_ALL) // optional, consume if present
	}

	// SELECT list
	sc.Columns = p.parseSelectList()

	// FROM clause (required for our use case)
	if p.match(TOKEN_FROM) {
		sc.From = p.parseFromClause()
	}

	// Parse optional clauses using dialect-driven approach
	p.parseClauses(sc)

	return sc
}

// parseClauses parses optional clauses using the dialect's clause sequence and handlers.
func (p *Parser) parseClauses(sc *core.SelectCore) {
	p.parseClausesWithDialect(sc)
}

// parseClausesWithDialect parses clauses using dialect.ClauseDef() for both
// parsing logic and slot-based assignment. This is fully declarative -
// no hardcoded clause knowledge in the parser.
func (p *Parser) parseClausesWithDialect(sc *core.SelectCore) {
	sequence := p.dialect.ClauseSequence()
	if sequence == nil {
		return // No clauses to parse for this dialect
	}

	for {
		matched := false

		// Try to match against any clause in the sequence
		for _, clauseType := range sequence {
			if p.check(clauseType) {
				def, ok := p.dialect.ClauseDefFor(clauseType)
				if !ok {
					p.addError(fmt.Sprintf("no definition for clause %s in dialect %s", clauseType, p.dialect.Name))
					p.nextToken()
					matched = true
					break
				}

				p.nextToken() // consume clause keyword

				handler := def.Handler.(spi.ClauseHandler)
				result, err := handler(p)
				if err != nil {
					p.addError(err.Error())
				}

				// Use slot-based assignment (declarative)
				p.assignToSlot(sc, def.Slot, result)
				matched = true
				break
			}
		}

		// Check for unsupported clause (known globally but not in this dialect)
		if !matched {
			if name, isKnown := core.IsKnownClause(p.token.Type); isKnown {
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
}

// assignToSlot stores the parsed clause result in the appropriate SelectCore field.
// This uses the declarative ClauseSlot enum to determine where to store data.
func (p *Parser) assignToSlot(sc *core.SelectCore, slot spi.ClauseSlot, result any) {
	if result == nil {
		return
	}

	switch slot {
	case spi.SlotWhere:
		if expr, ok := result.(core.Expr); ok {
			sc.Where = expr
		}

	case spi.SlotGroupBy:
		// Check for GROUP BY ALL marker (DuckDB extension)
		if marker, ok := result.(spi.GroupByAllMarker); ok && marker.IsGroupByAll() {
			sc.GroupByAll = true
			return
		}
		switch v := result.(type) {
		case []core.Expr:
			sc.GroupBy = v
		case []any:
			exprs := make([]core.Expr, len(v))
			for i, e := range v {
				if expr, ok := e.(core.Expr); ok {
					exprs[i] = expr
				}
			}
			sc.GroupBy = exprs
		}

	case spi.SlotHaving:
		if expr, ok := result.(core.Expr); ok {
			sc.Having = expr
		}

	case spi.SlotWindow:
		// Window definitions are complex - for now skip
		// TODO: Add window definitions support

	case spi.SlotOrderBy:
		// Check for ORDER BY ALL marker (DuckDB extension)
		if marker, ok := result.(spi.OrderByAllMarker); ok && marker.IsOrderByAll() {
			sc.OrderByAll = true
			sc.OrderByAllDesc = marker.IsDesc()
			return
		}
		switch v := result.(type) {
		case []core.OrderByItem:
			sc.OrderBy = v
		case []any:
			items := make([]core.OrderByItem, len(v))
			for i, item := range v {
				if obi, ok := item.(core.OrderByItem); ok {
					items[i] = obi
				}
			}
			sc.OrderBy = items
		}

	case spi.SlotLimit:
		if expr, ok := result.(core.Expr); ok {
			sc.Limit = expr
		}

	case spi.SlotOffset:
		if expr, ok := result.(core.Expr); ok {
			sc.Offset = expr
		}

	case spi.SlotQualify:
		if expr, ok := result.(core.Expr); ok {
			sc.Qualify = expr
		}

	case spi.SlotFetch:
		// Handle both parser.FetchClause and dialect-defined FetchClause types
		if fetch, ok := result.(*core.FetchClause); ok {
			sc.Fetch = fetch
		} else if result != nil {
			// Handle dialect-defined FetchClause by extracting fields via reflection-free interface
			sc.Fetch = convertToFetchClause(result)
		}

	case spi.SlotExtensions:
		if node, ok := result.(core.Node); ok {
			sc.Extensions = append(sc.Extensions, node)
		}
	}
}

// FetchClauseData is an interface for extracting data from dialect-defined FetchClause types.
type FetchClauseData interface {
	GetFirst() bool
	GetCount() spi.Expr
	GetPercent() bool
	GetWithTies() bool
}

// convertToFetchClause converts a dialect-defined FetchClause to parser.FetchClause.
func convertToFetchClause(result any) *core.FetchClause {
	if data, ok := result.(FetchClauseData); ok {
		count := data.GetCount()
		return &core.FetchClause{
			First:    data.GetFirst(),
			Count:    count,
			Percent:  data.GetPercent(),
			WithTies: data.GetWithTies(),
		}
	}
	return nil
}

// parseSelectList parses the list of SELECT items.
func (p *Parser) parseSelectList() []core.SelectItem {
	var items []core.SelectItem

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
func (p *Parser) parseSelectItem() core.SelectItem {
	item := core.SelectItem{}

	// Check for * or table.*
	if p.check(TOKEN_STAR) {
		item.Star = true
		p.nextToken()
		// Parse optional star modifiers (DuckDB: EXCLUDE, REPLACE, RENAME)
		item.Modifiers = p.parseStarModifiers()
		return item
	}

	// Check for table.* pattern using 3-token lookahead (no rollback needed)
	if p.check(TOKEN_IDENT) && p.checkPeek(TOKEN_DOT) && p.checkPeek2(TOKEN_STAR) {
		tableName := p.token.Literal
		p.nextToken() // consume identifier
		p.nextToken() // consume DOT
		p.nextToken() // consume STAR
		item.TableStar = tableName
		// Parse optional star modifiers (DuckDB: EXCLUDE, REPLACE, RENAME)
		item.Modifiers = p.parseStarModifiers()
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

// parseStarModifiers parses optional EXCLUDE/REPLACE/RENAME modifiers after * or table.*.
func (p *Parser) parseStarModifiers() []core.StarModifier {
	var modifiers []core.StarModifier

	for {
		h := p.dialect.StarModifierHandler(p.token.Type)
		if h == nil {
			break
		}
		handler := h.(spi.StarModifierHandler)

		p.nextToken() // consume modifier keyword

		mod, err := handler(p)
		if err != nil {
			p.addError(err.Error())
			break
		}

		if mod != nil {
			modifiers = append(modifiers, mod)
		}
	}

	return modifiers
}

// parseOrderByList parses a list of ORDER BY items.
func (p *Parser) parseOrderByList() []core.OrderByItem {
	var items []core.OrderByItem

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
func (p *Parser) parseOrderByItem() core.OrderByItem {
	item := core.OrderByItem{}
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
func (p *Parser) parseExpressionList() []core.Expr {
	var exprs []core.Expr

	for {
		expr := p.parseExpression()
		exprs = append(exprs, expr)

		if !p.match(TOKEN_COMMA) {
			break
		}
	}

	return exprs
}
