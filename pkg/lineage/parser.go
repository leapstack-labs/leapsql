package lineage

import (
	"fmt"
	"strings"
)

// Parser parses SQL into an AST.
type Parser struct {
	lexer        *Lexer
	token        Token // current token
	peek         Token // lookahead token
	peek2        Token // second lookahead token
	errors       []error
	inSelectList bool // true when parsing SELECT columns (to detect scalar subqueries)
}

// NewParser creates a new parser for the given SQL input.
func NewParser(sql string) *Parser {
	p := &Parser{
		lexer: NewLexer(sql),
	}
	// Read three tokens to initialize current, peek, and peek2
	p.nextToken()
	p.nextToken()
	p.nextToken()
	return p
}

// Parse parses the SQL and returns the AST.
func Parse(sql string) (*SelectStmt, error) {
	p := NewParser(sql)
	stmt := p.parseStatement()
	if len(p.errors) > 0 {
		return nil, p.errors[0]
	}
	return stmt, nil
}

// nextToken advances to the next token.
func (p *Parser) nextToken() {
	p.token = p.peek
	p.peek = p.peek2
	p.peek2 = p.lexer.NextToken()
}

// checkPeek2 returns true if the peek2 token is of the given type.
func (p *Parser) checkPeek2(t TokenType) bool {
	return p.peek2.Type == t
}

// check returns true if the current token is of the given type.
func (p *Parser) check(t TokenType) bool {
	return p.token.Type == t
}

// checkPeek returns true if the peek token is of the given type.
func (p *Parser) checkPeek(t TokenType) bool {
	return p.peek.Type == t
}

// match consumes the current token if it matches and returns true.
func (p *Parser) match(t TokenType) bool {
	if p.check(t) {
		p.nextToken()
		return true
	}
	return false
}

// matchAny consumes the current token if it matches any of the given types.
func (p *Parser) matchAny(types ...TokenType) bool {
	for _, t := range types {
		if p.check(t) {
			p.nextToken()
			return true
		}
	}
	return false
}

// expect consumes the current token if it matches, otherwise adds an error.
func (p *Parser) expect(t TokenType) bool {
	if p.check(t) {
		p.nextToken()
		return true
	}
	p.addError(fmt.Sprintf(ErrUnexpectedToken, p.token.Type, t))
	return false
}

// addError adds a parse error.
func (p *Parser) addError(msg string) {
	p.errors = append(p.errors, &ParseError{
		Pos:     p.token.Pos,
		Message: msg,
	})
}

// ---------- Statement Parsing ----------

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

// isKeyword returns true if the token is a reserved keyword that can't be used as alias.
func (p *Parser) isKeyword(tok Token) bool {
	switch tok.Type {
	case TOKEN_FROM, TOKEN_WHERE, TOKEN_GROUP, TOKEN_HAVING, TOKEN_ORDER,
		TOKEN_LIMIT, TOKEN_UNION, TOKEN_INTERSECT, TOKEN_EXCEPT,
		TOKEN_LEFT, TOKEN_RIGHT, TOKEN_INNER, TOKEN_OUTER, TOKEN_FULL,
		TOKEN_CROSS, TOKEN_JOIN, TOKEN_ON, TOKEN_QUALIFY:
		return true
	}
	return false
}

// parseFromClause parses the FROM clause.
func (p *Parser) parseFromClause() *FromClause {
	from := &FromClause{}
	from.Source = p.parseTableRef()

	// Parse JOINs
	for {
		join := p.parseJoin()
		if join == nil {
			break
		}
		from.Joins = append(from.Joins, join)
	}

	return from
}

// parseTableRef parses a table reference.
func (p *Parser) parseTableRef() TableRef {
	// LATERAL subquery
	if p.match(TOKEN_LATERAL) {
		return p.parseLateralTable()
	}

	// Derived table (subquery)
	if p.check(TOKEN_LPAREN) {
		return p.parseDerivedTable()
	}

	// Simple table name
	return p.parseTableName()
}

// parseTableName parses a table name with optional schema/catalog.
func (p *Parser) parseTableName() *TableName {
	table := &TableName{}

	if !p.check(TOKEN_IDENT) {
		p.addError("expected table name")
		return table
	}

	// Parse potentially qualified name: catalog.schema.table
	parts := []string{p.token.Literal}
	p.nextToken()

	for p.match(TOKEN_DOT) {
		if p.check(TOKEN_IDENT) {
			parts = append(parts, p.token.Literal)
			p.nextToken()
		}
	}

	switch len(parts) {
	case 1:
		table.Name = parts[0]
	case 2:
		table.Schema = parts[0]
		table.Name = parts[1]
	case 3:
		table.Catalog = parts[0]
		table.Schema = parts[1]
		table.Name = parts[2]
	}

	// Optional alias
	if p.match(TOKEN_AS) {
		if p.check(TOKEN_IDENT) {
			table.Alias = p.token.Literal
			p.nextToken()
		}
	} else if p.check(TOKEN_IDENT) && !p.isJoinKeyword(p.token) && !p.isClauseKeyword(p.token) {
		table.Alias = p.token.Literal
		p.nextToken()
	}

	return table
}

// isJoinKeyword returns true if token is a JOIN-related keyword.
func (p *Parser) isJoinKeyword(tok Token) bool {
	switch tok.Type {
	case TOKEN_JOIN, TOKEN_LEFT, TOKEN_RIGHT, TOKEN_INNER, TOKEN_OUTER,
		TOKEN_FULL, TOKEN_CROSS, TOKEN_ON, TOKEN_LATERAL:
		return true
	}
	return false
}

// isClauseKeyword returns true if token starts a new clause.
func (p *Parser) isClauseKeyword(tok Token) bool {
	switch tok.Type {
	case TOKEN_WHERE, TOKEN_GROUP, TOKEN_HAVING, TOKEN_ORDER, TOKEN_LIMIT,
		TOKEN_UNION, TOKEN_INTERSECT, TOKEN_EXCEPT, TOKEN_QUALIFY:
		return true
	}
	return false
}

// parseDerivedTable parses a derived table (subquery in FROM).
func (p *Parser) parseDerivedTable() *DerivedTable {
	p.expect(TOKEN_LPAREN)
	derived := &DerivedTable{}
	derived.Select = p.parseStatement()
	p.expect(TOKEN_RPAREN)

	// Alias is required for derived tables
	if p.match(TOKEN_AS) {
		if p.check(TOKEN_IDENT) {
			derived.Alias = p.token.Literal
			p.nextToken()
		}
	} else if p.check(TOKEN_IDENT) {
		derived.Alias = p.token.Literal
		p.nextToken()
	}

	return derived
}

// parseLateralTable parses a LATERAL subquery.
func (p *Parser) parseLateralTable() *LateralTable {
	p.expect(TOKEN_LPAREN)
	lateral := &LateralTable{}
	lateral.Select = p.parseStatement()
	p.expect(TOKEN_RPAREN)

	// Alias
	if p.match(TOKEN_AS) {
		if p.check(TOKEN_IDENT) {
			lateral.Alias = p.token.Literal
			p.nextToken()
		}
	} else if p.check(TOKEN_IDENT) {
		lateral.Alias = p.token.Literal
		p.nextToken()
	}

	return lateral
}

// parseJoin parses a JOIN clause.
func (p *Parser) parseJoin() *Join {
	join := &Join{}

	// Comma join (implicit cross join)
	if p.match(TOKEN_COMMA) {
		join.Type = JoinComma
		join.Right = p.parseTableRef()
		return join
	}

	// Determine join type
	switch {
	case p.match(TOKEN_CROSS):
		join.Type = JoinCross
		p.expect(TOKEN_JOIN)
		join.Right = p.parseTableRef()
		return join

	case p.match(TOKEN_LEFT):
		join.Type = JoinLeft
		p.match(TOKEN_OUTER) // optional

	case p.match(TOKEN_RIGHT):
		join.Type = JoinRight
		p.match(TOKEN_OUTER) // optional

	case p.match(TOKEN_FULL):
		join.Type = JoinFull
		p.match(TOKEN_OUTER) // optional

	case p.match(TOKEN_INNER):
		join.Type = JoinInner

	case p.check(TOKEN_JOIN):
		join.Type = JoinInner // default

	default:
		return nil // no join
	}

	if !p.expect(TOKEN_JOIN) {
		return nil
	}

	join.Right = p.parseTableRef()

	// ON clause
	if p.match(TOKEN_ON) {
		join.Condition = p.parseExpression()
	}

	return join
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

// ---------- Expression Parsing ----------

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
	not := false
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

// parsePrimary parses primary expressions.
func (p *Parser) parsePrimary() Expr {
	switch p.token.Type {
	case TOKEN_NUMBER:
		lit := &Literal{Type: LiteralNumber, Value: p.token.Literal}
		p.nextToken()
		return lit

	case TOKEN_STRING:
		lit := &Literal{Type: LiteralString, Value: p.token.Literal}
		p.nextToken()
		return lit

	case TOKEN_TRUE:
		p.nextToken()
		return &Literal{Type: LiteralBool, Value: "true"}

	case TOKEN_FALSE:
		p.nextToken()
		return &Literal{Type: LiteralBool, Value: "false"}

	case TOKEN_NULL:
		p.nextToken()
		return &Literal{Type: LiteralNull, Value: "null"}

	case TOKEN_CASE:
		return p.parseCaseExpr()

	case TOKEN_CAST:
		return p.parseCastExpr()

	case TOKEN_NOT:
		// EXISTS check
		if p.checkPeek(TOKEN_IDENT) && strings.ToLower(p.peek.Literal) == "exists" {
			p.nextToken() // consume NOT
			return p.parseExistsExpr(true)
		}
		// Regular NOT expression
		p.nextToken()
		return &UnaryExpr{Op: "NOT", Expr: p.parsePrimary()}

	case TOKEN_IDENT:
		// Check for EXISTS
		if strings.ToLower(p.token.Literal) == "exists" {
			return p.parseExistsExpr(false)
		}
		return p.parseIdentifierExpr()

	case TOKEN_LPAREN:
		return p.parseParenExpr()

	case TOKEN_STAR:
		// SELECT * context
		p.nextToken()
		return &StarExpr{}

	default:
		p.addError(fmt.Sprintf("unexpected token in expression: %s", p.token.Type))
		p.nextToken()
		return nil
	}
}

// parseIdentifierExpr parses an identifier which could be a column ref or function call.
func (p *Parser) parseIdentifierExpr() Expr {
	name := p.token.Literal
	p.nextToken()

	// Check if it's a function call
	if p.check(TOKEN_LPAREN) {
		return p.parseFuncCall(name)
	}

	// Qualified column reference: table.column or schema.table.column
	if p.check(TOKEN_DOT) {
		return p.parseQualifiedColumnRef(name)
	}

	// Simple column reference
	return &ColumnRef{Column: name}
}

// parseQualifiedColumnRef parses a qualified column reference.
func (p *Parser) parseQualifiedColumnRef(firstPart string) Expr {
	parts := []string{firstPart}

	for p.match(TOKEN_DOT) {
		// Check for table.*
		if p.check(TOKEN_STAR) {
			p.nextToken()
			return &StarExpr{Table: firstPart}
		}

		if p.check(TOKEN_IDENT) {
			parts = append(parts, p.token.Literal)
			p.nextToken()
		}
	}

	// Build column reference
	ref := &ColumnRef{}
	switch len(parts) {
	case 2:
		ref.Table = parts[0]
		ref.Column = parts[1]
	case 3:
		// schema.table.column - we'll use table.column for now
		ref.Table = parts[1]
		ref.Column = parts[2]
	default:
		ref.Column = parts[len(parts)-1]
	}

	return ref
}

// parseFuncCall parses a function call.
func (p *Parser) parseFuncCall(name string) Expr {
	fn := &FuncCall{Name: strings.ToUpper(name)}

	p.expect(TOKEN_LPAREN)

	// Handle COUNT(*) or other aggregate(*)
	if p.check(TOKEN_STAR) {
		fn.Star = true
		p.nextToken()
	} else if !p.check(TOKEN_RPAREN) {
		// Check for DISTINCT
		if p.match(TOKEN_DISTINCT) {
			fn.Distinct = true
		}

		// Parse arguments
		for {
			arg := p.parseExpression()
			fn.Args = append(fn.Args, arg)

			if !p.match(TOKEN_COMMA) {
				break
			}
		}
	}

	p.expect(TOKEN_RPAREN)

	// FILTER clause (for aggregates)
	if p.match(TOKEN_FILTER) {
		p.expect(TOKEN_LPAREN)
		p.expect(TOKEN_WHERE)
		fn.Filter = p.parseExpression()
		p.expect(TOKEN_RPAREN)
	}

	// OVER clause (window function)
	if p.match(TOKEN_OVER) {
		fn.Window = p.parseWindowSpec()
	}

	return fn
}

// parseWindowSpec parses a window specification.
func (p *Parser) parseWindowSpec() *WindowSpec {
	spec := &WindowSpec{}

	// Named window reference
	if p.check(TOKEN_IDENT) {
		spec.Name = p.token.Literal
		p.nextToken()
		return spec
	}

	p.expect(TOKEN_LPAREN)

	// PARTITION BY
	if p.match(TOKEN_PARTITION) {
		p.expect(TOKEN_BY)
		spec.PartitionBy = p.parseExpressionList()
	}

	// ORDER BY
	if p.match(TOKEN_ORDER) {
		p.expect(TOKEN_BY)
		spec.OrderBy = p.parseOrderByList()
	}

	// Frame specification
	if p.check(TOKEN_ROWS) || p.check(TOKEN_RANGE) || p.check(TOKEN_GROUPS) {
		spec.Frame = p.parseFrameSpec()
	}

	p.expect(TOKEN_RPAREN)
	return spec
}

// parseFrameSpec parses a window frame specification.
func (p *Parser) parseFrameSpec() *FrameSpec {
	frame := &FrameSpec{}

	// Frame type
	switch {
	case p.match(TOKEN_ROWS):
		frame.Type = FrameRows
	case p.match(TOKEN_RANGE):
		frame.Type = FrameRange
	case p.match(TOKEN_GROUPS):
		frame.Type = FrameGroups
	}

	// BETWEEN ... AND ...
	if p.match(TOKEN_BETWEEN) {
		frame.Start = p.parseFrameBound()
		p.expect(TOKEN_AND)
		frame.End = p.parseFrameBound()
	} else {
		// Single bound
		frame.Start = p.parseFrameBound()
	}

	return frame
}

// parseFrameBound parses a frame bound.
func (p *Parser) parseFrameBound() *FrameBound {
	bound := &FrameBound{}

	switch {
	case p.match(TOKEN_UNBOUNDED):
		if p.match(TOKEN_PRECEDING) {
			bound.Type = FrameUnboundedPreceding
		} else if p.match(TOKEN_FOLLOWING) {
			bound.Type = FrameUnboundedFollowing
		}

	case p.match(TOKEN_CURRENT):
		p.expect(TOKEN_ROW)
		bound.Type = FrameCurrentRow

	default:
		// N PRECEDING or N FOLLOWING
		bound.Offset = p.parseExpression()
		if p.match(TOKEN_PRECEDING) {
			bound.Type = FrameExprPreceding
		} else if p.match(TOKEN_FOLLOWING) {
			bound.Type = FrameExprFollowing
		}
	}

	return bound
}

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
				if p.token.Type == TOKEN_LPAREN {
					depth++
				} else if p.token.Type == TOKEN_RPAREN {
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
