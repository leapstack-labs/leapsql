// Package parser provides SQL parsing and column-level lineage extraction.
//
// # Parser Architecture
//
// The parser is split across multiple files for maintainability:
//
//   - parser.go (this file): Public API, Parser struct, token helpers
//   - parser_stmt.go: Statement parsing (WITH, SELECT body, ORDER BY)
//   - parser_from.go: FROM clause parsing (table refs, JOINs)
//   - parser_expr.go: Expression precedence parsing (OR, AND, comparisons, arithmetic)
//   - parser_primary.go: Primary expressions (literals, column refs, function calls)
//   - parser_window.go: Window specifications and frame specs
//   - parser_special.go: Special expressions (CASE, CAST, EXISTS, subqueries)
//
// # Usage
//
//	stmt, err := parser.Parse("SELECT a, b FROM t")
//	if err != nil {
//	    // handle error
//	}
//
// # Grammar Overview
//
// The parser implements a recursive descent parser for a subset of SQL:
//
//	statement     → [WITH cte_list] select_body
//	select_body   → select_core [(UNION|INTERSECT|EXCEPT) [ALL] select_body]
//	select_core   → SELECT [DISTINCT] select_list FROM from_clause
//	                [WHERE expr] [GROUP BY expr_list] [HAVING expr]
//	                [QUALIFY expr] [ORDER BY order_list] [LIMIT expr]
//
// See each file for detailed grammar rules for that section.
package parser

import (
	"fmt"

	"github.com/leapstack-labs/leapsql/pkg/dialect"
	_ "github.com/leapstack-labs/leapsql/pkg/dialects/ansi" // Register ANSI dialect for Parse()
	"github.com/leapstack-labs/leapsql/pkg/spi"
	"github.com/leapstack-labs/leapsql/pkg/token"
)

// Parser parses SQL into an AST.
type Parser struct {
	lexer        *Lexer
	token        Token // current token
	peek         Token // lookahead token
	peek2        Token // second lookahead token
	errors       []error
	inSelectList bool // true when parsing SELECT columns (to detect scalar subqueries)

	// Dialect support (optional - defaults to permissive parsing)
	dialect *dialect.Dialect
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

// NewParserWithDialect creates a new dialect-aware parser for the given SQL input.
func NewParserWithDialect(sql string, d *dialect.Dialect) *Parser {
	p := &Parser{
		lexer:   NewLexerWithDialect(sql, d),
		dialect: d,
	}
	// Read three tokens to initialize current, peek, and peek2
	p.nextToken()
	p.nextToken()
	p.nextToken()
	return p
}

// Parse parses the SQL and returns the AST using the ANSI dialect (strictest).
// Use ParseWithDialect for dialect-specific parsing, or ParsePermissive for
// backward-compatible permissive parsing that accepts all dialects.
func Parse(sql string) (*SelectStmt, error) {
	ansiDialect, ok := dialect.Get("ansi")
	if !ok {
		// Fallback to permissive if ANSI not registered (shouldn't happen)
		return ParsePermissive(sql)
	}
	return ParseWithDialect(sql, ansiDialect)
}

// ParsePermissive parses the SQL without dialect restrictions.
// This accepts all known SQL clauses regardless of dialect (backward-compatible mode).
func ParsePermissive(sql string) (*SelectStmt, error) {
	p := NewParser(sql)
	stmt := p.parseStatement()
	if len(p.errors) > 0 {
		return nil, p.errors[0]
	}
	return stmt, nil
}

// ParseWithDialect parses the SQL with a specific dialect and returns the AST.
// The dialect controls which syntax is allowed/rejected.
func ParseWithDialect(sql string, d *dialect.Dialect) (*SelectStmt, error) {
	p := NewParserWithDialect(sql, d)
	stmt := p.parseStatement()
	if len(p.errors) > 0 {
		return nil, p.errors[0]
	}
	return stmt, nil
}

// Dialect returns the parser's dialect, if any.
func (p *Parser) Dialect() *dialect.Dialect {
	return p.dialect
}

// ---------- Token Helpers ----------

// nextToken advances to the next token.
func (p *Parser) nextToken() {
	p.token = p.peek
	p.peek = p.peek2
	p.peek2 = p.lexer.NextToken()
}

// check returns true if the current token is of the given type.
func (p *Parser) check(t TokenType) bool {
	return p.token.Type == t
}

// checkPeek returns true if the peek token is of the given type.
func (p *Parser) checkPeek(t TokenType) bool {
	return p.peek.Type == t
}

// checkPeek2 returns true if the peek2 token is of the given type.
func (p *Parser) checkPeek2(t TokenType) bool {
	return p.peek2.Type == t
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
func (p *Parser) matchAny(types ...TokenType) bool { //nolint:unused // Reserved for future use
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

// ---------- Keyword Helpers ----------

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

// ---------- spi.ParserOps Implementation ----------
// These methods implement the spi.ParserOps interface for dialect clause handlers.

// Token returns the current token (implements spi.ParserOps).
func (p *Parser) Token() token.Token {
	return p.token
}

// Peek returns the lookahead token (implements spi.ParserOps).
func (p *Parser) Peek() token.Token {
	return p.peek
}

// Match consumes the current token if it matches (implements spi.ParserOps).
func (p *Parser) Match(t token.TokenType) bool {
	return p.match(t)
}

// Expect consumes the current token if it matches, otherwise returns an error (implements spi.ParserOps).
func (p *Parser) Expect(t token.TokenType) error {
	if p.check(t) {
		p.nextToken()
		return nil
	}
	return &ParseError{
		Pos:     p.token.Pos,
		Message: fmt.Sprintf(ErrUnexpectedToken, p.token.Type, t),
	}
}

// NextToken advances to the next token (implements spi.ParserOps).
func (p *Parser) NextToken() {
	p.nextToken()
}

// Check returns true if the current token is of the given type (implements spi.ParserOps).
func (p *Parser) Check(t token.TokenType) bool {
	return p.check(t)
}

// ParseExpression parses an expression (implements spi.ParserOps).
func (p *Parser) ParseExpression() (spi.Expr, error) {
	expr := p.parseExpression()
	if len(p.errors) > 0 {
		return nil, p.errors[len(p.errors)-1]
	}
	return expr, nil
}

// ParseExpressionList parses a comma-separated list of expressions (implements spi.ParserOps).
func (p *Parser) ParseExpressionList() ([]spi.Expr, error) {
	exprs := p.parseExpressionList()
	result := make([]spi.Expr, len(exprs))
	for i, e := range exprs {
		result[i] = e
	}
	if len(p.errors) > 0 {
		return nil, p.errors[len(p.errors)-1]
	}
	return result, nil
}

// ParseOrderByList parses an ORDER BY list (implements spi.ParserOps).
func (p *Parser) ParseOrderByList() ([]spi.OrderByItem, error) {
	items := p.parseOrderByList()
	result := make([]spi.OrderByItem, len(items))
	for i, item := range items {
		result[i] = item
	}
	if len(p.errors) > 0 {
		return nil, p.errors[len(p.errors)-1]
	}
	return result, nil
}

// ParseIdentifier parses an identifier (implements spi.ParserOps).
func (p *Parser) ParseIdentifier() (string, error) {
	if p.check(TOKEN_IDENT) {
		name := p.token.Literal
		p.nextToken()
		return name, nil
	}
	return "", &ParseError{
		Pos:     p.token.Pos,
		Message: fmt.Sprintf(ErrUnexpectedToken, p.token.Type, TOKEN_IDENT),
	}
}

// AddError adds a parse error (implements spi.ParserOps).
func (p *Parser) AddError(msg string) {
	p.addError(msg)
}

// Position returns the current token's position (implements spi.ParserOps).
func (p *Parser) Position() token.Position {
	return p.token.Pos
}
