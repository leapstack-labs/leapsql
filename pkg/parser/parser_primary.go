package parser

import (
	"github.com/leapstack-labs/leapsql/pkg/core"
	"fmt"
	"strings"

	"github.com/leapstack-labs/leapsql/pkg/token"
)

// Primary expression parsing: literals, column refs, function calls.
//
// Grammar:
//
//	primary       → literal | column_ref | func_call | paren_expr | case_expr | cast_expr | exists_expr
//	literal       → NUMBER | STRING | TRUE | FALSE | NULL
//	column_ref    → [table "."] column | [schema "." table "."] column
//	func_call     → identifier "(" [DISTINCT] [expr_list | "*"] ")" [FILTER "(" WHERE expr ")"] [OVER window_spec]

// parsePrimary parses primary expressions.
func (p *Parser) parsePrimary() core.Expr {
	// Check for dialect-specific prefix handlers first
	if p.dialect != nil {
		if handler := p.dialect.PrefixHandler(p.token.Type); handler != nil {
			p.nextToken() // consume the prefix token
			expr, err := handler(p)
			if err != nil {
				p.addError(err.Error())
				return nil
			}
			if expr != nil {
				return expr
			}
			return nil
		}
	}

	switch p.token.Type {
	case TOKEN_NUMBER:
		lit := &core.Literal{Type: core.LiteralNumber, Value: p.token.Literal}
		p.nextToken()
		return lit

	case TOKEN_STRING:
		lit := &core.Literal{Type: core.LiteralString, Value: p.token.Literal}
		p.nextToken()
		return lit

	case TOKEN_TRUE:
		p.nextToken()
		return &core.Literal{Type: core.LiteralBool, Value: "true"}

	case TOKEN_FALSE:
		p.nextToken()
		return &core.Literal{Type: core.LiteralBool, Value: "false"}

	case TOKEN_NULL:
		p.nextToken()
		return &core.Literal{Type: core.LiteralNull, Value: "null"}

	case TOKEN_CASE:
		return p.parseCaseExpr()

	case TOKEN_CAST:
		return p.parseCastExpr()

	case TOKEN_NOT:
		// EXISTS check
		if p.checkPeek(TOKEN_EXISTS) {
			p.nextToken() // consume NOT
			return p.parseExistsExpr(true)
		}
		// Regular NOT expression
		p.nextToken()
		return &core.UnaryExpr{Op: token.NOT, Expr: p.parsePrimary()}

	case TOKEN_EXISTS:
		return p.parseExistsExpr(false)

	case TOKEN_IDENT:
		return p.parseIdentifierExpr()

	case TOKEN_LPAREN:
		return p.parseParenExpr()

	case TOKEN_STAR:
		// SELECT * context
		p.nextToken()
		return &core.StarExpr{}

	case TOKEN_MACRO:
		macro := &core.MacroExpr{
			Content: p.token.Literal,
		}
		macro.Span = token.Span{
			Start: p.token.Pos,
			End:   p.tokenEnd(),
		}
		p.nextToken()
		return macro

	default:
		p.addError(fmt.Sprintf("unexpected token in expression: %s", p.token.Type))
		p.nextToken()
		return nil
	}
}

// parseIdentifierExpr parses an identifier which could be a column ref or function call.
func (p *Parser) parseIdentifierExpr() core.Expr {
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
	return &core.ColumnRef{Column: name}
}

// parseQualifiedColumnRef parses a qualified column reference.
func (p *Parser) parseQualifiedColumnRef(firstPart string) core.Expr {
	parts := []string{firstPart}

	for p.match(TOKEN_DOT) {
		// Check for table.*
		if p.check(TOKEN_STAR) {
			p.nextToken()
			return &core.StarExpr{Table: firstPart}
		}

		if p.check(TOKEN_IDENT) {
			parts = append(parts, p.token.Literal)
			p.nextToken()
		}
	}

	// Build column reference
	ref := &core.ColumnRef{}
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
func (p *Parser) parseFuncCall(name string) core.Expr {
	fn := &core.FuncCall{Name: strings.ToUpper(name)}

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
