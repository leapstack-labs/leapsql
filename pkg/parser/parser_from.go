package parser

import (
	"github.com/leapstack-labs/leapsql/pkg/core"
)

// FROM clause parsing: table references, derived tables, lateral joins, JOINs.
//
// Grammar:
//
//	from_clause   → table_ref (join)*
//	table_ref     → table_name | derived_table | lateral_table
//	table_name    → [catalog "."] [schema "."] identifier [AS identifier]
//	derived_table → "(" statement ")" [AS] identifier
//	lateral_table → LATERAL "(" statement ")" [AS] identifier
//	join          → join_type JOIN table_ref [ON expr] | "," table_ref
//	join_type     → [INNER] | LEFT [OUTER] | RIGHT [OUTER] | FULL [OUTER] | CROSS

// joinInner is the default join type for "plain JOIN" syntax.
// This is defined locally to avoid import cycles with dialect packages.
// The value matches what ANSI dialect registers for token.INNER.
const joinInner core.JoinType = "INNER"

// parseFromClause parses the FROM clause.
func (p *Parser) parseFromClause() *core.FromClause {
	from := &core.FromClause{}
	from.Source = p.parseTableRef()

	// Check for PIVOT/UNPIVOT extensions (transforms the source)
	from.Source = p.parseFromItemExtensions(from.Source)

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

// parseFromItemExtensions checks for dialect-specific FROM extensions (e.g., PIVOT, UNPIVOT).
func (p *Parser) parseFromItemExtensions(source core.TableRef) core.TableRef {
	if p.dialect == nil {
		return source
	}

	for {
		handler := p.dialect.FromItemHandler(p.token.Type)
		if handler == nil {
			break
		}

		p.nextToken() // consume the keyword (PIVOT, UNPIVOT, etc.)

		result, err := handler(p, source)
		if err != nil {
			p.addError(err.Error())
			break
		}

		source = result
	}

	return source
}

// parseTableRef parses a table reference.
func (p *Parser) parseTableRef() core.TableRef {
	// LATERAL subquery
	if p.match(TOKEN_LATERAL) {
		return p.parseLateralTable()
	}

	// Derived table (subquery)
	if p.check(TOKEN_LPAREN) {
		return p.parseDerivedTable()
	}

	// Macro table reference (e.g., {{ ref('table') }})
	if p.check(TOKEN_MACRO) {
		return p.parseMacroTable()
	}

	// Simple table name
	return p.parseTableName()
}

// parseTableName parses a table name with optional schema/catalog.
func (p *Parser) parseTableName() *core.TableName {
	table := &core.TableName{}

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

// parseDerivedTable parses a derived table (subquery in FROM).
func (p *Parser) parseDerivedTable() *core.DerivedTable {
	p.expect(TOKEN_LPAREN)
	derived := &core.DerivedTable{}
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
func (p *Parser) parseLateralTable() *core.LateralTable {
	p.expect(TOKEN_LPAREN)
	lateral := &core.LateralTable{}
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

// parseMacroTable parses a macro used as a table reference.
func (p *Parser) parseMacroTable() *core.MacroTable {
	macro := &core.MacroTable{
		Content: p.token.Literal,
	}
	macro.Span = p.makeSpan(p.token.Pos)
	p.nextToken()

	// Optional alias
	if p.match(TOKEN_AS) {
		if p.check(TOKEN_IDENT) {
			macro.Alias = p.token.Literal
			p.nextToken()
		}
	} else if p.check(TOKEN_IDENT) && !p.isJoinKeyword(p.token) && !p.isClauseKeyword(p.token) {
		macro.Alias = p.token.Literal
		p.nextToken()
	}

	return macro
}

// parseJoin parses a JOIN clause.
func (p *Parser) parseJoin() *core.Join {
	join := &core.Join{}

	// Comma join (implicit cross join) - hardcoded special case
	if p.match(TOKEN_COMMA) {
		join.Type = core.JoinComma
		join.Right = p.parseTableRef()
		return join
	}

	// Check for NATURAL modifier first
	if p.match(TOKEN_NATURAL) {
		join.Natural = true
	}

	// Try dialect join type lookup (covers standard + extensions)
	if p.dialect != nil {
		if def, ok := p.dialect.JoinTypeDef(p.token.Type); ok {
			join.Type = core.JoinType(def.Type)
			p.nextToken()

			// Handle optional modifier (OUTER for LEFT/RIGHT/FULL)
			if def.OptionalToken != 0 {
				p.match(def.OptionalToken)
			}

			// Check for compound syntax (LEFT SEMI, LEFT ANTI)
			if subDef, ok := p.dialect.JoinTypeDef(p.token.Type); ok {
				join.Type = core.JoinType(subDef.Type)
				p.nextToken()
			}

			if !p.expect(TOKEN_JOIN) {
				return nil
			}

			join.Right = p.parseTableRef()
			p.parseJoinCondition(join)
			return join
		}
	}

	// Plain JOIN (no type keyword) = INNER JOIN
	switch {
	case p.check(TOKEN_JOIN):
		join.Type = joinInner
	case join.Natural && p.check(TOKEN_JOIN):
		// NATURAL JOIN = NATURAL INNER JOIN
		join.Type = joinInner
	case !join.Natural:
		return nil // no join
	}

	if !p.expect(TOKEN_JOIN) {
		return nil
	}

	join.Right = p.parseTableRef()
	p.parseJoinCondition(join)
	return join
}

// parseJoinCondition handles ON/USING/NATURAL validation.
func (p *Parser) parseJoinCondition(join *core.Join) {
	switch {
	case join.Natural:
		// NATURAL JOIN cannot have ON or USING
		if p.check(TOKEN_ON) {
			p.addError("NATURAL JOIN cannot have ON clause")
		}
		if p.check(TOKEN_USING) {
			p.addError("NATURAL JOIN cannot have USING clause")
		}
	case p.match(TOKEN_ON):
		join.Condition = p.parseExpression()
	case p.match(TOKEN_USING):
		join.Using = p.parseUsingColumns()
	}
}

// parseUsingColumns parses the column list in USING (col1, col2, ...).
func (p *Parser) parseUsingColumns() []string {
	p.expect(TOKEN_LPAREN)
	var cols []string
	for {
		if !p.check(TOKEN_IDENT) {
			p.addError("expected column name in USING clause")
			break
		}
		cols = append(cols, p.token.Literal)
		p.nextToken()
		if !p.match(TOKEN_COMMA) {
			break
		}
	}
	p.expect(TOKEN_RPAREN)
	return cols
}
