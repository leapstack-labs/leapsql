package sql

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
