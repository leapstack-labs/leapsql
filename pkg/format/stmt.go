package format

import (
	"github.com/leapstack-labs/leapsql/pkg/dialect"
	"github.com/leapstack-labs/leapsql/pkg/parser"
	"github.com/leapstack-labs/leapsql/pkg/spi"
	"github.com/leapstack-labs/leapsql/pkg/token"
)

func (p *Printer) formatSelectStmt(stmt *parser.SelectStmt) {
	if stmt == nil {
		return
	}

	p.formatLeadingComments(stmt.LeadingComments)

	if stmt.With != nil {
		p.formatWithClause(stmt.With)
	}

	if stmt.Body != nil {
		p.formatSelectBody(stmt.Body)
	}

	p.formatTrailingComments(stmt.TrailingComments)
}

func (p *Printer) formatWithClause(with *parser.WithClause) {
	p.kw(token.WITH)
	if with.Recursive {
		p.space()
		p.kw(token.RECURSIVE)
	}
	p.writeln()

	p.indent()
	for i, cte := range with.CTEs {
		p.write(cte.Name)
		p.space()
		p.kw(token.AS)
		p.write(" (")
		p.writeln()

		p.indent()
		p.formatSelectStmt(cte.Select)
		p.dedent()

		p.write(")")
		if i < len(with.CTEs)-1 {
			p.write(",")
		}
		p.writeln()
	}
	p.dedent()
}

func (p *Printer) formatSelectBody(body *parser.SelectBody) {
	if body == nil {
		return
	}

	p.formatSelectCore(body.Left)

	if body.Op != parser.SetOpNone {
		// Use token mapping for set operations if possible, otherwise keep string conversion for now
		// assuming SetOpType string values match keywords
		// Ideally parser.SetOpType would map to tokens or we use a switch here
		switch body.Op {
		case parser.SetOpUnion:
			p.kw(token.UNION)
		case parser.SetOpUnionAll:
			p.kw(token.UNION, token.ALL)
		case parser.SetOpIntersect:
			p.kw(token.INTERSECT)
		case parser.SetOpExcept:
			p.kw(token.EXCEPT)
		}
		p.writeln()
		p.formatSelectBody(body.Right)
	}
}

func (p *Printer) formatSelectCore(core *parser.SelectCore) {
	if core == nil {
		return
	}

	// SELECT [DISTINCT]
	p.kw(token.SELECT)
	if core.Distinct {
		p.space()
		p.kw(token.DISTINCT)
	}
	p.writeln()

	// Columns
	p.indent()
	for i, col := range core.Columns {
		p.formatSelectItem(col)
		if i < len(core.Columns)-1 {
			p.write(",")
		}
		p.writeln()
	}
	p.dedent()

	// FROM
	if core.From != nil {
		p.kw(token.FROM)
		p.space()
		p.formatFromClause(core.From)
		p.writeln()
	}

	// Dynamic clauses
	sequence := p.dialect.ClauseSequence()
	for _, clauseType := range sequence {
		def, ok := p.dialect.ClauseDef(clauseType)
		if !ok {
			continue
		}

		p.formatClause(clauseType, def, core)
	}
}

func (p *Printer) formatClause(t token.TokenType, def dialect.ClauseDef, core *parser.SelectCore) {
	val := p.getClauseValue(core, def.Slot)
	// Only print if the value is non-nil/non-empty
	if !hasValue(val) {
		return
	}

	// Print keywords
	if len(def.Keywords) > 0 {
		for i, kw := range def.Keywords {
			if i > 0 {
				p.space()
			}
			p.keyword(kw) // Keep keyword() here as def.Keywords are strings from Dialect
		}
	} else {
		p.kw(t)
	}

	// Inline clauses (LIMIT, OFFSET)
	if def.Slot == spi.SlotLimit || def.Slot == spi.SlotOffset {
		p.space()
		p.formatSlotValue(def.Slot, val)
		p.writeln()
		return
	}

	// Block clauses (WHERE, GROUP BY, etc.)
	p.writeln()
	p.indent()
	p.formatSlotValue(def.Slot, val)
	p.writeln()
	p.dedent()
}

func hasValue(val any) bool {
	if val == nil {
		return false
	}
	switch v := val.(type) {
	case []parser.Expr:
		return len(v) > 0
	case []parser.OrderByItem:
		return len(v) > 0
	}
	return true
}

func (p *Printer) getClauseValue(core *parser.SelectCore, slot spi.ClauseSlot) any {
	switch slot {
	case spi.SlotWhere:
		return core.Where
	case spi.SlotGroupBy:
		return core.GroupBy
	case spi.SlotHaving:
		return core.Having
	case spi.SlotWindow:
		return nil // TODO
	case spi.SlotOrderBy:
		return core.OrderBy
	case spi.SlotLimit:
		return core.Limit
	case spi.SlotOffset:
		return core.Offset
	case spi.SlotQualify:
		return core.Qualify
	}
	return nil
}

func (p *Printer) formatSlotValue(slot spi.ClauseSlot, val any) {
	switch slot {
	case spi.SlotWhere, spi.SlotHaving, spi.SlotLimit, spi.SlotOffset, spi.SlotQualify:
		if expr, ok := val.(parser.Expr); ok {
			p.formatExpr(expr)
		}
	case spi.SlotGroupBy:
		if exprs, ok := val.([]parser.Expr); ok {
			for i, expr := range exprs {
				p.formatExpr(expr)
				if i < len(exprs)-1 {
					p.write(",")
					p.writeln() // Add newline after comma for GROUP BY
				}
			}
		}
	case spi.SlotOrderBy:
		if items, ok := val.([]parser.OrderByItem); ok {
			for i, item := range items {
				p.formatOrderByItem(item)
				if i < len(items)-1 {
					p.write(",")
					p.writeln() // Add newline after comma for ORDER BY
				}
			}
		}
	}
}

func (p *Printer) formatSelectItem(item parser.SelectItem) {
	if item.Star {
		p.write("*")
		return
	}
	if item.TableStar != "" {
		p.write(item.TableStar)
		p.write(".*")
		return
	}

	p.formatExpr(item.Expr)
	if item.Alias != "" {
		p.space()
		p.kw(token.AS)
		p.space()
		p.write(item.Alias)
	}
}

func (p *Printer) formatFromClause(from *parser.FromClause) {
	if from == nil {
		return
	}

	p.formatTableRef(from.Source)

	for _, join := range from.Joins {
		p.writeln()
		p.formatJoin(join)
	}
}

func (p *Printer) formatTableRef(ref parser.TableRef) {
	if ref == nil {
		return
	}

	switch t := ref.(type) {
	case *parser.TableName:
		p.formatTableName(t)
	case *parser.DerivedTable:
		p.formatDerivedTable(t)
	case *parser.LateralTable:
		p.formatLateralTable(t)
	case *parser.MacroTable:
		p.formatMacroTable(t)
	}
}

func (p *Printer) formatTableName(t *parser.TableName) {
	if t.Catalog != "" {
		p.write(t.Catalog)
		p.write(".")
	}
	if t.Schema != "" {
		p.write(t.Schema)
		p.write(".")
	}
	p.write(t.Name)
	if t.Alias != "" {
		p.space()
		p.write(t.Alias)
	}
}

func (p *Printer) formatDerivedTable(t *parser.DerivedTable) {
	p.write("(")
	p.writeln()
	p.indent()
	p.formatSelectStmt(t.Select)
	p.dedent()
	p.write(")")
	if t.Alias != "" {
		p.space()
		p.write(t.Alias)
	}
}

func (p *Printer) formatLateralTable(t *parser.LateralTable) {
	p.kw(token.LATERAL)
	p.write(" (")
	p.writeln()
	p.indent()
	p.formatSelectStmt(t.Select)
	p.dedent()
	p.write(")")
	if t.Alias != "" {
		p.space()
		p.write(t.Alias)
	}
}

func (p *Printer) formatMacroTable(t *parser.MacroTable) {
	// Macros are preserved exactly as written
	p.write(t.Content)
	if t.Alias != "" {
		p.space()
		p.write(t.Alias)
	}
}

func (p *Printer) formatJoin(join *parser.Join) {
	if join == nil {
		return
	}

	// Join type
	switch join.Type {
	case parser.JoinInner:
		p.kw(token.JOIN)
	case parser.JoinLeft:
		p.kw(token.LEFT, token.JOIN)
	case parser.JoinRight:
		p.kw(token.RIGHT, token.JOIN)
	case parser.JoinFull:
		p.kw(token.FULL, token.JOIN)
	case parser.JoinCross:
		p.kw(token.CROSS, token.JOIN)
	case parser.JoinComma:
		p.write(",")
	}
	p.space()

	p.formatTableRef(join.Right)

	// ON condition (indented)
	if join.Condition != nil {
		p.writeln()
		p.indent()
		p.kw(token.ON)
		p.space()
		p.formatExpr(join.Condition)
		p.dedent()
	}
}

func (p *Printer) formatOrderByItem(item parser.OrderByItem) {
	p.formatExpr(item.Expr)
	if item.Desc {
		p.space()
		p.kw(token.DESC)
	}
	if item.NullsFirst != nil {
		p.space()
		p.kw(token.NULLS)
		p.space()
		if *item.NullsFirst {
			p.kw(token.FIRST)
		} else {
			p.kw(token.LAST)
		}
	}
}
