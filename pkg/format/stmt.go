package format

import (
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/dialect"
	"github.com/leapstack-labs/leapsql/pkg/spi"
	"github.com/leapstack-labs/leapsql/pkg/token"
)

// joinInner is the JoinType value for plain INNER join.
// Defined locally to avoid import cycle with dialect packages.
const joinInner core.JoinType = "INNER"

func (p *Printer) formatSelectStmt(stmt *core.SelectStmt) {
	if stmt == nil {
		return
	}

	p.formatComments(stmt.LeadingComments)

	if stmt.With != nil {
		p.formatWithClause(stmt.With)
	}

	if stmt.Body != nil {
		p.formatSelectBody(stmt.Body)
	}

	p.formatTrailingComments(stmt.TrailingComments)
}

func (p *Printer) formatWithClause(with *core.WithClause) {
	p.kw(token.WITH)
	if with.Recursive {
		p.space()
		p.kw(token.RECURSIVE)
	}
	p.writeln()

	p.indent()
	p.formatList(len(with.CTEs), func(i int) {
		cte := with.CTEs[i]
		p.write(cte.Name)
		p.space()
		p.kw(token.AS)
		p.write(" (")
		p.writeln()

		p.indent()
		p.formatSelectStmt(cte.Select)
		p.dedent()

		p.write(")")
	}, ",", true)
	p.writeln()
	p.dedent()
}

func (p *Printer) formatSelectBody(body *core.SelectBody) {
	if body == nil {
		return
	}

	p.formatSelectCore(body.Left)

	if body.Op != core.SetOpNone {
		// Use token mapping for set operations if possible, otherwise keep string conversion for now
		// assuming SetOpType string values match keywords
		// Ideally parser.SetOpType would map to tokens or we use a switch here
		switch body.Op {
		case core.SetOpUnion:
			p.kw(token.UNION)
		case core.SetOpUnionAll:
			p.kw(token.UNION, token.ALL)
		case core.SetOpIntersect:
			p.kw(token.INTERSECT)
		case core.SetOpExcept:
			p.kw(token.EXCEPT)
		}

		// DuckDB extension: BY NAME (match columns by name, not position)
		if body.ByName {
			p.space()
			p.kw(token.BY)
			p.space()
			p.keyword("NAME") // Soft keyword - styled like keyword
		}

		p.writeln()
		p.formatSelectBody(body.Right)
	}
}

func (p *Printer) formatSelectCore(sc *core.SelectCore) {
	if sc == nil {
		return
	}

	// SELECT [DISTINCT]
	p.kw(token.SELECT)
	if sc.Distinct {
		p.space()
		p.kw(token.DISTINCT)
	}
	p.writeln()

	// Columns
	p.indent()
	p.formatList(len(sc.Columns), func(i int) { p.formatSelectItem(sc.Columns[i]) }, ",", true)
	p.writeln()
	p.dedent()

	// FROM
	if sc.From != nil {
		p.kw(token.FROM)
		p.space()
		p.formatFromClause(sc.From)
		p.writeln()
	}

	// Dynamic clauses
	sequence := p.dialect.ClauseSequence()
	for _, clauseType := range sequence {
		def, ok := p.dialect.ClauseDef(clauseType)
		if !ok {
			continue
		}

		p.formatClause(clauseType, def, sc)
	}
}

func (p *Printer) formatClause(t token.TokenType, def dialect.ClauseDef, sc *core.SelectCore) {
	// Handle GROUP BY ALL (DuckDB extension)
	if def.Slot == spi.SlotGroupBy && sc.GroupByAll {
		p.kw(token.GROUP)
		p.space()
		p.kw(token.BY)
		p.space()
		p.kw(token.ALL)
		p.writeln()
		return
	}

	// Handle ORDER BY ALL (DuckDB extension)
	if def.Slot == spi.SlotOrderBy && sc.OrderByAll {
		p.kw(token.ORDER)
		p.space()
		p.kw(token.BY)
		p.space()
		p.kw(token.ALL)
		if sc.OrderByAllDesc {
			p.space()
			p.kw(token.DESC)
		}
		p.writeln()
		return
	}

	val := p.getClauseValue(sc, def.Slot)
	if !hasValue(val) {
		return
	}

	// Print keywords
	if len(def.Keywords) > 0 {
		for i, kw := range def.Keywords {
			if i > 0 {
				p.space()
			}
			p.keyword(kw)
		}
	} else {
		p.kw(t)
	}

	if def.Inline {
		p.space()
		p.formatSlotValue(def.Slot, val)
	} else {
		p.writeln()
		p.indent()
		p.formatSlotValue(def.Slot, val)
		p.dedent()
	}
	p.writeln()
}

func hasValue(val any) bool {
	if val == nil {
		return false
	}
	switch v := val.(type) {
	case []core.Expr:
		return len(v) > 0
	case []core.OrderByItem:
		return len(v) > 0
	case *core.FetchClause:
		return v != nil
	}
	return true
}

func (p *Printer) getClauseValue(sc *core.SelectCore, slot spi.ClauseSlot) any {
	switch slot {
	case spi.SlotWhere:
		return sc.Where
	case spi.SlotGroupBy:
		return sc.GroupBy
	case spi.SlotHaving:
		return sc.Having
	case spi.SlotWindow:
		return nil // TODO
	case spi.SlotOrderBy:
		return sc.OrderBy
	case spi.SlotLimit:
		return sc.Limit
	case spi.SlotOffset:
		return sc.Offset
	case spi.SlotQualify:
		return sc.Qualify
	case spi.SlotFetch:
		return sc.Fetch
	}
	return nil
}

func (p *Printer) formatSlotValue(slot spi.ClauseSlot, val any) {
	switch slot {
	case spi.SlotWhere, spi.SlotHaving, spi.SlotLimit, spi.SlotOffset, spi.SlotQualify:
		if expr, ok := val.(core.Expr); ok {
			p.formatExpr(expr)
		}
	case spi.SlotGroupBy:
		if exprs, ok := val.([]core.Expr); ok {
			p.formatList(len(exprs), func(i int) { p.formatExpr(exprs[i]) }, ",", true)
		}
	case spi.SlotOrderBy:
		if items, ok := val.([]core.OrderByItem); ok {
			p.formatList(len(items), func(i int) { p.formatOrderByItem(items[i]) }, ",", true)
		}
	case spi.SlotFetch:
		if fetch, ok := val.(*core.FetchClause); ok {
			p.formatFetchClause(fetch)
		}
	}
}

func (p *Printer) formatSelectItem(item core.SelectItem) {
	if item.Star {
		p.write("*")
		p.formatStarModifiers(item.Modifiers)
		return
	}
	if item.TableStar != "" {
		p.write(item.TableStar)
		p.write(".*")
		p.formatStarModifiers(item.Modifiers)
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

// formatStarModifiers formats EXCLUDE/REPLACE/RENAME modifiers for star expressions.
func (p *Printer) formatStarModifiers(mods []core.StarModifier) {
	for _, mod := range mods {
		p.space()
		switch m := mod.(type) {
		case *core.ExcludeModifier:
			p.keyword("EXCLUDE")
			p.write(" (")
			for i, col := range m.Columns {
				if i > 0 {
					p.write(", ")
				}
				p.write(col)
			}
			p.write(")")

		case *core.ReplaceModifier:
			p.keyword("REPLACE")
			p.write(" (")
			for i, item := range m.Items {
				if i > 0 {
					p.write(", ")
				}
				// item.Expr is already core.Expr
				p.formatExpr(item.Expr)
				p.space()
				p.kw(token.AS)
				p.space()
				p.write(item.Alias)
			}
			p.write(")")

		case *core.RenameModifier:
			p.keyword("RENAME")
			p.write(" (")
			for i, item := range m.Items {
				if i > 0 {
					p.write(", ")
				}
				p.write(item.OldName)
				p.space()
				p.kw(token.AS)
				p.space()
				p.write(item.NewName)
			}
			p.write(")")
		}
	}
}

func (p *Printer) formatFromClause(from *core.FromClause) {
	if from == nil {
		return
	}

	p.formatTableRef(from.Source)

	for _, join := range from.Joins {
		p.writeln()
		p.formatJoin(join)
	}
}

func (p *Printer) formatTableRef(ref core.TableRef) {
	if ref == nil {
		return
	}

	switch t := ref.(type) {
	case *core.TableName:
		p.formatTableName(t)
	case *core.DerivedTable:
		p.formatDerivedTable(t)
	case *core.LateralTable:
		p.formatLateralTable(t)
	case *core.MacroTable:
		p.formatMacroTable(t)
	case *core.PivotTable:
		p.formatPivotTable(t)
	case *core.UnpivotTable:
		p.formatUnpivotTable(t)
	}
}

func (p *Printer) formatTableName(t *core.TableName) {
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

func (p *Printer) formatDerivedTable(t *core.DerivedTable) {
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

func (p *Printer) formatLateralTable(t *core.LateralTable) {
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

func (p *Printer) formatMacroTable(t *core.MacroTable) {
	// Macros are preserved exactly as written
	p.write(t.Content)
	if t.Alias != "" {
		p.space()
		p.write(t.Alias)
	}
}

func (p *Printer) formatPivotTable(t *core.PivotTable) {
	// Format source table
	p.formatTableRef(t.Source)
	p.writeln()

	p.keyword("PIVOT")
	p.write(" (")
	p.indent()

	// Aggregates
	for i, agg := range t.Aggregates {
		if i > 0 {
			p.write(",")
		}
		p.writeln()
		p.formatExpr(agg.Func)
		if agg.Alias != "" {
			p.space()
			p.kw(token.AS)
			p.space()
			p.write(agg.Alias)
		}
	}

	// FOR column
	p.writeln()
	p.keyword("FOR")
	p.space()
	p.write(t.ForColumn)
	p.space()
	p.kw(token.IN)
	p.space()

	if t.InStar {
		p.write("*")
	} else {
		p.write("(")
		for i, val := range t.InValues {
			if i > 0 {
				p.write(", ")
			}
			p.formatExpr(val.Value)
			if val.Alias != "" {
				p.space()
				p.kw(token.AS)
				p.space()
				p.write(val.Alias)
			}
		}
		p.write(")")
	}

	p.dedent()
	p.writeln()
	p.write(")")

	if t.Alias != "" {
		p.space()
		p.write(t.Alias)
	}
}

func (p *Printer) formatUnpivotTable(t *core.UnpivotTable) {
	// Format source table
	p.formatTableRef(t.Source)
	p.writeln()

	p.keyword("UNPIVOT")
	p.write(" (")
	p.indent()

	// Value columns
	p.writeln()
	if len(t.ValueColumns) > 1 {
		p.write("(")
		for i, col := range t.ValueColumns {
			if i > 0 {
				p.write(", ")
			}
			p.write(col)
		}
		p.write(")")
	} else if len(t.ValueColumns) == 1 {
		p.write(t.ValueColumns[0])
	}

	// FOR name_column
	p.space()
	p.keyword("FOR")
	p.space()
	p.write(t.NameColumn)
	p.space()
	p.kw(token.IN)
	p.write(" (")

	for i, group := range t.InColumns {
		if i > 0 {
			p.write(", ")
		}
		if len(group.Columns) > 1 {
			p.write("(")
			for j, col := range group.Columns {
				if j > 0 {
					p.write(", ")
				}
				p.write(col)
			}
			p.write(")")
		} else if len(group.Columns) == 1 {
			p.write(group.Columns[0])
		}
		if group.Alias != "" {
			p.space()
			p.kw(token.AS)
			p.space()
			p.write(group.Alias)
		}
	}
	p.write(")")

	p.dedent()
	p.writeln()
	p.write(")")

	if t.Alias != "" {
		p.space()
		p.write(t.Alias)
	}
}

func (p *Printer) formatJoin(join *core.Join) {
	if join == nil {
		return
	}

	// NATURAL modifier
	if join.Natural {
		p.kw(token.NATURAL)
		p.space()
	}

	// Join type
	switch join.Type {
	case joinInner:
		// Plain "JOIN" for inner (most common, cleaner output)
		p.kw(token.JOIN)
	case core.JoinComma:
		p.write(",")
	default:
		// Data-driven: JoinType string IS the keyword
		p.keyword(string(join.Type))
		p.space()
		p.kw(token.JOIN)
	}
	p.space()

	p.formatTableRef(join.Right)

	// USING clause (alternative to ON)
	if len(join.Using) > 0 {
		p.writeln()
		p.indent()
		p.kw(token.USING)
		p.write(" (")
		for i, col := range join.Using {
			if i > 0 {
				p.write(", ")
			}
			p.write(col)
		}
		p.write(")")
		p.dedent()
	} else if join.Condition != nil {
		// ON condition (indented)
		p.writeln()
		p.indent()
		p.kw(token.ON)
		p.space()
		p.formatExpr(join.Condition)
		p.dedent()
	}
	// NATURAL JOIN has neither ON nor USING - nothing to add
}

func (p *Printer) formatOrderByItem(item core.OrderByItem) {
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

func (p *Printer) formatFetchClause(fetch *core.FetchClause) {
	if fetch == nil {
		return
	}

	p.kw(token.FETCH)
	p.space()

	if fetch.First {
		p.kw(token.FIRST)
	} else {
		p.kw(token.NEXT)
	}

	if fetch.Count != nil {
		p.space()
		p.formatExpr(fetch.Count)
		if fetch.Percent {
			p.write(" PERCENT")
		}
	}

	p.space()
	p.kw(token.ROWS)
	p.space()

	if fetch.WithTies {
		p.kw(token.WITH)
		p.space()
		p.kw(token.TIES)
	} else {
		p.kw(token.ONLY)
	}
}
