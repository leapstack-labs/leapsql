package format

import (
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/token"
)

const complexityThreshold = 5

func (p *Printer) formatExpr(e core.Expr) {
	if e == nil {
		return
	}

	switch expr := e.(type) {
	case *core.Literal:
		p.formatLiteral(expr)
	case *core.ColumnRef:
		p.formatColumnRef(expr)
	case *core.BinaryExpr:
		p.formatBinaryExpr(expr)
	case *core.UnaryExpr:
		p.formatUnaryExpr(expr)
	case *core.FuncCall:
		p.formatFuncCall(expr)
	case *core.CaseExpr:
		p.formatCaseExpr(expr)
	case *core.CastExpr:
		p.formatCastExpr(expr)
	case *core.InExpr:
		p.formatInExpr(expr)
	case *core.BetweenExpr:
		p.formatBetweenExpr(expr)
	case *core.IsNullExpr:
		p.formatIsNullExpr(expr)
	case *core.IsBoolExpr:
		p.formatIsBoolExpr(expr)
	case *core.LikeExpr:
		p.formatLikeExpr(expr)
	case *core.ParenExpr:
		p.formatParenExpr(expr)
	case *core.SubqueryExpr:
		p.formatSubqueryExpr(expr)
	case *core.ExistsExpr:
		p.formatExistsExpr(expr)
	case *core.StarExpr:
		p.formatStarExpr(expr)
	case *core.MacroExpr:
		p.formatMacroExpr(expr)
	// DuckDB expression extensions (Phase 3)
	case *core.LambdaExpr:
		p.formatLambdaExpr(expr)
	case *core.StructLiteral:
		p.formatStructLiteral(expr)
	case *core.ListLiteral:
		p.formatListLiteral(expr)
	case *core.IndexExpr:
		p.formatIndexExpr(expr)
	}
}

func (p *Printer) exprComplexity(e core.Expr) int {
	if e == nil {
		return 0
	}

	switch expr := e.(type) {
	case *core.Literal, *core.ColumnRef, *core.StarExpr, *core.MacroExpr:
		return 1
	case *core.BinaryExpr:
		return 1 + p.exprComplexity(expr.Left) + p.exprComplexity(expr.Right)
	case *core.UnaryExpr:
		return 1 + p.exprComplexity(expr.Expr)
	case *core.FuncCall:
		score := 2
		for _, arg := range expr.Args {
			score += p.exprComplexity(arg)
		}
		return score
	case *core.ParenExpr:
		return p.exprComplexity(expr.Expr)
	case *core.CaseExpr:
		score := 2
		for _, w := range expr.Whens {
			score += p.exprComplexity(w.Condition) + p.exprComplexity(w.Result)
		}
		return score
	case *core.LambdaExpr:
		return 1 + p.exprComplexity(expr.Body)
	case *core.StructLiteral:
		score := 1
		for _, f := range expr.Fields {
			score += p.exprComplexity(f.Value)
		}
		return score
	case *core.ListLiteral:
		score := 1
		for _, elem := range expr.Elements {
			score += p.exprComplexity(elem)
		}
		return score
	case *core.IndexExpr:
		score := 1 + p.exprComplexity(expr.Expr)
		if expr.Index != nil {
			score += p.exprComplexity(expr.Index)
		}
		if expr.Start != nil {
			score += p.exprComplexity(expr.Start)
		}
		if expr.Stop != nil {
			score += p.exprComplexity(expr.Stop)
		}
		return score
	default:
		return 1
	}
}

func isLogicalOp(op token.TokenType) bool {
	return op == token.AND || op == token.OR
}

func (p *Printer) formatLiteral(lit *core.Literal) {
	switch lit.Type {
	case core.LiteralString:
		p.write("'")
		p.write(lit.Value)
		p.write("'")
	case core.LiteralBool:
		if lit.Value == "TRUE" || lit.Value == "true" {
			p.kw(token.TRUE)
		} else {
			p.kw(token.FALSE)
		}
	case core.LiteralNull:
		p.kw(token.NULL)
	default:
		p.write(lit.Value)
	}
}

func (p *Printer) formatColumnRef(col *core.ColumnRef) {
	if col.Table != "" {
		p.write(col.Table)
		p.write(".")
	}
	p.write(col.Column)
}

func (p *Printer) formatBinaryExpr(expr *core.BinaryExpr) {
	shouldBreak := p.exprComplexity(expr) > complexityThreshold && isLogicalOp(expr.Op)

	p.formatExpr(expr.Left)

	if shouldBreak {
		p.writeln()
		p.kw(expr.Op)
		p.space()
	} else {
		p.space()
		p.kw(expr.Op)
		p.space()
	}

	p.formatExpr(expr.Right)
}

func (p *Printer) formatUnaryExpr(expr *core.UnaryExpr) {
	p.kw(expr.Op)
	if expr.Op == token.NOT {
		p.space()
	}
	p.formatExpr(expr.Expr)
}

func (p *Printer) formatFuncCall(fn *core.FuncCall) {
	p.write(fn.Name)
	p.write("(")

	if fn.Distinct {
		p.kw(token.DISTINCT)
		p.space()
	}

	if fn.Star {
		p.write("*")
	} else {
		p.formatList(len(fn.Args), func(i int) { p.formatExpr(fn.Args[i]) }, ", ", false)
	}

	p.write(")")

	// FILTER clause
	if fn.Filter != nil {
		p.space()
		p.kw(token.FILTER)
		p.write(" (")
		p.kw(token.WHERE)
		p.space()
		p.formatExpr(fn.Filter)
		p.write(")")
	}

	// OVER clause (window function)
	if fn.Window != nil {
		p.space()
		p.formatWindowSpec(fn.Window)
	}
}

func (p *Printer) formatWindowSpec(w *core.WindowSpec) {
	p.kw(token.OVER)
	p.write(" (")

	if w.Name != "" {
		p.write(w.Name)
	}

	if len(w.PartitionBy) > 0 {
		p.writeln()
		p.indent()
		p.kw(token.PARTITION)
		p.space()
		p.kw(token.BY)
		p.space()
		p.formatList(len(w.PartitionBy), func(i int) { p.formatExpr(w.PartitionBy[i]) }, ", ", false)
		p.dedent()
	}

	if len(w.OrderBy) > 0 {
		p.writeln()
		p.indent()
		p.kw(token.ORDER)
		p.space()
		p.kw(token.BY)
		p.space()
		p.formatList(len(w.OrderBy), func(i int) { p.formatOrderByItem(w.OrderBy[i]) }, ", ", false)
		p.dedent()
	}

	if w.Frame != nil {
		p.writeln()
		p.indent()
		p.formatFrameSpec(w.Frame)
		p.dedent()
	}

	p.write(")")
}

func (p *Printer) formatFrameSpec(f *core.FrameSpec) {
	p.keyword(string(f.Type))
	p.space()
	p.kw(token.BETWEEN)
	p.space()
	p.formatFrameBound(f.Start)
	p.space()
	p.kw(token.AND)
	p.space()
	p.formatFrameBound(f.End)
}

func (p *Printer) formatFrameBound(b *core.FrameBound) {
	if b == nil {
		return
	}
	switch b.Type {
	case core.FrameUnboundedPreceding:
		p.kw(token.UNBOUNDED)
		p.space()
		p.kw(token.PRECEDING)
	case core.FrameUnboundedFollowing:
		p.kw(token.UNBOUNDED)
		p.space()
		p.kw(token.FOLLOWING)
	case core.FrameCurrentRow:
		p.kw(token.CURRENT)
		p.space()
		p.kw(token.ROW)
	case core.FrameExprPreceding:
		p.formatExpr(b.Offset)
		p.space()
		p.kw(token.PRECEDING)
	case core.FrameExprFollowing:
		p.formatExpr(b.Offset)
		p.space()
		p.kw(token.FOLLOWING)
	}
}

func (p *Printer) formatCaseExpr(c *core.CaseExpr) {
	p.kw(token.CASE)

	if c.Operand != nil {
		p.space()
		p.formatExpr(c.Operand)
	}

	p.writeln()
	p.indent()

	for _, w := range c.Whens {
		p.kw(token.WHEN)
		p.space()
		p.formatExpr(w.Condition)
		p.space()
		p.kw(token.THEN)
		p.space()
		p.formatExpr(w.Result)
		p.writeln()
	}

	if c.Else != nil {
		p.kw(token.ELSE)
		p.space()
		p.formatExpr(c.Else)
		p.writeln()
	}

	p.dedent()
	p.kw(token.END)
}

func (p *Printer) formatCastExpr(c *core.CastExpr) {
	p.kw(token.CAST)
	p.write("(")
	p.formatExpr(c.Expr)
	p.space()
	p.kw(token.AS)
	p.space()
	p.write(c.TypeName)
	p.write(")")
}

func (p *Printer) formatInExpr(in *core.InExpr) {
	p.formatExpr(in.Expr)
	if in.Not {
		p.space()
		p.kw(token.NOT)
	}
	p.space()
	p.kw(token.IN)
	p.write(" (")

	if in.Query != nil {
		p.writeln()
		p.indent()
		p.formatSelectStmt(in.Query)
		p.dedent()
	} else {
		p.formatList(len(in.Values), func(i int) { p.formatExpr(in.Values[i]) }, ", ", false)
	}

	p.write(")")
}

func (p *Printer) formatBetweenExpr(b *core.BetweenExpr) {
	p.formatExpr(b.Expr)
	if b.Not {
		p.space()
		p.kw(token.NOT)
	}
	p.space()
	p.kw(token.BETWEEN)
	p.space()
	p.formatExpr(b.Low)
	p.space()
	p.kw(token.AND)
	p.space()
	p.formatExpr(b.High)
}

func (p *Printer) formatIsNullExpr(is *core.IsNullExpr) {
	p.formatExpr(is.Expr)
	p.space()
	p.kw(token.IS)
	if is.Not {
		p.space()
		p.kw(token.NOT)
	}
	p.space()
	p.kw(token.NULL)
}

func (p *Printer) formatIsBoolExpr(is *core.IsBoolExpr) {
	p.formatExpr(is.Expr)
	p.space()
	p.kw(token.IS)
	if is.Not {
		p.space()
		p.kw(token.NOT)
	}
	p.space()
	if is.Value {
		p.kw(token.TRUE)
	} else {
		p.kw(token.FALSE)
	}
}

func (p *Printer) formatLikeExpr(like *core.LikeExpr) {
	p.formatExpr(like.Expr)
	if like.Not {
		p.space()
		p.kw(token.NOT)
	}
	p.space()
	p.kw(like.Op) // Just print the stored token
	p.space()
	p.formatExpr(like.Pattern)
}

func (p *Printer) formatParenExpr(paren *core.ParenExpr) {
	p.write("(")
	p.formatExpr(paren.Expr)
	p.write(")")
}

func (p *Printer) formatSubqueryExpr(sq *core.SubqueryExpr) {
	p.write("(")
	p.writeln()
	p.indent()
	p.formatSelectStmt(sq.Select)
	p.dedent()
	p.write(")")
}

func (p *Printer) formatExistsExpr(ex *core.ExistsExpr) {
	if ex.Not {
		p.kw(token.NOT)
		p.space()
	}
	p.kw(token.EXISTS)
	p.write(" (")
	p.writeln()
	p.indent()
	p.formatSelectStmt(ex.Select)
	p.dedent()
	p.write(")")
}

func (p *Printer) formatStarExpr(star *core.StarExpr) {
	if star.Table != "" {
		p.write(star.Table)
		p.write(".")
	}
	p.write("*")
}

func (p *Printer) formatMacroExpr(m *core.MacroExpr) {
	// Macros are preserved exactly as written
	p.write(m.Content)
}

// ---------- DuckDB Expression Extensions (Phase 3) ----------

func (p *Printer) formatLambdaExpr(lambda *core.LambdaExpr) {
	if len(lambda.Params) == 1 {
		p.write(lambda.Params[0])
	} else {
		p.write("(")
		for i, param := range lambda.Params {
			if i > 0 {
				p.write(", ")
			}
			p.write(param)
		}
		p.write(")")
	}
	p.write(" -> ")
	p.formatExpr(lambda.Body)
}

func (p *Printer) formatStructLiteral(s *core.StructLiteral) {
	p.write("{")
	for i, field := range s.Fields {
		if i > 0 {
			p.write(", ")
		}
		// In DuckDB struct literals, keys are always single-quoted strings
		p.write("'")
		p.write(field.Key)
		p.write("'")
		p.write(": ")
		p.formatExpr(field.Value)
	}
	p.write("}")
}

func (p *Printer) formatListLiteral(list *core.ListLiteral) {
	p.write("[")
	for i, elem := range list.Elements {
		if i > 0 {
			p.write(", ")
		}
		p.formatExpr(elem)
	}
	p.write("]")
}

func (p *Printer) formatIndexExpr(idx *core.IndexExpr) {
	p.formatExpr(idx.Expr)
	p.write("[")
	if idx.IsSlice {
		if idx.Start != nil {
			p.formatExpr(idx.Start)
		}
		p.write(":")
		if idx.Stop != nil {
			p.formatExpr(idx.Stop)
		}
	} else {
		p.formatExpr(idx.Index)
	}
	p.write("]")
}
