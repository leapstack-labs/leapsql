package format

import (
	"github.com/leapstack-labs/leapsql/pkg/parser"
	"github.com/leapstack-labs/leapsql/pkg/token"
)

const complexityThreshold = 5

func (p *Printer) formatExpr(e parser.Expr) {
	if e == nil {
		return
	}

	switch expr := e.(type) {
	case *parser.Literal:
		p.formatLiteral(expr)
	case *parser.ColumnRef:
		p.formatColumnRef(expr)
	case *parser.BinaryExpr:
		p.formatBinaryExpr(expr)
	case *parser.UnaryExpr:
		p.formatUnaryExpr(expr)
	case *parser.FuncCall:
		p.formatFuncCall(expr)
	case *parser.CaseExpr:
		p.formatCaseExpr(expr)
	case *parser.CastExpr:
		p.formatCastExpr(expr)
	case *parser.InExpr:
		p.formatInExpr(expr)
	case *parser.BetweenExpr:
		p.formatBetweenExpr(expr)
	case *parser.IsNullExpr:
		p.formatIsNullExpr(expr)
	case *parser.IsBoolExpr:
		p.formatIsBoolExpr(expr)
	case *parser.LikeExpr:
		p.formatLikeExpr(expr)
	case *parser.ParenExpr:
		p.formatParenExpr(expr)
	case *parser.SubqueryExpr:
		p.formatSubqueryExpr(expr)
	case *parser.ExistsExpr:
		p.formatExistsExpr(expr)
	case *parser.StarExpr:
		p.formatStarExpr(expr)
	case *parser.MacroExpr:
		p.formatMacroExpr(expr)
	// DuckDB expression extensions (Phase 3)
	case *parser.LambdaExpr:
		p.formatLambdaExpr(expr)
	case *parser.StructLiteral:
		p.formatStructLiteral(expr)
	case *parser.ListLiteral:
		p.formatListLiteral(expr)
	case *parser.IndexExpr:
		p.formatIndexExpr(expr)
	}
}

func (p *Printer) exprComplexity(e parser.Expr) int {
	if e == nil {
		return 0
	}

	switch expr := e.(type) {
	case *parser.Literal, *parser.ColumnRef, *parser.StarExpr, *parser.MacroExpr:
		return 1
	case *parser.BinaryExpr:
		return 1 + p.exprComplexity(expr.Left) + p.exprComplexity(expr.Right)
	case *parser.UnaryExpr:
		return 1 + p.exprComplexity(expr.Expr)
	case *parser.FuncCall:
		score := 2
		for _, arg := range expr.Args {
			score += p.exprComplexity(arg)
		}
		return score
	case *parser.ParenExpr:
		return p.exprComplexity(expr.Expr)
	case *parser.CaseExpr:
		score := 2
		for _, w := range expr.Whens {
			score += p.exprComplexity(w.Condition) + p.exprComplexity(w.Result)
		}
		return score
	case *parser.LambdaExpr:
		return 1 + p.exprComplexity(expr.Body)
	case *parser.StructLiteral:
		score := 1
		for _, f := range expr.Fields {
			score += p.exprComplexity(f.Value)
		}
		return score
	case *parser.ListLiteral:
		score := 1
		for _, elem := range expr.Elements {
			score += p.exprComplexity(elem)
		}
		return score
	case *parser.IndexExpr:
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

func (p *Printer) formatLiteral(lit *parser.Literal) {
	switch lit.Type {
	case parser.LiteralString:
		p.write("'")
		p.write(lit.Value)
		p.write("'")
	case parser.LiteralBool:
		if lit.Value == "TRUE" || lit.Value == "true" {
			p.kw(token.TRUE)
		} else {
			p.kw(token.FALSE)
		}
	case parser.LiteralNull:
		p.kw(token.NULL)
	default:
		p.write(lit.Value)
	}
}

func (p *Printer) formatColumnRef(col *parser.ColumnRef) {
	if col.Table != "" {
		p.write(col.Table)
		p.write(".")
	}
	p.write(col.Column)
}

func (p *Printer) formatBinaryExpr(expr *parser.BinaryExpr) {
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

func (p *Printer) formatUnaryExpr(expr *parser.UnaryExpr) {
	p.kw(expr.Op)
	if expr.Op == token.NOT {
		p.space()
	}
	p.formatExpr(expr.Expr)
}

func (p *Printer) formatFuncCall(fn *parser.FuncCall) {
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

func (p *Printer) formatWindowSpec(w *parser.WindowSpec) {
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

func (p *Printer) formatFrameSpec(f *parser.FrameSpec) {
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

func (p *Printer) formatFrameBound(b *parser.FrameBound) {
	if b == nil {
		return
	}
	switch b.Type {
	case parser.FrameUnboundedPreceding:
		p.kw(token.UNBOUNDED)
		p.space()
		p.kw(token.PRECEDING)
	case parser.FrameUnboundedFollowing:
		p.kw(token.UNBOUNDED)
		p.space()
		p.kw(token.FOLLOWING)
	case parser.FrameCurrentRow:
		p.kw(token.CURRENT)
		p.space()
		p.kw(token.ROW)
	case parser.FrameExprPreceding:
		p.formatExpr(b.Offset)
		p.space()
		p.kw(token.PRECEDING)
	case parser.FrameExprFollowing:
		p.formatExpr(b.Offset)
		p.space()
		p.kw(token.FOLLOWING)
	}
}

func (p *Printer) formatCaseExpr(c *parser.CaseExpr) {
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

func (p *Printer) formatCastExpr(c *parser.CastExpr) {
	p.kw(token.CAST)
	p.write("(")
	p.formatExpr(c.Expr)
	p.space()
	p.kw(token.AS)
	p.space()
	p.write(c.TypeName)
	p.write(")")
}

func (p *Printer) formatInExpr(in *parser.InExpr) {
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

func (p *Printer) formatBetweenExpr(b *parser.BetweenExpr) {
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

func (p *Printer) formatIsNullExpr(is *parser.IsNullExpr) {
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

func (p *Printer) formatIsBoolExpr(is *parser.IsBoolExpr) {
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

func (p *Printer) formatLikeExpr(like *parser.LikeExpr) {
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

func (p *Printer) formatParenExpr(paren *parser.ParenExpr) {
	p.write("(")
	p.formatExpr(paren.Expr)
	p.write(")")
}

func (p *Printer) formatSubqueryExpr(sq *parser.SubqueryExpr) {
	p.write("(")
	p.writeln()
	p.indent()
	p.formatSelectStmt(sq.Select)
	p.dedent()
	p.write(")")
}

func (p *Printer) formatExistsExpr(ex *parser.ExistsExpr) {
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

func (p *Printer) formatStarExpr(star *parser.StarExpr) {
	if star.Table != "" {
		p.write(star.Table)
		p.write(".")
	}
	p.write("*")
}

func (p *Printer) formatMacroExpr(m *parser.MacroExpr) {
	// Macros are preserved exactly as written
	p.write(m.Content)
}

// ---------- DuckDB Expression Extensions (Phase 3) ----------

func (p *Printer) formatLambdaExpr(lambda *parser.LambdaExpr) {
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

func (p *Printer) formatStructLiteral(s *parser.StructLiteral) {
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

func (p *Printer) formatListLiteral(list *parser.ListLiteral) {
	p.write("[")
	for i, elem := range list.Elements {
		if i > 0 {
			p.write(", ")
		}
		p.formatExpr(elem)
	}
	p.write("]")
}

func (p *Printer) formatIndexExpr(idx *parser.IndexExpr) {
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
