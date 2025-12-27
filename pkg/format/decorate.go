package format

import (
	"github.com/leapstack-labs/leapsql/pkg/core"
	"github.com/leapstack-labs/leapsql/pkg/token"
)

// Decorate attaches comments to AST nodes based on position.
// This enables comment preservation even when clauses are reordered.
func Decorate(stmt *core.SelectStmt, comments []*token.Comment) *core.SelectStmt {
	if len(comments) == 0 {
		return stmt
	}

	d := &decorator{
		comments: comments,
		used:     make([]bool, len(comments)),
	}
	d.decorateStmt(stmt)

	// Attach any remaining unused comments to the statement
	for i, c := range d.comments {
		if !d.used[i] {
			stmt.AddTrailingComment(c)
		}
	}

	return stmt
}

type decorator struct {
	comments []*token.Comment
	used     []bool
}

func (d *decorator) decorateStmt(stmt *core.SelectStmt) {
	if stmt == nil {
		return
	}

	d.attachComments(&stmt.NodeInfo)

	if stmt.With != nil {
		d.decorateWith(stmt.With)
	}

	if stmt.Body != nil {
		d.decorateBody(stmt.Body)
	}
}

func (d *decorator) decorateWith(with *core.WithClause) {
	for _, cte := range with.CTEs {
		d.decorateStmt(cte.Select)
	}
}

func (d *decorator) decorateBody(body *core.SelectBody) {
	if body == nil {
		return
	}

	if body.Left != nil {
		d.decorateCore(body.Left)
	}

	if body.Right != nil {
		d.decorateBody(body.Right)
	}
}

func (d *decorator) decorateCore(core *core.SelectCore) {
	if core == nil {
		return
	}

	d.attachComments(&core.NodeInfo)

	// Decorate expressions in SELECT list
	for i := range core.Columns {
		if core.Columns[i].Expr != nil {
			d.decorateExpr(core.Columns[i].Expr)
		}
	}

	// Decorate FROM clause
	if core.From != nil {
		d.decorateFrom(core.From)
	}

	// Decorate WHERE
	if core.Where != nil {
		d.decorateExpr(core.Where)
	}

	// Decorate GROUP BY
	for _, expr := range core.GroupBy {
		d.decorateExpr(expr)
	}

	// Decorate HAVING
	if core.Having != nil {
		d.decorateExpr(core.Having)
	}

	// Decorate ORDER BY
	for _, item := range core.OrderBy {
		d.decorateExpr(item.Expr)
	}
}

func (d *decorator) decorateFrom(from *core.FromClause) {
	if from == nil {
		return
	}

	d.decorateTableRef(from.Source)

	for _, join := range from.Joins {
		d.decorateTableRef(join.Right)
		if join.Condition != nil {
			d.decorateExpr(join.Condition)
		}
	}
}

func (d *decorator) decorateTableRef(ref core.TableRef) {
	if ref == nil {
		return
	}

	switch t := ref.(type) {
	case *core.DerivedTable:
		d.decorateStmt(t.Select)
	case *core.LateralTable:
		d.decorateStmt(t.Select)
	case *core.MacroTable:
		d.attachComments(&t.NodeInfo)
	}
}

func (d *decorator) decorateExpr(_ core.Expr) {
	// Expressions with NodeInfo can have comments attached
	// For now, we only attach to statement-level nodes
	// Expression-level comment attachment can be added later if needed
}

func (d *decorator) attachComments(node *core.NodeInfo) {
	if node == nil || !node.Span.IsValid() {
		return
	}

	span := node.Span

	for i, c := range d.comments {
		if d.used[i] {
			continue
		}

		// Leading: comment ends before node starts, on previous line
		if c.Span.End.Offset < span.Start.Offset &&
			c.Span.End.Line < span.Start.Line {
			node.AddLeadingComment(c)
			d.used[i] = true
			continue
		}

		// Trailing: comment starts after node ends, on same line
		if c.Span.Start.Offset >= span.End.Offset &&
			c.Span.Start.Line == span.End.Line {
			node.AddTrailingComment(c)
			d.used[i] = true
		}
	}
}
