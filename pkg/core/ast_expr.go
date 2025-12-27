package core

import "github.com/leapstack-labs/leapsql/pkg/token"

// ---------- Expression Types ----------

// ColumnRef represents a column reference (possibly qualified).
type ColumnRef struct {
	Table  string // optional table/alias qualifier
	Column string
}

func (*ColumnRef) exprNode() {}

// Pos implements Node.
func (c *ColumnRef) Pos() token.Position { return token.Position{} }

// End implements Node.
func (c *ColumnRef) End() token.Position { return token.Position{} }

// GetTable returns the table qualifier.
func (c *ColumnRef) GetTable() string { return c.Table }

// GetColumn returns the column name.
func (c *ColumnRef) GetColumn() string { return c.Column }

// Literal represents a literal value.
type Literal struct {
	Type  LiteralType
	Value string
}

func (*Literal) exprNode() {}

// Pos implements Node.
func (l *Literal) Pos() token.Position { return token.Position{} }

// End implements Node.
func (l *Literal) End() token.Position { return token.Position{} }

// LiteralType represents the type of a literal.
type LiteralType int

// LiteralType constants for SQL literal value types.
const (
	LiteralNumber LiteralType = iota
	LiteralString
	LiteralBool
	LiteralNull
)

// BinaryExpr represents a binary expression.
type BinaryExpr struct {
	Left  Expr
	Op    token.TokenType
	Right Expr
}

func (*BinaryExpr) exprNode() {}

// Pos implements Node.
func (b *BinaryExpr) Pos() token.Position {
	if b.Left != nil {
		return b.Left.Pos()
	}
	return token.Position{}
}

// End implements Node.
func (b *BinaryExpr) End() token.Position {
	if b.Right != nil {
		return b.Right.End()
	}
	return token.Position{}
}

// GetLeft returns the left operand.
func (b *BinaryExpr) GetLeft() Expr { return b.Left }

// GetRight returns the right operand.
func (b *BinaryExpr) GetRight() Expr { return b.Right }

// GetOp returns the operator.
func (b *BinaryExpr) GetOp() token.TokenType { return b.Op }

// UnaryExpr represents a unary expression.
type UnaryExpr struct {
	Op   token.TokenType
	Expr Expr
}

func (*UnaryExpr) exprNode() {}

// Pos implements Node.
func (u *UnaryExpr) Pos() token.Position { return token.Position{} }

// End implements Node.
func (u *UnaryExpr) End() token.Position {
	if u.Expr != nil {
		return u.Expr.End()
	}
	return token.Position{}
}

// FuncCall represents a function call.
type FuncCall struct {
	Name     string
	Distinct bool
	Args     []Expr
	Star     bool        // COUNT(*)
	Window   *WindowSpec // OVER clause
	Filter   Expr        // FILTER (WHERE ...) clause
}

func (*FuncCall) exprNode() {}

// Pos implements Node.
func (f *FuncCall) Pos() token.Position { return token.Position{} }

// End implements Node.
func (f *FuncCall) End() token.Position { return token.Position{} }

// WindowSpec represents a window specification (OVER clause).
type WindowSpec struct {
	Name        string // Named window reference
	PartitionBy []Expr
	OrderBy     []OrderByItem
	Frame       *FrameSpec
}

// FrameSpec represents a window frame specification.
type FrameSpec struct {
	Type  FrameType
	Start *FrameBound
	End   *FrameBound
}

// FrameType represents the type of window frame.
type FrameType string

// FrameType constants for window frame specification types.
const (
	FrameRows   FrameType = "ROWS"
	FrameRange  FrameType = "RANGE"
	FrameGroups FrameType = "GROUPS"
)

// FrameBound represents a window frame bound.
type FrameBound struct {
	Type   FrameBoundType
	Offset Expr // for N PRECEDING/FOLLOWING
}

// FrameBoundType represents the type of frame bound.
type FrameBoundType string

// FrameBoundType constants for window frame bound types.
const (
	FrameUnboundedPreceding FrameBoundType = "UNBOUNDED PRECEDING"
	FrameUnboundedFollowing FrameBoundType = "UNBOUNDED FOLLOWING"
	FrameCurrentRow         FrameBoundType = "CURRENT ROW"
	FrameExprPreceding      FrameBoundType = "EXPR PRECEDING"
	FrameExprFollowing      FrameBoundType = "EXPR FOLLOWING"
)

// CaseExpr represents a CASE expression.
type CaseExpr struct {
	Operand Expr // CASE operand WHEN... (optional)
	Whens   []WhenClause
	Else    Expr
}

func (*CaseExpr) exprNode() {}

// Pos implements Node.
func (c *CaseExpr) Pos() token.Position { return token.Position{} }

// End implements Node.
func (c *CaseExpr) End() token.Position { return token.Position{} }

// WhenClause represents a WHEN clause in CASE expression.
type WhenClause struct {
	Condition Expr
	Result    Expr
}

// CastExpr represents a CAST expression.
type CastExpr struct {
	Expr     Expr
	TypeName string
}

func (*CastExpr) exprNode() {}

// Pos implements Node.
func (c *CastExpr) Pos() token.Position { return token.Position{} }

// End implements Node.
func (c *CastExpr) End() token.Position { return token.Position{} }

// InExpr represents an IN expression.
type InExpr struct {
	Expr   Expr
	Not    bool
	Values []Expr      // IN (1, 2, 3)
	Query  *SelectStmt // IN (SELECT ...)
}

func (*InExpr) exprNode() {}

// Pos implements Node.
func (i *InExpr) Pos() token.Position { return token.Position{} }

// End implements Node.
func (i *InExpr) End() token.Position { return token.Position{} }

// BetweenExpr represents a BETWEEN expression.
type BetweenExpr struct {
	Expr Expr
	Not  bool
	Low  Expr
	High Expr
}

func (*BetweenExpr) exprNode() {}

// Pos implements Node.
func (b *BetweenExpr) Pos() token.Position { return token.Position{} }

// End implements Node.
func (b *BetweenExpr) End() token.Position { return token.Position{} }

// IsNullExpr represents an IS NULL expression.
type IsNullExpr struct {
	Expr Expr
	Not  bool
}

func (*IsNullExpr) exprNode() {}

// Pos implements Node.
func (i *IsNullExpr) Pos() token.Position { return token.Position{} }

// End implements Node.
func (i *IsNullExpr) End() token.Position { return token.Position{} }

// IsBoolExpr represents an IS [NOT] TRUE/FALSE expression.
type IsBoolExpr struct {
	Expr  Expr
	Not   bool
	Value bool // true for IS TRUE, false for IS FALSE
}

func (*IsBoolExpr) exprNode() {}

// Pos implements Node.
func (i *IsBoolExpr) Pos() token.Position { return token.Position{} }

// End implements Node.
func (i *IsBoolExpr) End() token.Position { return token.Position{} }

// LikeExpr represents a LIKE expression.
type LikeExpr struct {
	Expr    Expr
	Not     bool
	Pattern Expr
	Op      token.TokenType // token.LIKE or dialect-registered ILIKE
}

func (*LikeExpr) exprNode() {}

// Pos implements Node.
func (l *LikeExpr) Pos() token.Position { return token.Position{} }

// End implements Node.
func (l *LikeExpr) End() token.Position { return token.Position{} }

// ParenExpr represents a parenthesized expression.
type ParenExpr struct {
	Expr Expr
}

func (*ParenExpr) exprNode() {}

// Pos implements Node.
func (p *ParenExpr) Pos() token.Position {
	if p.Expr != nil {
		return p.Expr.Pos()
	}
	return token.Position{}
}

// End implements Node.
func (p *ParenExpr) End() token.Position {
	if p.Expr != nil {
		return p.Expr.End()
	}
	return token.Position{}
}

// GetExpr returns the inner expression.
func (p *ParenExpr) GetExpr() Expr { return p.Expr }

// StarExpr represents a * expression (for SELECT *).
type StarExpr struct {
	Table string // optional table qualifier for t.*
}

func (*StarExpr) exprNode() {}

// Pos implements Node.
func (s *StarExpr) Pos() token.Position { return token.Position{} }

// End implements Node.
func (s *StarExpr) End() token.Position { return token.Position{} }

// SubqueryExpr represents a subquery used as an expression (e.g., in EXISTS).
type SubqueryExpr struct {
	Select *SelectStmt
}

func (*SubqueryExpr) exprNode() {}

// Pos implements Node.
func (s *SubqueryExpr) Pos() token.Position { return token.Position{} }

// End implements Node.
func (s *SubqueryExpr) End() token.Position { return token.Position{} }

// ExistsExpr represents an EXISTS expression.
type ExistsExpr struct {
	Not    bool
	Select *SelectStmt
}

func (*ExistsExpr) exprNode() {}

// Pos implements Node.
func (e *ExistsExpr) Pos() token.Position { return token.Position{} }

// End implements Node.
func (e *ExistsExpr) End() token.Position { return token.Position{} }

// MacroExpr represents a template macro expression (e.g., {{ ref('table') }}).
type MacroExpr struct {
	NodeInfo
	Content string // raw {{ ... }} content including delimiters
}

func (*MacroExpr) exprNode() {}

// ---------- DuckDB Expression Extensions ----------

// LambdaExpr represents a lambda expression: x -> expr or (x, y) -> expr.
// Used with list functions like list_transform, list_filter, list_reduce.
type LambdaExpr struct {
	NodeInfo
	Params []string // Parameter names
	Body   Expr     // Lambda body expression
}

func (*LambdaExpr) exprNode() {}

// GetParams returns the parameter names.
func (l *LambdaExpr) GetParams() []string { return l.Params }

// GetBody returns the lambda body.
func (l *LambdaExpr) GetBody() Expr { return l.Body }

// StructLiteral represents a struct literal: {'key': value, ...}.
// DuckDB syntax for creating anonymous structs/records.
type StructLiteral struct {
	NodeInfo
	Fields []StructField
}

func (*StructLiteral) exprNode() {}

// StructField represents a field in a struct literal.
type StructField struct {
	Key   string // Field name (can be identifier or string)
	Value Expr   // Field value
}

// ListLiteral represents a list/array literal: [expr, expr, ...].
// DuckDB syntax for creating array values.
type ListLiteral struct {
	NodeInfo
	Elements []Expr // Expression elements
}

func (*ListLiteral) exprNode() {}

// IndexExpr represents array indexing or slicing: arr[i] or arr[start:stop].
// Supports DuckDB array subscript and slice syntax.
type IndexExpr struct {
	NodeInfo
	Expr  Expr // The expression being indexed
	Index Expr // Simple index (non-nil if not a slice)
	// For slicing (arr[start:stop])
	IsSlice bool
	Start   Expr // nil means from beginning
	Stop    Expr // nil means to end (named Stop to avoid shadowing End() method)
}

func (*IndexExpr) exprNode() {}
