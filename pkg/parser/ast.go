package parser

import "github.com/leapstack-labs/leapsql/pkg/token"

// Statement represents a SQL statement.
type Statement interface {
	stmtNode()
}

// Expr represents an expression in SQL.
type Expr interface {
	exprNode()
}

// TableRef represents a table reference in FROM clause.
type TableRef interface {
	tableRefNode()
}

// NodeInfo provides common fields for all AST nodes.
// Embed this in node types that need position/comment tracking.
type NodeInfo struct {
	Span             token.Span
	LeadingComments  []*token.Comment
	TrailingComments []*token.Comment
}

// GetSpan returns the node's source span.
func (n *NodeInfo) GetSpan() token.Span {
	return n.Span
}

// AddLeadingComment adds a leading comment to the node.
func (n *NodeInfo) AddLeadingComment(c *token.Comment) {
	n.LeadingComments = append(n.LeadingComments, c)
}

// AddTrailingComment adds a trailing comment to the node.
func (n *NodeInfo) AddTrailingComment(c *token.Comment) {
	n.TrailingComments = append(n.TrailingComments, c)
}

// ---------- Statement Types ----------

// SelectStmt represents a complete SELECT statement with optional WITH clause.
type SelectStmt struct {
	NodeInfo
	With *WithClause
	Body *SelectBody
}

func (*SelectStmt) stmtNode() {}

// WithClause represents a WITH clause with CTEs.
type WithClause struct {
	NodeInfo
	Recursive bool
	CTEs      []*CTE
}

// CTE represents a Common Table Expression.
type CTE struct {
	NodeInfo
	Name   string
	Select *SelectStmt
}

// SelectBody represents the body of a SELECT with possible set operations.
type SelectBody struct {
	NodeInfo
	Left  *SelectCore
	Op    SetOpType   // UNION, INTERSECT, EXCEPT, or empty
	All   bool        // UNION ALL
	Right *SelectBody // For chained set operations
}

// SetOpType represents the type of set operation.
type SetOpType string

// SetOpType constants for set operations in queries.
const (
	SetOpNone      SetOpType = ""
	SetOpUnion     SetOpType = "UNION"
	SetOpUnionAll  SetOpType = "UNION ALL"
	SetOpIntersect SetOpType = "INTERSECT"
	SetOpExcept    SetOpType = "EXCEPT"
)

// SelectCore represents the core SELECT clause.
type SelectCore struct {
	NodeInfo
	Distinct bool
	Columns  []SelectItem
	From     *FromClause
	Where    Expr
	GroupBy  []Expr
	Having   Expr
	Windows  []WindowDef // Named window definitions (WINDOW clause)
	Qualify  Expr        // DuckDB/Snowflake window function filter
	OrderBy  []OrderByItem
	Limit    Expr
	Offset   Expr
	Fetch    *FetchClause // FETCH FIRST/NEXT support (SQL:2008)

	// Extensions holds rare/custom dialect-specific nodes (e.g., CONNECT BY, SAMPLE).
	// Use this for dialect features that are too specialized to warrant typed fields.
	Extensions []Node
}

// Node is a generic AST node interface for extensions.
type Node interface {
	node()
}

// FetchClause represents FETCH FIRST/NEXT n ROWS ONLY/WITH TIES (SQL:2008).
type FetchClause struct {
	First    bool // true = FIRST, false = NEXT (semantically identical)
	Count    Expr // Number of rows (nil = 1 row implied)
	Percent  bool // FETCH FIRST n PERCENT ROWS
	WithTies bool // true = WITH TIES, false = ONLY
}

func (*FetchClause) node() {}

// WindowDef represents a named window definition in the WINDOW clause.
// Example: WINDOW w AS (PARTITION BY x ORDER BY y)
type WindowDef struct {
	Name string
	Spec *WindowSpec
}

func (WindowDef) node() {}

// SelectItem represents an item in the SELECT list.
type SelectItem struct {
	Star      bool   // SELECT *
	TableStar string // SELECT t.*
	Expr      Expr   // Expression
	Alias     string // AS alias
}

// FromClause represents the FROM clause.
type FromClause struct {
	NodeInfo
	Source TableRef
	Joins  []*Join
}

// Join represents a JOIN clause.
type Join struct {
	NodeInfo
	Type      JoinType
	Natural   bool // NATURAL JOIN modifier
	Right     TableRef
	Condition Expr     // ON clause (mutually exclusive with Using)
	Using     []string // USING (col1, col2) columns
}

// JoinType represents the type of join.
// The value is the SQL keyword (e.g., "LEFT", "INNER", "SEMI").
// Join type constants are defined in their respective dialect packages:
//   - ANSI joins (INNER, LEFT, RIGHT, FULL, CROSS): pkg/dialects/ansi/join_types.go
//   - DuckDB joins (SEMI, ANTI, ASOF, POSITIONAL): pkg/adapters/duckdb/dialect/join_types.go
type JoinType string

// JoinComma represents an implicit cross join using comma syntax.
// This is kept in the parser package because it's syntactically unique
// (not a TYPE JOIN keyword pattern) and universal across all SQL dialects.
const JoinComma JoinType = ","

// OrderByItem represents an item in ORDER BY clause.
type OrderByItem struct {
	Expr       Expr
	Desc       bool
	NullsFirst *bool // nil means default, true = NULLS FIRST, false = NULLS LAST
}

// ---------- Table Reference Types ----------

// TableName represents a table name reference.
type TableName struct {
	NodeInfo
	Catalog string
	Schema  string
	Name    string
	Alias   string
}

func (*TableName) tableRefNode() {}

// DerivedTable represents a subquery in FROM clause.
type DerivedTable struct {
	NodeInfo
	Select *SelectStmt
	Alias  string
}

func (*DerivedTable) tableRefNode() {}

// LateralTable represents a LATERAL subquery.
type LateralTable struct {
	NodeInfo
	Select *SelectStmt
	Alias  string
}

func (*LateralTable) tableRefNode() {}

// MacroTable represents a macro used as a table reference (e.g., {{ ref('table') }}).
type MacroTable struct {
	NodeInfo
	Content string // raw {{ ... }} content including delimiters
	Alias   string
}

func (*MacroTable) tableRefNode() {}

// ---------- Expression Types ----------

// ColumnRef represents a column reference (possibly qualified).
type ColumnRef struct {
	Table  string // optional table/alias qualifier
	Column string
}

func (*ColumnRef) exprNode() {}

// Literal represents a literal value.
type Literal struct {
	Type  LiteralType
	Value string
}

func (*Literal) exprNode() {}

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

// UnaryExpr represents a unary expression.
type UnaryExpr struct {
	Op   token.TokenType
	Expr Expr
}

func (*UnaryExpr) exprNode() {}

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

// InExpr represents an IN expression.
type InExpr struct {
	Expr   Expr
	Not    bool
	Values []Expr      // IN (1, 2, 3)
	Query  *SelectStmt // IN (SELECT ...)
}

func (*InExpr) exprNode() {}

// BetweenExpr represents a BETWEEN expression.
type BetweenExpr struct {
	Expr Expr
	Not  bool
	Low  Expr
	High Expr
}

func (*BetweenExpr) exprNode() {}

// IsNullExpr represents an IS NULL expression.
type IsNullExpr struct {
	Expr Expr
	Not  bool
}

func (*IsNullExpr) exprNode() {}

// IsBoolExpr represents an IS [NOT] TRUE/FALSE expression.
type IsBoolExpr struct {
	Expr  Expr
	Not   bool
	Value bool // true for IS TRUE, false for IS FALSE
}

func (*IsBoolExpr) exprNode() {}

// LikeExpr represents a LIKE expression.
type LikeExpr struct {
	Expr    Expr
	Not     bool
	Pattern Expr
	Op      token.TokenType // token.LIKE or dialect-registered ILIKE
}

func (*LikeExpr) exprNode() {}

// ParenExpr represents a parenthesized expression.
type ParenExpr struct {
	Expr Expr
}

func (*ParenExpr) exprNode() {}

// StarExpr represents a * expression (for SELECT *).
type StarExpr struct {
	Table string // optional table qualifier for t.*
}

func (*StarExpr) exprNode() {}

// SubqueryExpr represents a subquery used as an expression (e.g., in EXISTS).
type SubqueryExpr struct {
	Select *SelectStmt
}

func (*SubqueryExpr) exprNode() {}

// ExistsExpr represents an EXISTS expression.
type ExistsExpr struct {
	Not    bool
	Select *SelectStmt
}

func (*ExistsExpr) exprNode() {}

// MacroExpr represents a template macro expression (e.g., {{ ref('table') }}).
type MacroExpr struct {
	NodeInfo
	Content string // raw {{ ... }} content including delimiters
}

func (*MacroExpr) exprNode() {}
