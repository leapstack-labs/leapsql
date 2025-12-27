package core

import "github.com/leapstack-labs/leapsql/pkg/token"

// ---------- Statement Types ----------

// SelectStmt represents a complete SELECT statement with optional WITH clause.
type SelectStmt struct {
	NodeInfo
	With *WithClause
	Body *SelectBody
}

func (*SelectStmt) stmtNode() {}

// Pos implements Node.
func (s *SelectStmt) Pos() token.Position { return s.NodeInfo.Pos() }

// End implements Node.
func (s *SelectStmt) End() token.Position { return s.NodeInfo.End() }

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
	Left   *SelectCore
	Op     SetOpType   // UNION, INTERSECT, EXCEPT, or empty
	All    bool        // UNION ALL
	ByName bool        // DuckDB: BY NAME (match columns by name, not position)
	Right  *SelectBody // For chained set operations
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
	Distinct       bool
	Columns        []SelectItem
	From           *FromClause
	Where          Expr
	GroupBy        []Expr
	GroupByAll     bool // DuckDB: GROUP BY ALL (auto-group by non-aggregate columns)
	Having         Expr
	Windows        []WindowDef // Named window definitions (WINDOW clause)
	Qualify        Expr        // DuckDB/Snowflake window function filter
	OrderBy        []OrderByItem
	OrderByAll     bool // DuckDB: ORDER BY ALL (order by all select columns)
	OrderByAllDesc bool // DuckDB: Direction for ORDER BY ALL (true = DESC)
	Limit          Expr
	Offset         Expr
	Fetch          *FetchClause // FETCH FIRST/NEXT support (SQL:2008)

	// Extensions holds rare/custom dialect-specific nodes (e.g., CONNECT BY, SAMPLE).
	// Use this for dialect features that are too specialized to warrant typed fields.
	Extensions []Node
}

// FetchClause represents FETCH FIRST/NEXT n ROWS ONLY/WITH TIES (SQL:2008).
type FetchClause struct {
	First    bool // true = FIRST, false = NEXT (semantically identical)
	Count    Expr // Number of rows (nil = 1 row implied)
	Percent  bool // FETCH FIRST n PERCENT ROWS
	WithTies bool // true = WITH TIES, false = ONLY
}

// WindowDef represents a named window definition in the WINDOW clause.
// Example: WINDOW w AS (PARTITION BY x ORDER BY y)
type WindowDef struct {
	Name string
	Spec *WindowSpec
}

// SelectItem represents an item in the SELECT list.
type SelectItem struct {
	Star      bool           // SELECT *
	TableStar string         // SELECT t.*
	Expr      Expr           // Expression
	Alias     string         // AS alias
	Modifiers []StarModifier // DuckDB: EXCLUDE, REPLACE, RENAME modifiers
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
//   - Standard joins (INNER, LEFT, RIGHT, FULL, CROSS): pkg/dialect/joins.go
//   - DuckDB joins (SEMI, ANTI, ASOF, POSITIONAL): pkg/dialects/duckdb/join_types.go
type JoinType string

// JoinComma represents an implicit cross join using comma syntax.
// This is kept in the core package because it's syntactically unique
// (not a TYPE JOIN keyword pattern) and universal across all SQL dialects.
const JoinComma JoinType = ","

// OrderByItem represents an item in ORDER BY clause.
type OrderByItem struct {
	Expr       Expr
	Desc       bool
	NullsFirst *bool // nil means default, true = NULLS FIRST, false = NULLS LAST
}
