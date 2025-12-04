# SQL Lineage Parser Specification

This is the definitive specification for the lineage parser. All implementation must conform to this document.

## Scope

| Attribute | Value |
|-----------|-------|
| **Target Dialect** | DuckDB only (ANSI SQL compliant) |
| **Input** | SQL SELECT/WITH statements |
| **Output** | Column-level lineage graph |
| **Language** | Go |
| **Package** | `pkg/lineage` |

## Constraints (Non-Negotiable)

| # | Constraint | Error Message |
|---|------------|---------------|
| 1 | Models must start with `SELECT` or `WITH` | `"statement must begin with SELECT or WITH"` |
| 2 | No scalar subqueries in SELECT columns | `"scalar subqueries in SELECT columns are not supported; rewrite using a CTE and JOIN"` |
| 3 | Recursive CTEs | Allowed |
| 4 | Derived tables in FROM | Allowed |
| 5 | UNION/INTERSECT/EXCEPT | Allowed |
| 6 | Subqueries in WHERE/HAVING (for IN) | Allowed |

## DuckDB Lexer Configuration

| Property | Value |
|----------|-------|
| Identifier quote | `"` (double quote) |
| Identifier quote end | `"` (same) |
| String quote | `'` (single quote) |
| String escape | `''` (doubled single quote) |
| Line comment | `--` |
| Block comment | `/* */` |
| `\|\|` operator | String concatenation |
| Case sensitivity | Case-insensitive (normalize to lowercase) |

## Token Types

```go
const (
    // Special
    TOKEN_EOF
    TOKEN_ILLEGAL

    // Literals
    TOKEN_IDENT   // identifier
    TOKEN_NUMBER  // 123, 45.67, 1e10
    TOKEN_STRING  // 'hello'

    // Operators
    TOKEN_PLUS    // +
    TOKEN_MINUS   // -
    TOKEN_STAR    // *
    TOKEN_SLASH   // /
    TOKEN_PERCENT // %
    TOKEN_DPIPE   // ||
    TOKEN_EQ      // =
    TOKEN_NE      // != or <>
    TOKEN_LT      // <
    TOKEN_GT      // >
    TOKEN_LE      // <=
    TOKEN_GE      // >=
    TOKEN_DOT     // .
    TOKEN_COMMA   // ,
    TOKEN_LPAREN  // (
    TOKEN_RPAREN  // )
    TOKEN_LBRACKET // [
    TOKEN_RBRACKET // ]

    // Keywords
    TOKEN_SELECT
    TOKEN_FROM
    TOKEN_WHERE
    TOKEN_JOIN
    TOKEN_ON
    TOKEN_AS
    TOKEN_AND
    TOKEN_OR
    TOKEN_NOT
    TOKEN_NULL
    TOKEN_TRUE
    TOKEN_FALSE
    TOKEN_CASE
    TOKEN_WHEN
    TOKEN_THEN
    TOKEN_ELSE
    TOKEN_END
    TOKEN_CAST
    TOKEN_DISTINCT
    TOKEN_ALL
    TOKEN_WITH
    TOKEN_RECURSIVE
    TOKEN_UNION
    TOKEN_INTERSECT
    TOKEN_EXCEPT
    TOKEN_LEFT
    TOKEN_RIGHT
    TOKEN_FULL
    TOKEN_INNER
    TOKEN_OUTER
    TOKEN_CROSS
    TOKEN_LATERAL
    TOKEN_OVER
    TOKEN_PARTITION
    TOKEN_ORDER
    TOKEN_BY
    TOKEN_ASC
    TOKEN_DESC
    TOKEN_NULLS
    TOKEN_FIRST
    TOKEN_LAST
    TOKEN_ROWS
    TOKEN_RANGE
    TOKEN_GROUPS
    TOKEN_UNBOUNDED
    TOKEN_PRECEDING
    TOKEN_FOLLOWING
    TOKEN_CURRENT
    TOKEN_ROW
    TOKEN_BETWEEN
    TOKEN_IN
    TOKEN_LIKE
    TOKEN_ILIKE
    TOKEN_IS
    TOKEN_GROUP
    TOKEN_HAVING
    TOKEN_QUALIFY
    TOKEN_LIMIT
    TOKEN_OFFSET
    TOKEN_FILTER
    TOKEN_WITHIN
)
```

## Grammar (EBNF)

### Statements

```ebnf
Statement       = [ WithClause ] SelectBody .

WithClause      = "WITH" [ "RECURSIVE" ] CTE { "," CTE } .
CTE             = identifier "AS" "(" SelectBody ")" .

SelectBody      = SelectCore { SetOp SelectCore } .
SetOp           = "UNION" [ "ALL" ] | "INTERSECT" | "EXCEPT" .

SelectCore      = "SELECT" [ "DISTINCT" | "ALL" ] SelectList
                  "FROM" FromClause
                  [ "WHERE" Expression ]
                  [ "GROUP" "BY" ExpressionList ]
                  [ "HAVING" Expression ]
                  [ "QUALIFY" Expression ]
                  [ "ORDER" "BY" OrderByList ]
                  [ "LIMIT" Expression [ "OFFSET" Expression ] ] .

SelectList      = SelectItem { "," SelectItem } .
SelectItem      = "*"
                | identifier "." "*"
                | Expression [ [ "AS" ] identifier ] .

FromClause      = TableRef { JoinClause } .
JoinClause      = [ JoinType ] "JOIN" TableRef "ON" Expression
                | "CROSS" "JOIN" TableRef
                | "," TableRef .
JoinType        = "LEFT" [ "OUTER" ]
                | "RIGHT" [ "OUTER" ]
                | "FULL" [ "OUTER" ]
                | "INNER" .

TableRef        = TableName [ [ "AS" ] identifier ]
                | "(" SelectBody ")" [ "AS" ] identifier
                | "LATERAL" "(" SelectBody ")" [ "AS" ] identifier .
TableName       = [ identifier "." [ identifier "." ] ] identifier .
```

### Expressions

```ebnf
Expression      = OrExpr .
OrExpr          = AndExpr { "OR" AndExpr } .
AndExpr         = NotExpr { "AND" NotExpr } .
NotExpr         = [ "NOT" ] Comparison .

Comparison      = Addition [ CompOp Addition ]
                | Addition [ "NOT" ] "IN" "(" ExpressionList ")"
                | Addition [ "NOT" ] "IN" "(" SelectBody ")"
                | Addition [ "NOT" ] "BETWEEN" Addition "AND" Addition
                | Addition "IS" [ "NOT" ] "NULL"
                | Addition [ "NOT" ] ( "LIKE" | "ILIKE" ) Addition .
CompOp          = "=" | "!=" | "<>" | "<" | ">" | "<=" | ">=" .

Addition        = Multiplication { ( "+" | "-" | "||" ) Multiplication } .
Multiplication  = Unary { ( "*" | "/" | "%" ) Unary } .
Unary           = [ "-" | "+" ] Primary .

Primary         = Literal
                | ColumnRef
                | FunctionCall
                | CaseExpr
                | CastExpr
                | "(" Expression ")"
                | ArrayAccess .

Literal         = NUMBER | STRING | "TRUE" | "FALSE" | "NULL" .

ColumnRef       = [ identifier "." [ identifier "." ] ] identifier .

FunctionCall    = identifier "(" [ "DISTINCT" ] [ ExpressionList | "*" ] ")"
                  [ "FILTER" "(" "WHERE" Expression ")" ]
                  [ WindowSpec ] .

WindowSpec      = "OVER" "(" [ PartitionBy ] [ OrderBy ] [ FrameSpec ] ")"
                | "OVER" identifier .
PartitionBy     = "PARTITION" "BY" ExpressionList .
OrderBy         = "ORDER" "BY" OrderByList .
OrderByList     = OrderByItem { "," OrderByItem } .
OrderByItem     = Expression [ "ASC" | "DESC" ] [ "NULLS" ( "FIRST" | "LAST" ) ] .
FrameSpec       = ( "ROWS" | "RANGE" | "GROUPS" ) FrameBound .
FrameBound      = "UNBOUNDED" "PRECEDING"
                | "UNBOUNDED" "FOLLOWING"
                | "CURRENT" "ROW"
                | Expression "PRECEDING"
                | Expression "FOLLOWING"
                | "BETWEEN" FrameBound "AND" FrameBound .

CaseExpr        = "CASE" [ Expression ] { "WHEN" Expression "THEN" Expression }
                  [ "ELSE" Expression ] "END" .
CastExpr        = "CAST" "(" Expression "AS" TypeName ")" .
TypeName        = identifier [ "(" NUMBER [ "," NUMBER ] ")" ] .

ArrayAccess     = Primary "[" Expression "]" .

ExpressionList  = Expression { "," Expression } .
```

## AST Types

### Statement Nodes

```go
type SelectStmt struct {
    With *WithClause  // nil if no WITH
    Body *SelectBody
}

type WithClause struct {
    Recursive bool
    CTEs      []*CTE
}

type CTE struct {
    Name   string
    Select *SelectStmt
}

type SelectBody struct {
    Left  *SelectCore
    Op    SetOp       // UNION, INTERSECT, EXCEPT, or empty
    All   bool        // true for UNION ALL
    Right *SelectBody // nil if no set operation
}

type SetOp int
const (
    SetOpNone SetOp = iota
    SetOpUnion
    SetOpIntersect
    SetOpExcept
)

type SelectCore struct {
    Distinct bool
    Columns  []*SelectItem
    From     *FromClause
    Where    Expr
    GroupBy  []Expr
    Having   Expr
    Qualify  Expr
    OrderBy  []*OrderByItem
    Limit    Expr
    Offset   Expr
}

type SelectItem struct {
    Star      bool   // SELECT *
    TableStar string // SELECT t.* (table name)
    Expr      Expr   // expression (if not star)
    Alias     string // AS alias
}

type FromClause struct {
    Source TableRef
    Joins  []*Join
}

type Join struct {
    Type      JoinType
    Right     TableRef
    Condition Expr
}

type JoinType int
const (
    JoinInner JoinType = iota
    JoinLeft
    JoinRight
    JoinFull
    JoinCross
)

type TableRef interface {
    tableRef()
}

type TableName struct {
    Catalog string
    Schema  string
    Name    string
    Alias   string
}

type DerivedTable struct {
    Select *SelectStmt
    Alias  string
}

type LateralTable struct {
    Select *SelectStmt
    Alias  string
}

type OrderByItem struct {
    Expr       Expr
    Desc       bool
    NullsFirst *bool // nil = default, true = NULLS FIRST, false = NULLS LAST
}
```

### Expression Nodes

```go
type Expr interface {
    expr()
}

type ColumnRef struct {
    Table  string // optional qualifier
    Column string
}

type Literal struct {
    Type  LiteralType
    Value string
}

type LiteralType int
const (
    LitNull LiteralType = iota
    LitBool
    LitNumber
    LitString
)

type BinaryExpr struct {
    Left  Expr
    Op    string // +, -, *, /, %, ||, AND, OR, =, !=, <, >, <=, >=
    Right Expr
}

type UnaryExpr struct {
    Op   string // -, +, NOT
    Expr Expr
}

type FuncCall struct {
    Name     string
    Distinct bool
    Args     []Expr
    Star     bool        // COUNT(*)
    Filter   Expr        // FILTER (WHERE ...)
    Window   *WindowSpec
}

type WindowSpec struct {
    Ref         string // named window reference
    PartitionBy []Expr
    OrderBy     []*OrderByItem
    Frame       *FrameSpec
}

type FrameSpec struct {
    Type  FrameType // ROWS, RANGE, GROUPS
    Start *FrameBound
    End   *FrameBound // nil if not BETWEEN
}

type FrameType int
const (
    FrameRows FrameType = iota
    FrameRange
    FrameGroups
)

type FrameBound struct {
    Type   BoundType
    Offset Expr // nil for UNBOUNDED/CURRENT ROW
}

type BoundType int
const (
    BoundUnboundedPreceding BoundType = iota
    BoundUnboundedFollowing
    BoundCurrentRow
    BoundPreceding
    BoundFollowing
)

type CaseExpr struct {
    Operand Expr          // CASE <operand> WHEN... (nil for searched CASE)
    Whens   []*WhenClause
    Else    Expr
}

type WhenClause struct {
    Condition Expr
    Result    Expr
}

type CastExpr struct {
    Expr     Expr
    TypeName string
}

type InExpr struct {
    Expr    Expr
    Not     bool
    List    []Expr      // IN (1, 2, 3)
    Select  *SelectStmt // IN (SELECT ...)
}

type BetweenExpr struct {
    Expr Expr
    Not  bool
    Low  Expr
    High Expr
}

type IsNullExpr struct {
    Expr Expr
    Not  bool
}

type LikeExpr struct {
    Expr    Expr
    Not     bool
    Pattern Expr
    ILike   bool // true for ILIKE
}

type ParenExpr struct {
    Expr Expr
}

type ArrayAccess struct {
    Array Expr
    Index Expr
}
```

## Public API

```go
// Parse parses a SQL statement into an AST.
// Returns error if SQL is invalid or violates constraints.
func Parse(sql string) (*SelectStmt, error)

// ExtractLineage extracts column-level lineage from a SQL statement.
// Schema is optional; required for SELECT * expansion.
func ExtractLineage(sql string, schema Schema) (*ModelLineage, error)

// Schema maps table names to column names (for * expansion)
type Schema map[string][]string
```

## Lineage Output Types

```go
// ModelLineage is the complete lineage for a SQL statement
type ModelLineage struct {
    Sources []string        // upstream table names
    Columns []*ColumnLineage
}

// ColumnLineage is lineage for a single output column
type ColumnLineage struct {
    Name      string       // output column name
    Sources   []*ColumnRef // source columns (fully resolved)
    Transform string       // transformation: "", "SUM", "COUNT", etc.
}

// ColumnRef identifies a source column
type ColumnRef struct {
    Table  string
    Column string
}
```

## Function Classification

Functions are classified for lineage purposes:

| Type | Description | Examples |
|------|-------------|----------|
| **Passthrough** | Column values flow through | `TRIM`, `UPPER`, `COALESCE`, `CAST` |
| **Aggregate** | Many rows to one | `SUM`, `COUNT`, `AVG`, `MIN`, `MAX`, `ARRAY_AGG` |
| **Window** | Requires OVER clause | `ROW_NUMBER`, `RANK`, `LAG`, `LEAD` |
| **Generator** | No input columns | `CURRENT_TIMESTAMP`, `RANDOM`, `UUID` |

Default: **Passthrough** (unknown functions pass through all column refs)

## File Structure

```
pkg/lineage/
├── SPEC.md           # This specification
├── token.go          # TokenType enum, Token struct
├── lexer.go          # Lexer implementation
├── lexer_test.go
├── ast.go            # All AST node types
├── parser.go         # Recursive descent parser
├── parser_test.go
├── lineage.go        # ExtractLineage implementation
├── lineage_test.go
├── errors.go         # Error types
├── readme.md         # Usage documentation
├── parser-plan.md    # Implementation plan (reference only)
└── dialect-plan.md   # Future dialect expansion (deferred)
```

## Error Handling

All errors must include position information:

```go
type ParseError struct {
    Pos     Position
    Message string
}

type Position struct {
    Line   int
    Column int
    Offset int
}

func (e *ParseError) Error() string {
    return fmt.Sprintf("line %d, column %d: %s", e.Pos.Line, e.Pos.Column, e.Message)
}
```

## Standard Error Messages

| Condition | Message |
|-----------|---------|
| Statement doesn't start with SELECT/WITH | `"statement must begin with SELECT or WITH"` |
| Scalar subquery in SELECT | `"scalar subqueries in SELECT columns are not supported; rewrite using a CTE and JOIN"` |
| Unexpected token | `"unexpected token %s, expected %s"` |
| Unterminated string | `"unterminated string literal"` |
| Unterminated identifier | `"unterminated quoted identifier"` |
| Invalid number | `"invalid number literal"` |

## Implementation Order

1. **Lexer** - `token.go`, `lexer.go`, `lexer_test.go`
2. **AST** - `ast.go`
3. **Parser** - `parser.go`, `parser_test.go`, `errors.go`
4. **Lineage** - `lineage.go`, `lineage_test.go`
