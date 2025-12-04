# lineage

A SQL column-level lineage library for Go.

## Overview

This library extracts column-level lineage from SQL queries by parsing SQL into an AST and resolving column references through CTEs, aliases, and joins. Initially targets **DuckDB** with a dialect system designed for future expansion to other databases.

## Design Goals

1. **Full SQL parsing**: Recursive descent parser that builds a complete AST
2. **Lineage-focused**: Optimized for SELECT/WITH statements (transformation models)
3. **DuckDB-first**: Start with DuckDB, architecture allows adding other dialects later
4. **CTE-aware**: Properly resolve lineage through Common Table Expressions
5. **Opinionated**: Enforce best practices by disallowing problematic patterns

## Design Constraints (Opinionated)

These constraints enforce best practices and simplify parsing:

| Constraint | Rationale |
|------------|-----------|
| **Models must start with SELECT or WITH** | Models are transformations, not DML |
| **No scalar subqueries in SELECT columns** | Must rewrite as CTE + JOIN (better practice) |
| **Recursive CTEs allowed** | Legitimate use case for hierarchies/graphs |
| **Derived tables in FROM allowed** | Standard SQL pattern |
| **UNION/INTERSECT/EXCEPT allowed** | Common and necessary |

### What's Disallowed

```sql
-- Scalar subquery in SELECT (disallowed)
SELECT 
    customer_id,
    (SELECT MAX(order_date) FROM orders WHERE orders.customer_id = c.customer_id) AS last_order
FROM customers c

-- CTE alternative (allowed - better practice)
WITH last_orders AS (
    SELECT customer_id, MAX(order_date) AS last_order
    FROM orders
    GROUP BY customer_id
)
SELECT c.customer_id, lo.last_order
FROM customers c
LEFT JOIN last_orders lo ON c.customer_id = lo.customer_id
```

Error message for violations: `"Scalar subqueries in SELECT columns are not supported. Rewrite using a CTE and JOIN."`

## Architecture

```
pkg/lineage/
├── token.go          # Token types and Token struct
├── lexer.go          # Dialect-aware tokenizer
├── lexer_test.go     # Lexer unit tests
├── ast.go            # AST node types (Expr, Statement, etc.)
├── parser.go         # Recursive descent parser
├── parser_test.go    # Parser unit tests
├── dialect.go        # Dialect interface and base implementation
├── dialects.go       # Built-in dialect configurations
├── scope.go          # Scope/symbol table for resolution
├── resolver.go       # Column resolution through CTEs and aliases
├── resolver_test.go  # Resolver unit tests
├── lineage.go        # Main API: ExtractLineage()
├── lineage_test.go   # Integration tests
└── errors.go         # Error types and messages
```

## Grammar

### Statement Grammar

```ebnf
Statement       = [ "WITH" [ "RECURSIVE" ] CTEList ] SelectStatement .
CTEList         = CTE { "," CTE } .
CTE             = identifier "AS" "(" SelectStatement ")" .

SelectStatement = SelectCore { SetOp SelectCore } .
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
JoinType        = "LEFT" [ "OUTER" ] | "RIGHT" [ "OUTER" ] | "FULL" [ "OUTER" ] | "INNER" .

TableRef        = TableName [ [ "AS" ] identifier ]
                | "(" SelectStatement ")" [ "AS" ] identifier
                | LateralClause .
TableName       = [ identifier "." [ identifier "." ] ] identifier .

LateralClause   = "LATERAL" "(" SelectStatement ")" [ "AS" ] identifier .
```

### Expression Grammar

```ebnf
Expression      = OrExpr .
OrExpr          = AndExpr { "OR" AndExpr } .
AndExpr         = NotExpr { "AND" NotExpr } .
NotExpr         = [ "NOT" ] Comparison .

Comparison      = Addition [ CompOp Addition ]
                | Addition [ "NOT" ] "IN" "(" ExpressionList ")"
                | Addition [ "NOT" ] "IN" "(" SelectStatement ")"  -- Only in WHERE/HAVING
                | Addition [ "NOT" ] "BETWEEN" Addition "AND" Addition
                | Addition "IS" [ "NOT" ] "NULL"
                | Addition [ "NOT" ] "LIKE" Addition .
CompOp          = "=" | "!=" | "<>" | "<" | ">" | "<=" | ">=" .

Addition        = Multiplication { ( "+" | "-" | "||" ) Multiplication } .
Multiplication  = Unary { ( "*" | "/" | "%" ) Unary } .
Unary           = [ "-" | "+" | "NOT" ] Primary .

Primary         = Literal
                | ColumnRef
                | FunctionCall
                | CaseExpr
                | CastExpr
                | "(" Expression ")"
                | ArrayExpr
                | StructExpr .

Literal         = NUMBER | STRING | "TRUE" | "FALSE" | "NULL" .

ColumnRef       = [ identifier "." [ identifier "." ] ] identifier .

FunctionCall    = identifier "(" [ "DISTINCT" ] [ ExpressionList ] ")" [ WindowSpec ]
                | identifier "(" "*" ")" [ WindowSpec ] .

WindowSpec      = "OVER" "(" [ PartitionBy ] [ OrderBy ] [ FrameSpec ] ")"
                | "OVER" identifier .
PartitionBy     = "PARTITION" "BY" ExpressionList .
OrderBy         = "ORDER" "BY" OrderByList .
OrderByList     = OrderByItem { "," OrderByItem } .
OrderByItem     = Expression [ "ASC" | "DESC" ] [ "NULLS" ( "FIRST" | "LAST" ) ] .
FrameSpec       = ( "ROWS" | "RANGE" | "GROUPS" ) FrameBound .
FrameBound      = "UNBOUNDED" "PRECEDING"
                | "CURRENT" "ROW"
                | Expression "PRECEDING"
                | "BETWEEN" FrameBound "AND" FrameBound .

CaseExpr        = "CASE" [ Expression ] { "WHEN" Expression "THEN" Expression } [ "ELSE" Expression ] "END" .
CastExpr        = "CAST" "(" Expression "AS" TypeName ")" .
TypeName        = identifier [ "(" NUMBER [ "," NUMBER ] ")" ] .

ExpressionList  = Expression { "," Expression } .
```

## Dialect System

Dialects are defined as Go structs (not YAML) for type safety and performance. The base parser handles 90%+ of SQL; dialects override specific syntax quirks.

### DuckDB Dialect (Initial Target)

DuckDB follows ANSI SQL standards closely, making it an ideal starting point:

| Feature | DuckDB Behavior |
|---------|-----------------|
| **Identifier quoting** | `"double quotes"` (ANSI standard) |
| **String literals** | `'single quotes'` with `''` escape |
| **Case sensitivity** | Case-insensitive (identifiers normalized to lowercase) |
| **String concatenation** | `\|\|` operator |
| **QUALIFY clause** | Supported |
| **LATERAL subqueries** | Supported |
| **JSON access** | Functions (`json_extract()`) not path syntax |

### Dialect Interface (For Future Expansion)

```go
// Dialect customizes parsing and resolution for a specific SQL dialect
type Dialect interface {
    // Lexer customization
    IdentifierQuoteChar() rune       // " for DuckDB/ANSI
    IdentifierQuoteEndChar() rune    // Same as start char
    StringEscapeChar() rune          // '' doubling for ANSI
    IsCaseSensitive() bool           // false for DuckDB
    
    // Function handling
    GetFunction(name string) *FunctionDef
    
    // Resolution
    DefaultCatalog() string
    DefaultSchema() string
}
```

### Current Dialect

```go
var Dialects = map[string]Dialect{
    "duckdb": &DuckDBDialect{},
    // Future: databricks, snowflake, bigquery, postgres
}
```

### Function Classification

Functions are classified by how they affect lineage:

```go
type LineageType string

const (
    LineagePassthrough LineageType = "passthrough" // TRIM(x) <- x
    LineageAggregate   LineageType = "aggregate"   // SUM(x) <- x (many-to-one)
    LineageWindow      LineageType = "window"      // ROW_NUMBER() OVER(...)
    LineageNone        LineageType = "none"        // UUID(), CURRENT_TIMESTAMP
)

type FunctionDef struct {
    Name        string
    Lineage     LineageType
    ArgCount    int  // -1 for variadic
    IsWindow    bool
}
```

## AST Node Types

### Statement Nodes

```go
type Statement interface{ stmtNode() }

type SelectStmt struct {
    With   *WithClause
    Body   *SelectBody
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
    Left   *SelectCore
    Op     SetOpType      // UNION, INTERSECT, EXCEPT
    All    bool           // UNION ALL
    Right  *SelectBody    // For chained set operations
}

type SelectCore struct {
    Distinct bool
    Columns  []SelectItem
    From     *FromClause
    Where    Expr
    GroupBy  []Expr
    Having   Expr
    Qualify  Expr
    OrderBy  []OrderByItem
    Limit    Expr
    Offset   Expr
}

type SelectItem struct {
    Star      bool     // SELECT *
    TableStar string   // SELECT t.*
    Expr      Expr     // Expression
    Alias     string   // AS alias
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

type TableRef interface{ tableRefNode() }

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
```

### Expression Nodes

```go
type Expr interface{ exprNode() }

type ColumnRef struct {
    Table  string  // optional qualifier
    Column string
}

type Literal struct {
    Type  LiteralType  // Number, String, Bool, Null
    Value string
}

type BinaryExpr struct {
    Left  Expr
    Op    string  // +, -, *, /, ||, AND, OR, =, <, etc.
    Right Expr
}

type UnaryExpr struct {
    Op   string  // -, +, NOT
    Expr Expr
}

type FuncCall struct {
    Name     string
    Distinct bool
    Args     []Expr
    Star     bool        // COUNT(*)
    Window   *WindowSpec
}

type WindowSpec struct {
    Name        string      // Named window reference
    PartitionBy []Expr
    OrderBy     []OrderByItem
    Frame       *FrameSpec
}

type CaseExpr struct {
    Operand Expr        // CASE operand WHEN... (optional)
    Whens   []WhenClause
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
    Expr   Expr
    Not    bool
    Values []Expr
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

type ParenExpr struct {
    Expr Expr
}
```

## Lineage Output

```go
// ColumnRef identifies a column in a table/model
type ColumnRef struct {
    Model  string // table or model name (fully resolved)
    Column string // column name
}

// ColumnLineage represents lineage for a single output column
type ColumnLineage struct {
    Output    string      // output column name
    Inputs    []ColumnRef // source columns (fully resolved through CTEs)
    Transform string      // transformation applied (SUM, CONCAT, etc.)
}

// ModelLineage represents complete lineage for a SQL query/model
type ModelLineage struct {
    Model   string          // model name (if known)
    Sources []string        // upstream tables/models
    Columns []ColumnLineage // column-level lineage
}
```

## CTE Resolution

CTEs are resolved in topological order since they can reference each other.

### Resolution Algorithm

```
1. Extract CTEs from SQL (WITH clauses)
2. Order CTEs by dependency (CTE2 uses CTE1 -> process CTE1 first)
3. For each CTE in order:
   a. Parse SELECT columns
   b. For each column expression:
      - Extract column references
      - Resolve refs against already-processed CTEs
   c. Store resolved columns in registry
4. Parse final SELECT statement
5. Resolve against complete registry
6. Output: flat lineage graph
```

### Example Resolution

```sql
WITH 
  order_totals AS (
    SELECT customer_id, SUM(amount) AS total
    FROM orders
    GROUP BY customer_id
  ),
  enriched AS (
    SELECT c.id, c.name, ot.total
    FROM customers c
    JOIN order_totals ot ON c.id = ot.customer_id
  )
SELECT id, name, total FROM enriched
```

Resolution trace for `total`:
```
Step 1: Parse order_totals CTE
  - total -> SUM(orders.amount) -> [orders.amount]
  - Register: order_totals.total = [orders.amount]

Step 2: Parse enriched CTE  
  - total -> ot.total -> order_totals.total -> [orders.amount]
  - Register: enriched.total = [orders.amount]

Step 3: Parse final SELECT
  - total -> enriched.total -> [orders.amount]

Output: total <- orders.amount (transform: SUM)
```

## API

### Main Functions

```go
// Parse parses SQL into an AST
func Parse(sql string, dialect string) (*SelectStmt, error)

// ExtractLineage extracts column-level lineage from SQL
func ExtractLineage(sql string, dialect string, schema Schema) (*ModelLineage, error)
```

### Schema (Optional)

For `SELECT *` expansion:

```go
// Schema provides table/column information for * expansion
type Schema map[string][]string // table -> columns

// Example
schema := Schema{
    "customers": {"id", "name", "email"},
    "orders":    {"id", "customer_id", "amount", "date"},
}
```

## Usage

```go
package main

import (
    "fmt"
    "github.com/your/dbgo/pkg/lineage"
)

func main() {
    sql := `
    WITH order_totals AS (
        SELECT customer_id, SUM(amount) AS total
        FROM orders
        GROUP BY customer_id
    )
    SELECT 
        c.customer_id,
        c.name,
        ot.total AS lifetime_value
    FROM customers c
    LEFT JOIN order_totals ot ON c.customer_id = ot.customer_id
    `
    
    result, err := lineage.ExtractLineage(sql, "databricks", nil)
    if err != nil {
        panic(err)
    }
    
    for _, col := range result.Columns {
        fmt.Printf("%s <- %v (%s)\n", 
            col.Output, 
            col.Inputs, 
            col.Transform)
    }
    // Output:
    // customer_id <- [customers.customer_id] (passthrough)
    // name <- [customers.name] (passthrough)
    // lifetime_value <- [orders.amount] (SUM)
}
```

## Implementation Phases

### Phase 1: Lexer (2 days)
- Token types definition
- Lexer struct with dialect config
- String literal handling (with escapes)
- Identifier handling (quoted and unquoted)
- Number handling (integer, decimal, scientific)
- Comment stripping (line and block)
- Keyword recognition
- Dialect-specific tokenization (backtick vs double-quote)

### Phase 2: AST Types (1 day)
- Expression node types
- Statement node types
- TableRef variants
- Helper methods (String() for debugging)

### Phase 3: Expression Parser (2-3 days)
- Operator precedence implementation
- Binary expressions (+, -, *, /, ||, AND, OR, comparisons)
- Unary expressions (-, NOT)
- Column references (table.column)
- Function calls with arguments
- CASE expressions
- CAST expressions
- Window specifications (OVER clause)
- IN, BETWEEN, IS NULL, LIKE
- Parenthesized expressions

### Phase 4: Statement Parser (2 days)
- SELECT core (columns, FROM, WHERE, GROUP BY, etc.)
- FROM clause with JOINs
- Table aliases
- Derived tables (subquery in FROM)
- WITH clause and CTEs
- Recursive CTE support
- UNION/INTERSECT/EXCEPT
- Subquery rejection in SELECT columns

### Phase 5: Dialect System (0.5 days)
- DuckDB dialect configuration (hardcoded for now)
- Function classification for common DuckDB functions
- Placeholder interface for future dialect expansion

### Phase 6: Scope and Resolution (2 days)
- Symbol table for table/alias tracking
- CTE registration and ordering
- Column reference resolution
- Alias resolution (table.col -> real_table.col)
- Self-reference detection for recursive CTEs

### Phase 7: Lineage Extraction (2 days)
- AST walking for column collection
- Function classification application
- CTE lineage propagation
- UNION column position mapping
- Final lineage output structure

### Phase 8: Testing and Polish (2-3 days)
- Test with all testdata/ models
- Error message quality
- Edge case handling
- Documentation
- Performance testing with large SQL

**Total: ~12-14 days (~2.5 weeks)** (reduced due to DuckDB-only focus)

## Success Criteria

1. **Parses all testdata/ models** without errors
2. **Correct lineage extraction** for:
   - Simple SELECT with columns and aliases
   - JOINs (LEFT, RIGHT, INNER, CROSS)
   - CTEs (including recursive)
   - UNION/INTERSECT/EXCEPT
   - Aggregate functions (SUM, COUNT, etc.)
   - Window functions with OVER clause
   - CASE expressions
   - CAST expressions
3. **Proper error messages** for unsupported patterns
4. **Dialect support** for: DuckDB (other dialects deferred)
5. **Performance**: Parse 1000-line SQL in <100ms

## Edge Cases

### SELECT *

Requires schema information to expand:

```go
// Without schema - returns wildcard marker
lineage, err := ExtractLineage("SELECT * FROM customers", "databricks", nil)
// Returns: [{Output: "*", Inputs: [{customers, "*"}]}]

// With schema - expands columns
schema := Schema{"customers": {"id", "name", "email"}}
lineage, err := ExtractLineage("SELECT * FROM customers", "databricks", schema)
// Returns lineage for id, name, email
```

### Aliased Tables

```sql
SELECT a.id FROM customers AS a
-- Resolved: customers.id
```

### Self-Joins

```sql
SELECT a.id, b.name 
FROM customers a
JOIN customers b ON a.parent_id = b.id
-- Both resolve to customers table
```

### UNION/INTERSECT/EXCEPT

```sql
SELECT id, name FROM customers
UNION ALL
SELECT id, name FROM archived_customers
-- Each output column has inputs from both tables
-- Output: id <- [customers.id, archived_customers.id]
```

## References

- [sqlglot](https://github.com/tobymao/sqlglot) - Python SQL parser with lineage (reference)
- [Crafting Interpreters](https://craftinginterpreters.com/) - Parser design patterns
- [dialect-plan.md](./dialect-plan.md) - Detailed dialect system design
- [parser-plan.md](./parser-plan.md) - Detailed parser implementation plan
