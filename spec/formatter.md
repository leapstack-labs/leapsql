# LeapSQL Formatter Specification (`pkg/format`)

Target: Go Implementation

Inspiration: gofmt (Strict, Standard), ruff (Fast, Correct)

## 1. Executive Summary

The LeapSQL Formatter is a deterministic, opinionated tool designed to standardize `.sql` and `.star` files within a LeapSQL project. Unlike generic SQL formatters, it is "Multi-Language Aware," capable of handling:

1. **YAML Frontmatter** (Model metadata)
2. **Pure SQL** (Transformations)
3. **Starlark** (Macros/Functions)

The goal is zero configuration. The user runs `leapsql fmt`, and the code transforms into the canonical style.

---

## 2. Architecture & Pipeline

The formatter operates as a pipeline that disassembles the file, routes components to specialized formatters, and reassembles the result.

### 2.1 The Pipeline Flow

Code snippet

```
graph TD
    A[Input Source File] --> B{File Type?}
    B -->|.star| C[Starlark Formatter]
    B -->|.sql| D[LeapSQL Splitter]

    D --> E[YAML Frontmatter]
    D --> F[SQL Body]

    E --> G[YAML Formatter]
    F --> H[SQL AST Visitor]

    G --> I[Reassembler]
    H --> I
    C --> J[Output]
    I --> J
```

### 2.2 External Dependencies

We leverage the best-in-class Go-native parsers for the non-SQL parts to avoid reinventing wheels.

- **YAML:** `gopkg.in/yaml.v3` (Standard for Go)
- **Starlark:** `github.com/bazelbuild/buildtools/build` (The official Google/Bazel Go implementation)
- **SQL:** Internal `pkg/parser` (Your custom parser)

---

## 3. Component Specifications

### 3.1 The Splitter (`pkg/format/split.go`)

Responsible for safely identifying the boundaries between Frontmatter and SQL without regex brittleness.

- **Logic:**
  1. Read start of file.
  2. Check for `/*---` token.
  3. Scan until closing `---*/`.
  4. Everything remaining is SQL.
- **Output:** `struct { RawFrontmatter string; RawSQL string; HasFrontmatter bool }`

### 3.2 The Frontmatter Formatter (`pkg/format/yaml.go`)

- **Input:** Raw string inside the block.
- **Action:**
  1. `yaml.Unmarshal` into `yaml.Node` (preserves comments better than map[string]interface{}).
  2. `yaml.Marshal` with `Indent: 2`.
- **Output:** Cleaned YAML string.

### 3.3 The Starlark Formatter (`pkg/format/starlark.go`)

Used for standalone `.star` files or (in the future) embedded script blocks.

- **Implementation:** Wrapper around `buildtools`.
  Go
  ```
  import "github.com/bazelbuild/buildtools/build"

  func FormatStarlark(filename string, src []byte) ([]byte, error) {
      // Parse matches the Starlark/Python spec exactly
      f, err := build.Parse(filename, src)
      if err != nil { return nil, err }

      // build.Format applies standard "black" python style
      return build.Format(f), nil
  }
  ```

### 3.4 The SQL Printer (`pkg/format/printer.go`)

This is the core custom work. It uses your `pkg/parser` AST.

#### 3.4.1 Structure

Go

```
type Printer struct {
    // Input
    dialect   *dialect.Dialect
    comments  []*ast.Comment // Sorted list of comments

    // State
    buffer    *bytes.Buffer
    indent    int            // Current depth (0, 1, 2...)
    lastPos   int            // Cursor position in original source

    // Config (Hardcoded defaults per spec)
    tabWidth  int            // 2 spaces
}
```

#### 3.4.2 The Semantic Block System

We classify AST nodes to determine indentation rules:

| **Block Type** | **Examples**              | **Rule**                                             |
| -------------- | ------------------------- | ---------------------------------------------------- |
| **Clause**     | `SELECT`, `FROM`, `WHERE` | Newline, Indent 0. Uppercase Keyword.                |
| **List**       | `[col1, col2, ...]`       | Indent +1. Multiline if count > 3 or line len > 80.  |
| **Composite**  | `CASE`, `WITH`            | Indent +1. Explicit closing token alignment (`END`). |
| **Infix**      | `a = b`, `x AND y`        | Inline unless complexity threshold exceeded.         |
| **Call**       | `COUNT(x)`, `my_macro(y)` | Tight packing (no spaces inside parens).             |

---

## 4. The Style Guide (Canonical Rules)

This defines the "LeapSQL Style".

### 4.1 General

- **Indentation:** 2 Spaces.
- **Max Line Length:** 100 Characters (Soft limit).
- **Newlines:** Unix (`\n`).

### 4.2 SQL Specifics

- **Keywords:** `UPPERCASE` (e.g., `SELECT`, `AS`, `ON`).
- **Nulls/Booleans:** `UPPERCASE` (`NULL`, `TRUE`, `FALSE`).
- **Identifiers:** Preserved as-is. Quoted only if required by dialect/reserved words.
- **Commas:** **Trailing**. (Easier for diffs and adding lines).
  SQL
  ```
  -- Correct
  SELECT
    id,
    name,
  FROM table
  ```
- **Aliases:** `AS` is explicit for columns. Optional for tables.
  SQL
  ```
  SELECT col AS c FROM table t
  ```

### 4.3 Clause Layout ("River" Style)

Left-align keywords to the margin. Indent content.

SQL

```
SELECT
  u.id,
  count(*) AS total
FROM users u
LEFT JOIN orders o
  ON u.id = o.user_id
WHERE
  u.active = TRUE
  AND o.amount > 0
```

### 4.4 Macro Calls

Treat Macros (`my_macro(arg)`) exactly like function calls.

- **Inline:** `SELECT my_macro(x) FROM ...`
- **Multiline:** Only if arguments are complex.

---

## 5. Implementation Logic: "The Hard Parts"

### 5.1 Comment Interleaving

You must map comments to AST nodes **before** printing.

1. **Parse:** Get AST and `[]Comment`.
2. **Mapping:** Create a `map[Node]*CommentGroup`.

   - _Leading Comment:_ Comment ends immediately before Node line starts.
   - _Trailing Comment:_ Comment starts on the same line as Node ends.
   - _Dangling Comment:_ Comment inside a block but not attached to children (e.g., empty `WHERE` block).

3. **Printing:** When `Visit(Node)` is called:

   - Print Leading Comments.
   - Print Node.
   - Print Trailing Comments.

### 5.2 Heuristic Line Breaking (Complexity Score)

To avoid the "wall of text" in `WHERE` clauses:

Go

```
func (p *Printer) shouldBreak(expr ast.Expr) bool {
    // Base cost
    cost := 0

    // Walk expression tree
    // +1 for every BinaryOp (AND, OR)
    // +2 for every FuncCall
    // +5 for Subquery

    return cost > 5
}
```
