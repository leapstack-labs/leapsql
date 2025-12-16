### 1. The High-Level Architecture

The system is split into three distinct layers:

- **Contracts (`pkg/`):** Public interfaces defining _what_ a DB is.
- **Implementations (`pkg/adapters/`):** The actual code for Postgres, DuckDB, etc.
- **Core Engine (`internal/`):** The logic that uses the contracts.

### 2. The Directory Structure

Plaintext

```
github.com/my-org/dbt-go/
├── cmd/
│   └── dbt/
│       └── main.go             # Entrypoint: Imports adapters to register them
├── pkg/
│   ├── adapter/                # RUNTIME CONTRACT
│   │   ├── interface.go        # Interface: Open(), Query(), Begin()
│   │   └── registry.go         # Global Map: stores factory functions
│   ├── dialect/                # STATIC CONTRACT (LSP/AST)
│   │   ├── definition.go       # Struct: Keywords, Quotes, Functions
│   │   └── registry.go         # Global Map: stores syntax rules
│   └── adapters/               # IMPLEMENTATIONS (Public Reference)
│       ├── postgres/
│       │   ├── adapter.go      # Runtime logic (database/sql)
│       │   └── dialect.go      # Static rules (syntax definition)
│       └── duckdb/
│           ├── adapter.go
│           └── dialect.go
└── internal/
    └── core/
        └── runner.go           # The Engine: Calls pkg/adapter & pkg/dialect
```

---

### 3. The Components

#### A. The Contracts (`pkg/`)

These are the shared rules. They have **zero dependencies** on implementations.

**`pkg/adapter/interface.go` (Runtime)**

Go

```
type Adapter interface {
    Open(ctx context.Context, config Config) error
    Query(ctx context.Context, sql string) (Rows, error)
    GenerateSchemaSQL(schema string) string
}
```

**`pkg/dialect/definition.go` (Static/LSP)**

Go

```
type Dialect struct {
    Name        string
    QuoteOpen   string
    Keywords    []string
    Functions   map[string]string // e.g. "NVL" -> "COALESCE"
}
```

#### B. The Implementation (`pkg/adapters/duckdb`)

The adapter package is the "bridge" that fulfills both contracts.

**`pkg/adapters/duckdb/dialect.go` (Syntax Rules)**

Go

```
package duckdb

import "github.com/my-org/dbt-go/pkg/dialect"

var Syntax = dialect.New("duckdb").
    Identifiers(`"`, `"`).
    Aggregates("LIST", "AVG").
    Build()
```

**`pkg/adapters/duckdb/adapter.go` (Connection Logic)**

Go

```
package duckdb

import (
    "github.com/my-org/dbt-go/pkg/adapter"
    _ "github.com/marcboeker/go-duckdb" // The actual Driver
)

type Adapter struct { ... }
func (a *Adapter) Open(...) error { ... }
```

**`pkg/adapters/duckdb/init.go` (The Wiring)**

Go

```
package duckdb

import (
    "github.com/my-org/dbt-go/pkg/adapter"
    "github.com/my-org/dbt-go/pkg/dialect"
)

func init() {
    // 1. Register Runtime (for the Runner)
    adapter.Register("duckdb", func(c adapter.Config) adapter.Adapter {
        return &Adapter{config: c}
    })

    // 2. Register Static Syntax (for the LSP)
    dialect.Register(Syntax)
}
```

---

### 4. How it Works in Practice

#### Scenario A: The Runner (Runtime)

1. **User** runs `dbt run --profiles-dir .`.
2. **`main.go`** has `import _ "pkg/adapters/duckdb"`.
3. **`init()`** fires, registering the "duckdb" factory.
4. **Core** reads `type: duckdb` from config.
5. **Core** calls `adapter.Get("duckdb")` -> Returns the connection object.
6. **Core** executes SQL.

#### Scenario B: The LSP (Static Analysis)

1. **LSP** starts up (no DB connection required).
2. **LSP** sees `type: duckdb`.
3. **LSP** calls `dialect.Get("duckdb")`.
4. **LSP** receives the `Dialect` struct (containing `LIST`, `AVG`, `"` quotes).
5. **LSP** parses the Jinja/SQL file using those rules to generate the AST.
