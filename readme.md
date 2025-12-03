# Project Overview: DBGo (Working Title)

**The Compiled, Database-State Data Transformation Engine**

### 1\. The Core Philosophy

**DBGo** is a modern data transformation tool written in **Go**. Unlike existing tools that rely on runtime interpretation (Python/Jinja) and file-based state (JSON artifacts), DBGo treats data models as **compiled software** and manages state in a **live database**.

**The Goal:** To provide the developer experience of a modern compiler (instant feedback, type safety, single binary) with the deployment power of a database-backed state engine (virtual environments, zero-copy staging).

-----

### 2\. Architecture & The "Unlock"

#### A. State Management: Database \> Files

Instead of generating a static `manifest.json`, DBGo stores the entire project graph and execution history in a backend database (Postgres or embedded SQLite).

  * **Virtual Environments:** "Staging" is just a metadata pointer to a specific commit hash in the DB. We can create ephemeral environments in milliseconds without copying data.
  * **Concurrency Control:** The DB acts as a mutex. Two CI jobs cannot overwrite the same table simultaneously.
  * **Drift Detection:** The tool queries the State DB to know *exactly* what is running in production, preventing "stale artifact" errors.

#### B. The Compilation Step: SQL $\to$ Go $\to$ Binary

We do not interpret SQL at runtime. We parse `.sql` files and generate Go source code, which is compiled into a project-specific binary.

  * **Type Safety:** If Model A changes a column name, Model B (downstream) fails to compile immediately.
  * **Performance:** Parsing 5,000 models happens in milliseconds via concurrent Go routines.
  * **Distribution:** The output is a single binary. No Python, no `pip`, no `venv` hell.

-----

### 3\. The Syntax Specification

We use standard `.sql` files augmented with "Magic Comments" (Pragmas) for imports and configuration, and C-style Preprocessor directives for logic.

**Example Model:** `models/finance/revenue.sql`

```sql
-- @config: materialized='incremental'
-- @config: unique_key='transaction_id'

-- 1. Explicit Imports (Top of file = Fast Parsing)
-- @import: orders = models.staging.stg_orders
-- @import: rates  = models.seeds.exchange_rates

SELECT 
    o.transaction_id,
    o.amount * r.rate as usd_amount
FROM :orders as o              -- 2. Variable Usage (':' prefix)
LEFT JOIN :rates as r 
    ON o.currency = r.currency
    
-- 3. Logic Control (Preprocessor Style)
#if is_incremental
  WHERE o.updated_at > (SELECT max(updated_at) FROM :this)
#endif
```

-----

### 4\. Technical Implementation Plan

#### The Parser & Code Generator

  * **Input:** User writes `.sql` files.
  * **Parser:** Scans only the header comments (`-- @import`) to build the DAG. This makes graph generation instant ($O(1)$ lookup vs dbt's $O(N)$ regex scan).
  * **Generator:** Creates a Go struct for every model.
      * `stg_orders` $\to$ `type StgOrders struct { ... }`
      * Validation logic is embedded into the struct methods.

#### The Adapter Interface (Repository Pattern)

We define a strict Go Interface that new database adapters must implement. This makes adding Databricks/DuckDB/Clickhouse trivial and strictly typed.

```go
type Adapter interface {
    // Core connectivity
    Connect(ctx context.Context, cfg Config) (Connection, error)
    
    // Execution
    Exec(ctx context.Context, sql string) error
    Query(ctx context.Context, sql string) (*Rows, error)
    
    // Catalog & State
    GetTableMetadata(ctx context.Context, table string) (Metadata, error)
}
```

#### The Developer Experience (The "Watcher")

Similar to modern frontend tools (Vite/Tailwind):

1.  User runs `dbgo dev`.
2.  User saves a `.sql` file.
3.  `dbgo` detects change $\to$ Generates Go Code $\to$ Recompiles Memory Binary $\to$ Runs Model.
4.  Total loop time: \< 500ms.

-----

### 5\. Competitive Comparison

| Feature | dbt Core | DBGo (This Project) |
| :--- | :--- | :--- |
| **Language** | Python | Go (Golang) |
| **Parsing Strategy** | Runtime Regex (Slow) | Compile-time Headers (Instant) |
| **Templating** | Jinja2 (Complex, Fragile) | Native SQL + Preprocessor (`#if`) |
| **Dependency Mgmt** | `ref('string')` | Explicit Import / Go Structs |
| **State Storage** | `manifest.json` (File) | PostgreSQL / SQLite (Database) |
| **Deployment** | Python Environment | Single Static Binary |
| **Environments** | Physical Schemas | Virtual Pointers |

### 6\. Immediate Next Steps

1.  **Define the Grammar:** Finalize the "Magic Comment" syntax specs.
2.  **Build the Parser:** Write a Go tool that scans a directory of `.sql` files and outputs a simple DOT graph based on `@import` tags.
3.  **Prototype State DB:** Define the schema for the Postgres table that will hold the state (Run ID, Model Hash, Materialization Status).
