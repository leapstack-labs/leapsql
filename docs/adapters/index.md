---
title: Adapters
description: Understanding database adapters in LeapSQL
---

# Adapters

Adapters provide a consistent interface for LeapSQL to interact with different database systems. They abstract away database-specific details, allowing the engine to execute transformations regardless of the underlying database.

## What is an Adapter?

An adapter is a component that:

- Establishes connections to a database
- Executes SQL statements
- Retrieves table metadata
- Loads data from external files (like CSVs)

## Adapter Interface

All adapters implement a common interface:

```go
type Adapter interface {
    // Connect establishes a connection to the database
    Connect(ctx context.Context, cfg Config) error
    
    // Close releases database resources
    Close() error
    
    // Exec executes SQL that doesn't return rows
    Exec(ctx context.Context, sql string) error
    
    // Query executes SQL that returns rows
    Query(ctx context.Context, sql string) (*Rows, error)
    
    // GetTableMetadata retrieves column and table information
    GetTableMetadata(ctx context.Context, table string) (*Metadata, error)
    
    // LoadCSV loads a CSV file into a table
    LoadCSV(ctx context.Context, tableName string, filePath string) error
}
```

## Configuration

Adapters are configured through a `Config` struct:

```go
type Config struct {
    Type     string            // Database type (e.g., "duckdb")
    Path     string            // File path for file-based databases
    Host     string            // Hostname for network databases
    Port     int               // Port number
    Database string            // Database name
    Username string            // Authentication username
    Password string            // Authentication password
    Schema   string            // Default schema
    Options  map[string]string // Driver-specific options
}
```

## Available Adapters

| Adapter | Status | Description |
|---------|--------|-------------|
| [DuckDB](/adapters/duckdb) | Stable | High-performance analytical database |

## Metadata

Adapters can retrieve metadata about tables:

```go
type Metadata struct {
    Schema    string   // Schema containing the table
    Name      string   // Table name
    Columns   []Column // Column definitions
    RowCount  int64    // Approximate row count
    SizeBytes int64    // Approximate size
}

type Column struct {
    Name       string // Column name
    Type       string // Data type
    Nullable   bool   // Allows NULL values
    PrimaryKey bool   // Part of primary key
    Position   int    // Ordinal position
}
```

This metadata powers features like:
- Documentation generation
- Column lineage tracking
- Schema validation

## Using Adapters

Adapters are typically managed by the engine, but you can use them directly:

```go
import "github.com/leapstack-labs/leapsql/internal/adapter"

// Create adapter
db := adapter.NewDuckDBAdapter()

// Connect
cfg := adapter.Config{Path: "./warehouse.duckdb"}
if err := db.Connect(ctx, cfg); err != nil {
    log.Fatal(err)
}
defer db.Close()

// Execute SQL
err := db.Exec(ctx, "CREATE TABLE users (id INT, name VARCHAR)")

// Query data
rows, err := db.Query(ctx, "SELECT * FROM users")

// Get metadata
meta, err := db.GetTableMetadata(ctx, "users")
```

## In-Memory vs Persistent

File-based adapters like DuckDB support both modes:

```go
// In-memory (temporary, fast)
cfg := adapter.Config{Path: ":memory:"}

// Persistent (survives restarts)
cfg := adapter.Config{Path: "./data/warehouse.duckdb"}
```

## Future Adapters

LeapSQL's adapter architecture is designed for extensibility. Potential future adapters include:

- PostgreSQL
- SQLite
- ClickHouse
- Snowflake
- BigQuery

The common interface ensures that models work across different databases with minimal changes.
