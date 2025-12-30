---
title: query
description: Query the state database
---

# query

Query the LeapSQL state database directly using SQL.

## Usage

```bash
leapsql query [SQL] [flags]
leapsql query [command]
```

## Commands

| Command | Description |
|---------|-------------|
| `tables` | List all tables and views in the state database |
| `views` | List views only |
| `schema <table>` | Show schema for a table or view |
| `search <term>` | Full-text search across models |

## Flags

| Flag | Description |
|------|-------------|
| `-i, --input <path>` | Read SQL from file |
| `-f, --format <fmt>` | Output format: table, json, csv, md (default: table) |

## Examples

### Execute SQL directly

```bash
leapsql query "SELECT * FROM v_runs"
leapsql query "SELECT path, status FROM v_model_runs WHERE status = 'failed'"
```

### List tables and views

```bash
leapsql query tables
```

### Show table schema

```bash
leapsql query schema models
leapsql query schema v_dependencies
```

### Full-text search

```bash
leapsql query search "revenue"
leapsql query search "customer" --format json
```

### Output formats

```bash
# JSON (for scripting)
leapsql query "SELECT * FROM v_runs" --format json

# CSV (for export)
leapsql query "SELECT * FROM v_models" --format csv > models.csv

# Markdown (for documentation)
leapsql query "SELECT * FROM v_runs LIMIT 5" --format md
```

### Read from file

```bash
leapsql query -i my_query.sql
```

### Pipe from stdin

```bash
echo "SELECT * FROM v_models" | leapsql query
cat complex_query.sql | leapsql query --format json
```

### Interactive REPL

When invoked without arguments, enters interactive mode:

```bash
leapsql query
```

```
LeapSQL Query REPL (state: .leapsql/state.db)
Type .help for commands, .quit to exit

leapsql> .tables
leapsql> SELECT * FROM v_runs LIMIT 3;
leapsql> .schema models
leapsql> .quit
```

#### REPL Commands

| Command | Description |
|---------|-------------|
| `.help` | Show available commands |
| `.tables` | List all tables and views |
| `.views` | List views only |
| `.schema <name>` | Show schema for table/view |
| `.clear` | Clear the screen |
| `.quit` / `.exit` | Exit the REPL |

## Available Views

The state database includes these user-friendly views:

| View | Description |
|------|-------------|
| `v_models` | Models with derived folder |
| `v_runs` | Recent runs with duration |
| `v_model_runs` | Model execution history |
| `v_failed_runs` | Failed runs with error details |
| `v_stale_models` | Models not in last successful run |
| `v_dependencies` | Model dependencies by path |
| `v_dependents` | Reverse dependencies |
| `v_sources` | External data sources |
| `v_columns` | Model output columns |
| `v_column_sources` | Column lineage |
| `v_macros` | Registered macros |

## Common Queries

### Recent failed runs

```sql
SELECT model_path, error, started_at 
FROM v_failed_runs 
ORDER BY started_at DESC 
LIMIT 10;
```

### Model dependencies

```sql
SELECT * FROM v_dependencies 
WHERE model_path = 'marts.dim_customers';
```

### Models referencing a source

```sql
SELECT model_path FROM v_source_refs 
WHERE source_name = 'raw_orders';
```

### Run duration statistics

```sql
SELECT 
    environment,
    COUNT(*) as runs,
    ROUND(AVG(duration_secs), 1) as avg_duration,
    MAX(duration_secs) as max_duration
FROM v_runs 
GROUP BY environment;
```
