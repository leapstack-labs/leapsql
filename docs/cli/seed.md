---
title: seed
description: Load seed data from CSV files into your database
---

# seed

The `seed` command loads CSV files from your seeds directory into database tables.

## Usage

```bash
leapsql seed [options]
```

## Options

| Option | Default | Description |
|--------|---------|-------------|
| `-models` | `models` | Path to models directory |
| `-seeds` | `seeds` | Path to seeds directory |
| `-macros` | `macros` | Path to macros directory |
| `-database` | (in-memory) | Path to DuckDB database file |
| `-state` | `.leapsql/state.db` | Path to state database |
| `-env` | `dev` | Environment name |

## Examples

### Load all seeds

```bash
leapsql seed
```

### Load seeds from a custom directory

```bash
leapsql seed -seeds ./data/seeds
```

### Load seeds into a persistent database

```bash
leapsql seed -database ./warehouse.duckdb
```

## How It Works

1. LeapSQL scans the seeds directory for CSV files
2. Each CSV file becomes a table with the same name (minus the `.csv` extension)
3. DuckDB automatically infers column types from the CSV content
4. Tables are created using `CREATE OR REPLACE TABLE` (existing data is replaced)

## CSV Requirements

- Files must have a `.csv` extension
- First row must contain column headers
- DuckDB handles most CSV formats automatically

### Example seed file

`seeds/raw_customers.csv`:
```csv
id,name,email,created_at
1,Alice,alice@example.com,2024-01-15
2,Bob,bob@example.com,2024-01-16
3,Charlie,charlie@example.com,2024-01-17
```

This creates a table named `raw_customers` with columns `id`, `name`, `email`, and `created_at`.

## Output

```
Loading seeds from seeds...
Seeds loaded successfully
```

## Automatic Type Inference

DuckDB's `read_csv_auto` function infers appropriate types:

| CSV Content | Inferred Type |
|-------------|---------------|
| `123` | INTEGER |
| `123.45` | DOUBLE |
| `true`/`false` | BOOLEAN |
| `2024-01-15` | DATE |
| `2024-01-15 10:30:00` | TIMESTAMP |
| `hello` | VARCHAR |

## When to Use

- **Development**: Quickly populate test data
- **Static lookups**: Load reference data that rarely changes
- **Testing**: Set up known data states for tests

## Seeds vs Sources

Seeds are for small, static datasets that you version control with your project. For large or frequently updated data, use external sources instead.

## Relationship with run

The `run` command automatically loads seeds before executing models, so you typically don't need to run `seed` separately. Use `seed` when you want to:

- Load seeds without running models
- Debug seed loading issues
- Refresh seed data in a persistent database
