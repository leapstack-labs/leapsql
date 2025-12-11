---
title: run
description: Execute models and build your data transformations
---

# run

The `run` command executes your SQL models in dependency order, creating tables or views in your database.

## Usage

```bash
leapsql run [options]
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
| `-select` | (all) | Comma-separated list of models to run |
| `-downstream` | `false` | Include downstream dependents when using `-select` |
| `-v` | `false` | Verbose output |

## Examples

### Run all models

```bash
leapsql run
```

This discovers all models in your project, loads seeds, and executes models in topological order.

### Run with a persistent database

```bash
leapsql run -database ./warehouse.duckdb
```

### Run specific models

```bash
leapsql run -select stg_customers,stg_orders
```

### Run models with downstream dependents

```bash
leapsql run -select stg_customers -downstream
```

This runs `stg_customers` and all models that depend on it.

### Run with verbose output

```bash
leapsql run -v
```

Shows detailed progress including seed loading and model discovery steps.

### Custom directories

```bash
leapsql run -models ./sql/models -seeds ./sql/seeds -macros ./sql/macros
```

## Execution Order

LeapSQL automatically determines the correct execution order based on model dependencies:

1. Seeds are loaded first
2. Models are discovered and parsed
3. Dependencies are extracted from `ref()` calls
4. Models execute in topological order (dependencies before dependents)

## Output

A successful run displays:

```
Found 8 models
Running all models...
Run abc123: success
Completed in 245ms
```

If a model fails, the error is displayed and the run stops.

## The build Alias

The `build` command is an alias for `run`:

```bash
leapsql build  # equivalent to: leapsql run
```
