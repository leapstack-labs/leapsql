---
title: CLI Overview
description: Command-line interface reference for LeapSQL
---

# CLI Overview

LeapSQL provides a command-line interface for running models, managing seeds, and inspecting your data transformation pipeline.

## Installation

```bash
go install github.com/leapstack-labs/leapsql/cmd/leapsql@latest
```

## Basic Usage

```bash
leapsql <command> [options]
```

## Commands

| Command | Description |
|---------|-------------|
| `run` | Run all models or specific models |
| `build` | Alias for `run` |
| `list` | List all models and their dependencies |
| `seed` | Load seed data from CSV files |
| `dag` | Show the dependency graph |
| `docs` | Generate and serve documentation |
| `version` | Show version information |

## Global Flags

These flags are available for most commands:

| Flag | Default | Description |
|------|---------|-------------|
| `--models` | `models` | Path to models directory |
| `--seeds` | `seeds` | Path to seeds directory |
| `--macros` | `macros` | Path to macros directory |
| `--database` | (in-memory) | Path to DuckDB database |
| `--state` | `.leapsql/state.db` | Path to state database |
| `--env` | `dev` | Environment name |
| `-v` | `false` | Verbose output |

## Quick Examples

### Run All Models

```bash
leapsql run
```

### Run Specific Models

```bash
leapsql run --select staging.stg_customers,staging.stg_orders
```

### Run with Downstream Dependencies

```bash
leapsql run --select staging.stg_customers --downstream
```

### Use Persistent Database

```bash
leapsql run --database ./data/warehouse.duckdb
```

### Load Seeds Only

```bash
leapsql seed
```

### List Models

```bash
leapsql list
```

### View Dependency Graph

```bash
leapsql dag
```

### Generate Documentation

```bash
leapsql docs build
leapsql docs serve --port 3000
```

## Environment Variables

LeapSQL respects these environment variables:

| Variable | Description |
|----------|-------------|
| `LEAPSQL_MODELS_DIR` | Default models directory |
| `LEAPSQL_DATABASE_PATH` | Default database path |
| `LEAPSQL_ENV` | Default environment |

Command-line flags take precedence over environment variables.

## Exit Codes

| Code | Meaning |
|------|---------|
| `0` | Success |
| `1` | Error (check stderr for details) |

## Getting Help

```bash
# General help
leapsql help
leapsql --help

# Command-specific help
leapsql run --help
leapsql docs --help
```

## Next Steps

- [run Command](/cli/run) - Detailed run command reference
- [list Command](/cli/list) - Model listing options
- [dag Command](/cli/dag) - Dependency visualization
- [seed Command](/cli/seed) - Seed loading options
- [docs Command](/cli/docs) - Documentation generation
