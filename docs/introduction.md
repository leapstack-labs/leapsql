# Introduction

LeapSQL is a modern SQL transformation framework designed to make data transformations simple, transparent, and maintainable. Unlike traditional transformation tools that require you to learn proprietary functions and DSLs, LeapSQL lets you write pure SQL while automatically handling dependencies and lineage tracking.

## What is LeapSQL?

LeapSQL is a command-line tool that:

- **Parses SQL models** from `.sql` files with optional YAML frontmatter for configuration
- **Automatically detects dependencies** between models by analyzing your SQL queries
- **Tracks column-level lineage** showing exactly how data flows through your transformations
- **Executes models in order** based on the dependency graph
- **Supports templating** with Starlark expressions and macros for dynamic SQL generation

## Key Features

### Automatic Dependency Detection

Unlike other tools that require explicit `ref()` function calls, LeapSQL analyzes your SQL to automatically detect which tables and models each query depends on:

```sql
-- LeapSQL automatically knows this depends on staging_customers and staging_orders
SELECT
    c.customer_id,
    c.name,
    SUM(o.amount) as lifetime_value
FROM staging_customers c
JOIN staging_orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name
```

### Column-Level Lineage

LeapSQL doesn't just track table dependencies - it tracks how individual columns flow through your transformations:

```
customers.lifetime_value <- staging_orders.amount (via SUM aggregation)
customers.name <- staging_customers.name (direct)
customers.customer_id <- staging_customers.customer_id (direct)
```

### Starlark Templating

For dynamic SQL generation, LeapSQL uses Starlark - a Python-like language that's safe, hermetic, and deterministic:

```sql
/*---
name: daily_metrics
materialized: table
---*/

SELECT
    date,
    {* for metric in ['revenue', 'orders', 'customers'] *}
    SUM({{ metric }}) as total_{{ metric }},
    {* endfor *}
FROM events
WHERE date >= '{{ env.START_DATE }}'
GROUP BY date
```

### Multiple Materializations

Choose how each model is persisted:

- **table**: Full table replacement (DROP + CREATE)
- **view**: Lightweight SQL views
- **incremental**: Efficient updates using merge/upsert operations

## How It Works

1. **Write SQL models** in `.sql` files with optional frontmatter configuration
2. **Run `leapsql run`** to execute your models
3. LeapSQL **parses and analyzes** each model to build a dependency graph
4. Models are **executed in topological order** respecting dependencies
5. **Lineage information** is captured and stored for analysis

## Comparison with Other Tools

| Feature | LeapSQL | dbt | SQLMesh |
|---------|---------|-----|---------|
| Automatic dependency detection | Yes | No (requires ref()) | Partial |
| Column-level lineage | Yes | Limited | Yes |
| Templating language | Starlark | Jinja | Python/SQL |
| Primary database | DuckDB | Various | Various |
| Configuration | YAML frontmatter | YAML files | Python |

## Next Steps

- [Quickstart](/quickstart) - Get up and running in 5 minutes
- [Installation](/installation) - Detailed installation instructions
- [Project Structure](/project-structure) - Learn how to organize your project
