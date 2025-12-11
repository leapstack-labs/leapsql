---
title: list
description: List all models and their dependencies
---

# list

The `list` command displays all models in your project along with their materialization type and dependencies.

## Usage

```bash
leapsql list [options]
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

### List all models

```bash
leapsql list
```

### List models from a custom directory

```bash
leapsql list -models ./sql/models
```

## Output Format

The output shows models in execution order with their materialization type and dependencies:

```
Models (8 total):

   1. models/staging/stg_customers.sql    [table]
   2. models/staging/stg_orders.sql       [table]
   3. models/staging/stg_products.sql     [table]
   4. models/marts/dim_customers.sql      [table] <- stg_customers
   5. models/marts/dim_products.sql       [view] <- stg_products
   6. models/marts/fct_orders.sql         [table] <- stg_orders, dim_customers
   7. models/marts/order_summary.sql      [view] <- fct_orders, dim_customers
   8. models/marts/revenue_by_customer.sql [table] <- fct_orders, dim_customers
```

Each line shows:
- **Execution order number** - The position in the build sequence
- **Model path** - Relative path to the model file
- **Materialization** - `[table]` or `[view]`
- **Dependencies** - Models this model depends on (shown with `<-`)

## Use Cases

### Verify project structure

Before running your models, use `list` to verify that:
- All models are discovered
- Dependencies are correctly detected
- Materialization settings are correct

### Debug dependency issues

If models are executing in an unexpected order, `list` shows the resolved dependency graph.

### Project documentation

Use `list` output to understand the flow of data through your models.
