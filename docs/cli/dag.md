---
title: dag
description: Visualize the dependency graph of your models
---

# dag

The `dag` command displays the dependency graph organized by execution levels, showing both upstream dependencies and downstream dependents for each model.

## Usage

```bash
leapsql dag [options]
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

### Show the dependency graph

```bash
leapsql dag
```

### Custom models directory

```bash
leapsql dag -models ./sql/models
```

## Output Format

The output groups models by execution level:

```
Dependency Graph (execution levels):

Level 0:
  stg_customers
    used by: dim_customers, fct_orders
  stg_orders
    used by: fct_orders
  stg_products
    used by: dim_products

Level 1:
  dim_customers
    depends on: stg_customers
    used by: order_summary, revenue_by_customer
  dim_products
    depends on: stg_products
  fct_orders
    depends on: stg_orders, dim_customers
    used by: order_summary, revenue_by_customer

Level 2:
  order_summary
    depends on: fct_orders, dim_customers
  revenue_by_customer
    depends on: fct_orders, dim_customers

Total: 8 models, 10 dependencies
```

### Understanding Levels

- **Level 0**: Models with no dependencies (typically staging models or those reading from seeds)
- **Level 1**: Models that depend only on Level 0 models
- **Level 2**: Models that depend on Level 1 models
- And so on...

Models at the same level can execute in parallel since they have no interdependencies.

## Key Information

For each model, the DAG shows:

- **depends on**: Upstream models that must execute first
- **used by**: Downstream models that depend on this model

## Use Cases

### Understand data flow

The DAG visualization helps understand how data flows through your project from sources to final outputs.

### Identify parallelization opportunities

Models at the same level can run concurrently. This helps estimate potential performance improvements.

### Impact analysis

When modifying a model, check its "used by" list to understand which downstream models might be affected.

### Debug circular dependencies

If you have circular dependencies, the DAG command will fail with an error message indicating the cycle.
