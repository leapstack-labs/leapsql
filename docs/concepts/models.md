---
title: Models
description: Understanding SQL models in LeapSQL
---

Models are the core building blocks of LeapSQL. Each model is a SQL file that defines a transformation, producing a table or view in your database.

## What is a Model?

A model is a `.sql` file that contains:

1. **Optional frontmatter** - YAML configuration specifying how the model should be built
2. **SQL query** - The transformation logic that produces your output

```sql
/*---
name: customer_orders
materialized: table
---*/

SELECT
    c.customer_id,
    c.customer_name,
    COUNT(o.order_id) as total_orders,
    SUM(o.amount) as total_spent
FROM stg_customers c
LEFT JOIN stg_orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.customer_name
```

## Model Naming

### File Names

Model files should use lowercase with underscores:

- `customer_summary.sql`
- `stg_orders.sql`
- `fct_daily_sales.sql`

### Model Names

The model name is determined by (in order of precedence):

1. The `name` field in frontmatter
2. The filename without the `.sql` extension

```sql
/*---
name: my_custom_name  -- This takes precedence
---*/

SELECT * FROM source
```

If no frontmatter `name` is specified, a file named `customer_orders.sql` creates a model called `customer_orders`.

## Automatic Dependency Detection

LeapSQL's most powerful feature is automatic dependency detection. You don't need to use special functions like `ref()` - just write normal SQL:

```sql
-- LeapSQL automatically detects dependencies on:
-- - stg_customers
-- - stg_orders
SELECT
    c.customer_id,
    c.customer_name,
    o.order_id,
    o.amount
FROM stg_customers c
JOIN stg_orders o ON c.customer_id = o.customer_id
```

### How It Works

LeapSQL parses your SQL using a full SQL parser to identify:

- Tables in `FROM` clauses
- Tables in `JOIN` clauses
- Subqueries and CTEs
- `UNION` sources

It then matches these against:
1. Other models in your project
2. Tables created from seeds
3. External tables in your database

### Dependency Resolution

When you run `leapsql run`, models are executed in topological order:

```
stg_customers ─┐
               ├──> customer_orders ──> customer_summary
stg_orders ────┘
```

This ensures dependencies are always built before the models that need them.

## Model Types

### Staging Models

First layer of transformation - clean and standardize raw data:

```sql
/*---
name: stg_customers
materialized: view
---*/

SELECT
    id as customer_id,
    TRIM(name) as customer_name,
    LOWER(email) as email,
    created_at::TIMESTAMP as created_at
FROM raw_customers
WHERE id IS NOT NULL
```

### Intermediate Models

Complex business logic and aggregations:

```sql
/*---
name: int_customer_metrics
materialized: view
---*/

SELECT
    customer_id,
    COUNT(DISTINCT order_id) as order_count,
    SUM(amount) as total_revenue,
    AVG(amount) as avg_order_value,
    MIN(order_date) as first_order_date,
    MAX(order_date) as last_order_date
FROM stg_orders
GROUP BY customer_id
```

### Mart Models

Final models ready for consumption:

```sql
/*---
name: dim_customers
materialized: table
---*/

SELECT
    c.customer_id,
    c.customer_name,
    c.email,
    c.created_at,
    COALESCE(m.order_count, 0) as lifetime_orders,
    COALESCE(m.total_revenue, 0) as lifetime_revenue,
    m.first_order_date,
    m.last_order_date,
    CASE
        WHEN m.total_revenue > 1000 THEN 'high_value'
        WHEN m.total_revenue > 100 THEN 'medium_value'
        ELSE 'low_value'
    END as customer_tier
FROM stg_customers c
LEFT JOIN int_customer_metrics m ON c.customer_id = m.customer_id
```

## Common Table Expressions (CTEs)

CTEs are fully supported and don't create separate dependencies:

```sql
/*---
name: customer_analysis
materialized: table
---*/

WITH customer_orders AS (
    SELECT
        customer_id,
        COUNT(*) as order_count,
        SUM(amount) as total_spent
    FROM stg_orders
    GROUP BY customer_id
),

customer_segments AS (
    SELECT
        customer_id,
        CASE
            WHEN total_spent > 1000 THEN 'premium'
            WHEN total_spent > 100 THEN 'standard'
            ELSE 'basic'
        END as segment
    FROM customer_orders
)

SELECT
    c.customer_id,
    c.customer_name,
    co.order_count,
    co.total_spent,
    cs.segment
FROM stg_customers c
LEFT JOIN customer_orders co ON c.customer_id = co.customer_id
LEFT JOIN customer_segments cs ON c.customer_id = cs.customer_id
```

## Templating in Models

Models can use Starlark templating for dynamic SQL:

### Expressions

```sql
SELECT
    id,
    name,
    created_at
FROM customers
WHERE created_at >= '{{ env.START_DATE }}'
```

### Control Flow

```sql
SELECT
    id,
    {* for col in ['name', 'email', 'phone'] *}
    {{ col }},
    {* endfor *}
    created_at
FROM customers
```

### Macros

```sql
SELECT
    customer_id,
    {{ utils.safe_divide('total_revenue', 'order_count') }} as avg_order_value
FROM customer_metrics
```

See the [Templating](/templating/overview) section for complete documentation.

## Model Selection

Run specific models using the `--select` flag:

```bash
# Run a single model
leapsql run --select customer_orders

# Run multiple models
leapsql run --select customer_orders --select order_metrics

# Run a model and all its dependencies
leapsql run --select +customer_orders

# Run a model and all models that depend on it
leapsql run --select customer_orders+
```

## Best Practices

### 1. One Model, One Purpose

Each model should have a single, clear purpose:

```sql
-- Good: Single purpose
/*--- name: customer_order_count ---*/
SELECT customer_id, COUNT(*) as orders FROM orders GROUP BY 1

-- Bad: Multiple unrelated outputs
/*--- name: customer_and_product_metrics ---*/
SELECT ... -- customer metrics
UNION ALL
SELECT ... -- product metrics (unrelated)
```

### 2. Descriptive Names

Use names that describe what the model contains:

- `fct_daily_orders` - Fact table of daily order data
- `dim_customers` - Customer dimension table
- `int_customer_rfm` - Intermediate RFM (Recency, Frequency, Monetary) calculation

### 3. Document with Frontmatter

Use frontmatter to document ownership and purpose:

```sql
/*---
name: customer_lifetime_value
materialized: table
owner: analytics-team
meta:
  purpose: Calculate customer LTV for marketing segmentation
  refresh: daily
---*/
```

### 4. Keep Dependencies Shallow

Avoid deep dependency chains that are hard to debug:

```
Good: raw -> staging -> mart (2 hops)
Bad: raw -> stg -> int1 -> int2 -> int3 -> mart (5 hops)
```

## Next Steps

- [Frontmatter](/concepts/frontmatter) - All configuration options
- [Materializations](/concepts/materializations) - Table, view, and incremental
- [Dependencies](/concepts/dependencies) - Deep dive into dependency detection
