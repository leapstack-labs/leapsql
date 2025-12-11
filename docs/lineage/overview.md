---
title: Lineage Overview
description: Understanding data lineage in LeapSQL
---

# Lineage Overview

LeapSQL automatically tracks how data flows through your transformations, providing both table-level and column-level lineage.

## What is Data Lineage?

Data lineage traces the origin and transformation of data as it moves through your pipeline:

```
raw_customers ──┐
                ├──> stg_customers ──> customer_360
raw_orders ─────┴──> stg_orders ────┘
```

LeapSQL tracks two levels of lineage:

1. **Table Lineage**: Which tables/models depend on which other tables
2. **Column Lineage**: Which specific columns flow into which output columns

## Why Lineage Matters

### Impact Analysis

Know what breaks when something changes:

```
If raw_customers schema changes:
  └── stg_customers (directly affected)
      └── customer_360 (indirectly affected)
      └── customer_report (indirectly affected)
```

### Debugging

Trace data issues back to their source:

```
customer_360.lifetime_value is wrong
  └── Comes from stg_orders.amount (check here)
      └── Comes from raw_orders.amount (or here)
```

### Compliance

Understand where sensitive data flows:

```
raw_customers.email (PII)
  └── stg_customers.email
      └── customer_360.email
      └── email_campaigns.recipient
```

### Documentation

Auto-generated data flow documentation.

## How LeapSQL Tracks Lineage

### Automatic Detection

LeapSQL parses your SQL to automatically detect lineage:

```sql
/*---
name: customer_summary
---*/

SELECT
    c.customer_id,          -- From stg_customers.customer_id
    c.name,                 -- From stg_customers.name
    COUNT(o.order_id) as order_count,  -- From stg_orders.order_id
    SUM(o.amount) as total_spent       -- From stg_orders.amount
FROM stg_customers c
LEFT JOIN stg_orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name
```

LeapSQL automatically knows:
- **Table lineage**: `customer_summary` depends on `stg_customers` and `stg_orders`
- **Column lineage**:
  - `customer_summary.customer_id` <- `stg_customers.customer_id`
  - `customer_summary.name` <- `stg_customers.name`
  - `customer_summary.order_count` <- `stg_orders.order_id` (via COUNT)
  - `customer_summary.total_spent` <- `stg_orders.amount` (via SUM)

### No Manual Annotation

Unlike some tools, you don't need to annotate your SQL:

```sql
-- Not required: ref('stg_customers')
-- Not required: source('raw', 'customers')
-- Just write normal SQL!

SELECT * FROM stg_customers
```

## Viewing Lineage

### DAG Command

View table-level lineage:

```bash
leapsql dag
```

Output:
```
raw_customers
└── stg_customers
    └── customer_summary
        └── customer_report

raw_orders
└── stg_orders
    └── customer_summary
        └── customer_report
    └── order_metrics
```

### State Database

Lineage is stored in the state database (`.leapsql/state.db`):

```sql
-- Table dependencies
SELECT * FROM dependencies;

-- Column lineage
SELECT * FROM column_lineage;
```

## Lineage Types

### Direct Lineage

Column passes through unchanged:

```sql
SELECT
    customer_id,  -- Direct: customer_id ← source.customer_id
    name          -- Direct: name ← source.name
FROM source
```

### Transformed Lineage

Column is derived from source columns:

```sql
SELECT
    UPPER(name) as name,     -- Transformed: name ← source.name
    first_name || ' ' || last_name as full_name  -- Transformed: full_name ← first_name, last_name
FROM source
```

### Aggregated Lineage

Column is aggregated from source:

```sql
SELECT
    customer_id,
    COUNT(*) as order_count,  -- Aggregated: order_count ← orders (implicit)
    SUM(amount) as total      -- Aggregated: total ← source.amount
FROM orders
GROUP BY customer_id
```

### Filtered Lineage

Source data is filtered but columns pass through:

```sql
SELECT
    customer_id,
    name
FROM customers
WHERE status = 'active'  -- Filter doesn't change lineage
```

## Complex Lineage Scenarios

### JOINs

LeapSQL tracks lineage through joins:

```sql
SELECT
    c.customer_id,      -- From customers.customer_id
    c.name,             -- From customers.name
    o.order_date,       -- From orders.order_date
    o.amount            -- From orders.amount
FROM customers c
JOIN orders o ON c.customer_id = o.customer_id
```

### Subqueries

Lineage is traced through subqueries:

```sql
SELECT
    customer_id,
    total_amount
FROM (
    SELECT
        customer_id,
        SUM(amount) as total_amount  -- From orders.amount
    FROM orders
    GROUP BY customer_id
) subquery
```

### CTEs

Common Table Expressions are handled correctly:

```sql
WITH customer_totals AS (
    SELECT
        customer_id,
        SUM(amount) as total  -- From orders.amount
    FROM orders
    GROUP BY customer_id
)
SELECT
    c.name,
    ct.total  -- Traced back to orders.amount
FROM customers c
JOIN customer_totals ct ON c.customer_id = ct.customer_id
```

### UNION

Lineage tracks multiple sources:

```sql
SELECT customer_id, amount FROM orders_2023
UNION ALL
SELECT customer_id, amount FROM orders_2024
-- Lineage: output columns ← orders_2023 AND orders_2024
```

### CASE Statements

Conditional lineage is tracked:

```sql
SELECT
    customer_id,
    CASE
        WHEN region = 'US' THEN domestic_price
        ELSE international_price
    END as price
    -- Lineage: price ← region, domestic_price, international_price
FROM products
```

## Lineage Limitations

### Dynamic SQL

Template-generated SQL is analyzed after rendering:

```sql
SELECT {{ dynamic_column }} FROM {{ dynamic_table }}
-- Lineage based on rendered values
```

### External Functions

Custom database functions may obscure lineage:

```sql
SELECT my_custom_function(col1, col2) as result
-- LeapSQL may not know which columns affect result
```

### Complex Expressions

Very complex expressions may have simplified lineage:

```sql
SELECT
    COALESCE(
        NULLIF(TRIM(a.col1), ''),
        b.col2,
        'default'
    ) as value
-- Lineage shows: value ← a.col1, b.col2
```

## Best Practices

### 1. Keep Transformations Traceable

```sql
-- Good: Clear lineage
SELECT
    customer_id,
    revenue / orders as avg_order_value
FROM metrics

-- Less clear: What contributed to this?
SELECT
    customer_id,
    complex_udf(a, b, c, d, e, f) as mystery_value
FROM metrics
```

### 2. Use Meaningful Aliases

```sql
-- Good: Aliases help understanding
SELECT
    src.customer_id,
    src.name as customer_name,
    SUM(ord.amount) as total_revenue
FROM customers src
JOIN orders ord ON ...

-- Less helpful
SELECT
    a.x,
    a.y,
    SUM(b.z)
FROM t1 a
JOIN t2 b ON ...
```

### 3. Document Complex Derivations

```sql
/*---
name: customer_score
meta:
  column_notes:
    risk_score: |
      Calculated using formula:
      - Base: customer_age_days
      - Modified by: order_count, return_rate
      - See: docs/risk-scoring.md
---*/
```

## Next Steps

- [Table Lineage](/lineage/table-lineage) - Deep dive into table dependencies
- [Column Lineage](/lineage/column-lineage) - Column-level tracking details
- [Dependencies](/concepts/dependencies) - How dependencies are detected
