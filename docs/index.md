---
layout: home
hero:
  name: LeapSQL
  text: SQL Transformation Framework
  tagline: Transform your data with pure SQL. No ref() functions, no boilerplate - just write SQL and let LeapSQL handle the rest.
  image:
    dark: /logo-index-dark.svg
    light: /logo-index-light.svg
    alt: LeapSQL Logo
  actions:
    - theme: brand
      text: Get Started
      link: /quickstart
    - theme: alt
      text: GitHub
      link: https://github.com/leapstack-labs/leapsql

features:
  - title: Pure SQL
    details: Write standard SQL without learning a new DSL. Dependencies are automatically detected from your queries.
  - title: Column-Level Lineage
    details: Track data flow at the column level. Know exactly where each field comes from and how it's transformed.
  - title: Starlark Macros
    details: Create reusable SQL patterns with Starlark, a Python-like language that's safe and deterministic.
  - title: Fast Execution
    details: Built in Go for speed. Run transformations on DuckDB for lightning-fast local development.
---

## Quick Example

```sql
/*---
name: customers
materialized: table
---*/

SELECT
    c.id,
    c.name,
    c.email,
    COUNT(o.id) as order_count,
    SUM(o.amount) as total_spent
FROM raw_customers c
LEFT JOIN raw_orders o ON c.id = o.customer_id
GROUP BY c.id, c.name, c.email
```

LeapSQL automatically:

- Detects that this model depends on `raw_customers` and `raw_orders`
- Tracks that `order_count` comes from `orders.id` and `total_spent` from `orders.amount`
- Builds and runs models in the correct order
