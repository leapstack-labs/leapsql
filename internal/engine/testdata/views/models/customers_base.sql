/*---
name: customers_base
materialized: table
---*/
SELECT id, name, email, status FROM raw_customers
