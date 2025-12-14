/*---
name: stg_customers
materialized: table
---*/

SELECT 
    id,
    name,
    email
FROM raw_customers
