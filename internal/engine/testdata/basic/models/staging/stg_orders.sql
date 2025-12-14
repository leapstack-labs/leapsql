/*---
name: stg_orders
materialized: table
---*/

SELECT 
    id,
    customer_id,
    amount,
    status
FROM raw_orders
