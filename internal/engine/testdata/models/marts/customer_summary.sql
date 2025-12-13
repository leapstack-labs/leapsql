/*---
name: customer_summary
materialized: table
---*/

SELECT 
    c.id as customer_id,
    c.name as customer_name,
    c.email,
    COUNT(o.id) as total_orders,
    COALESCE(SUM(o.amount), 0) as total_amount
FROM staging.stg_customers c
LEFT JOIN staging.stg_orders o ON c.id = o.customer_id
GROUP BY c.id, c.name, c.email
