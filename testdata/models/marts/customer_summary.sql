-- @config(materialized='table')
-- @import(staging.stg_customers, staging.stg_orders)
-- Customer summary with order metrics

SELECT 
    c.customer_id,
    c.customer_name,
    c.email,
    c.is_active,
    COUNT(DISTINCT o.order_id) AS total_orders,
    COALESCE(SUM(o.order_total), 0) AS lifetime_value,
    MIN(o.order_date) AS first_order_date,
    MAX(o.order_date) AS last_order_date
FROM staging.stg_customers c
LEFT JOIN staging.stg_orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.customer_name, c.email, c.is_active
