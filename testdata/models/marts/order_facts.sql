-- @config(materialized='incremental', unique_key='order_id')
-- @import(staging.stg_orders, staging.stg_customers)
-- Incremental order facts - only processes new orders

SELECT 
    o.order_id,
    o.customer_id,
    c.customer_name,
    o.product_id,
    o.quantity,
    o.unit_price,
    o.order_total,
    o.order_date,
    o.status,
    CURRENT_TIMESTAMP AS processed_at
FROM staging.stg_orders o
JOIN staging.stg_customers c ON o.customer_id = c.customer_id
-- #if is_incremental
WHERE o.order_date > (SELECT COALESCE(MAX(order_date), '1900-01-01') FROM {{ this }})
-- #endif
