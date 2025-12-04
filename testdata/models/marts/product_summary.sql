-- @config(materialized='table')
-- Product sales metrics

SELECT 
    p.product_id,
    p.product_name,
    p.category,
    p.price AS unit_price,
    COUNT(DISTINCT o.order_id) AS times_ordered,
    SUM(o.quantity) AS total_units_sold,
    SUM(o.order_total) AS total_revenue
FROM staging.stg_products p
LEFT JOIN staging.stg_orders o ON p.product_id = o.product_id
GROUP BY p.product_id, p.product_name, p.category, p.price
