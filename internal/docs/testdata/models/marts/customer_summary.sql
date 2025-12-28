-- Customer summary mart
-- Aggregates customer metrics
-- @config materialized='table'

SELECT
    c.id,
    c.name,
    COUNT(o.id) as order_count,
    SUM(o.amount) as total_amount
FROM {{ ref('stg_customers') }} c
LEFT JOIN {{ ref('stg_orders') }} o ON c.id = o.customer_id
GROUP BY c.id, c.name
