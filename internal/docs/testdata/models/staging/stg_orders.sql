-- Order staging model
-- @config materialized='view'

SELECT
    o.id,
    o.customer_id,
    o.amount
FROM raw_orders o
JOIN {{ ref('stg_customers') }} c ON o.customer_id = c.id
