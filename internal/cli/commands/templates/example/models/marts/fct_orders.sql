/*---
materialized: table
description: Order fact table with customer and product details
owner: analytics-team
tags: [marts, orders, fact]
---*/
SELECT
    o.order_id,
    o.order_date,
    o.order_status,
    o.quantity,
    c.customer_id,
    c.customer_name,
    p.product_id,
    p.product_name,
    p.unit_price,
    p.category,
    (o.quantity * p.unit_price) AS line_total,
    {{ utils.generate_surrogate_key('o.order_id', 'o.customer_id', 'o.product_id') }} AS order_key
FROM staging.stg_orders o
JOIN staging.stg_customers c ON o.customer_id = c.customer_id
JOIN staging.stg_products p ON o.product_id = p.product_id
