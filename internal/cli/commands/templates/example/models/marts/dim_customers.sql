/*---
materialized: table
description: Customer dimension with order statistics
owner: analytics-team
tags: [marts, customers, dimension]
---*/
SELECT
    c.customer_id,
    c.customer_name,
    c.email,
    c.signup_date,
    COUNT(DISTINCT o.order_id) AS total_orders,
    COALESCE(SUM(o.quantity), 0) AS total_items_ordered,
    MIN(o.order_date) AS first_order_date,
    MAX(o.order_date) AS last_order_date
FROM staging.stg_customers c
LEFT JOIN staging.stg_orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.customer_name, c.email, c.signup_date
