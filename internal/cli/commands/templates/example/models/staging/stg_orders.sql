/*---
materialized: table
description: Cleaned order data with standardized columns
owner: data-team
tags: [staging, orders]
---*/
SELECT
    id AS order_id,
    customer_id,
    product_id,
    quantity,
    CAST(order_date AS DATE) AS order_date,
    UPPER(status) AS order_status
FROM raw_orders
