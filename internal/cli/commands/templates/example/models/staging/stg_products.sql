/*---
materialized: table
description: Product catalog with cleaned data
owner: data-team
tags: [staging, products]
---*/
SELECT
    id AS product_id,
    name AS product_name,
    price AS unit_price,
    UPPER(category) AS category
FROM raw_products
