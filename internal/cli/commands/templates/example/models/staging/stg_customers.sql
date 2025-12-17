/*---
materialized: table
description: Cleaned customer data from raw source
owner: data-team
tags: [staging, customers]
---*/
SELECT
    id AS customer_id,
    name AS customer_name,
    LOWER(email) AS email,
    CAST(created_at AS DATE) AS signup_date
FROM raw_customers
