/*---
name: formatted_orders
materialized: table
---*/
SELECT 
    id,
    customer_id,
    {{ formatting.dollars("amount") }} as amount_dollars,
    {{ formatting.status_label("status") }} as status_label
FROM raw_orders
