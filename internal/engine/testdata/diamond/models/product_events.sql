/*---
name: product_events
materialized: table
---*/
SELECT product_id, COUNT(*) as event_count
FROM base_events
GROUP BY product_id
