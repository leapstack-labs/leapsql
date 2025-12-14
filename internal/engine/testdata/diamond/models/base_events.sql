/*---
name: base_events
materialized: table
---*/
SELECT id, event_type, user_id, product_id, created_at
FROM raw_events
