/*---
name: combined_report
materialized: table
---*/
SELECT 
    u.user_id,
    u.event_count as user_events,
    p.product_id,
    p.event_count as product_events
FROM user_events u
CROSS JOIN product_events p
