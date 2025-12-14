/*---
name: user_events
materialized: table
---*/
SELECT user_id, COUNT(*) as event_count
FROM base_events
GROUP BY user_id
