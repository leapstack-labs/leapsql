/*---
name: event_log
materialized: incremental
unique_key: event_id
---*/
SELECT event_id, event_type, payload, created_at
FROM raw_events
