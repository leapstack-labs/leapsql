/*---
materialized: table
---*/
-- Example staging model
SELECT 
    1 as id,
    'example' as name,
    CURRENT_TIMESTAMP as created_at
