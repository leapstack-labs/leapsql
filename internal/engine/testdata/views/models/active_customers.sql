/*---
name: active_customers
materialized: view
---*/
SELECT id, name, email
FROM customers_base
WHERE status = 'active'
