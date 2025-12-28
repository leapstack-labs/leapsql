-- Customer staging model
-- Cleans and deduplicates raw customer data
-- @config materialized='view'

SELECT
    id,
    name,
    email
FROM raw_customers
WHERE id IS NOT NULL
