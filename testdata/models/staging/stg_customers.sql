-- @config(materialized='table')
-- Staging model for customers
-- Cleans and standardizes raw customer data

SELECT 
    CAST(id AS INTEGER) AS customer_id,
    TRIM(email) AS email,
    TRIM(name) AS customer_name,
    CAST(created_at AS DATE) AS created_at,
    CASE WHEN is_active THEN true ELSE false END AS is_active
FROM raw_customers
