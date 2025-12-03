-- @config(materialized='table')
-- Staging model for products
-- Cleans and standardizes raw product data

SELECT 
    CAST(id AS INTEGER) AS product_id,
    TRIM(name) AS product_name,
    LOWER(TRIM(category)) AS category,
    CAST(price AS DECIMAL(10,2)) AS price
FROM raw_products
