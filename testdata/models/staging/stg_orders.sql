-- @config(materialized='table')
-- Staging model for orders
-- Cleans and standardizes raw order data

SELECT 
    CAST(id AS INTEGER) AS order_id,
    CAST(customer_id AS INTEGER) AS customer_id,
    CAST(product_id AS INTEGER) AS product_id,
    CAST(quantity AS INTEGER) AS quantity,
    CAST(unit_price AS DECIMAL(10,2)) AS unit_price,
    CAST(order_date AS DATE) AS order_date,
    LOWER(status) AS status,
    CAST(quantity AS DECIMAL) * CAST(unit_price AS DECIMAL) AS order_total
FROM raw_orders
WHERE status != 'cancelled'
