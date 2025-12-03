-- @config(materialized='view')
-- @import(marts.customer_summary, marts.product_summary)
-- Executive dashboard summary combining customer and product metrics

WITH customer_metrics AS (
    SELECT 
        COUNT(*) AS total_customers,
        SUM(CASE WHEN is_active THEN 1 ELSE 0 END) AS active_customers,
        SUM(total_orders) AS total_orders,
        SUM(lifetime_value) AS total_revenue,
        AVG(lifetime_value) AS avg_customer_value
    FROM marts.customer_summary
),
product_metrics AS (
    SELECT 
        COUNT(*) AS total_products,
        SUM(times_ordered) AS total_product_orders,
        COUNT(DISTINCT category) AS categories
    FROM marts.product_summary
)
SELECT 
    c.total_customers,
    c.active_customers,
    c.total_orders,
    ROUND(c.total_revenue, 2) AS total_revenue,
    ROUND(c.avg_customer_value, 2) AS avg_customer_value,
    p.total_products,
    p.categories
FROM customer_metrics c, product_metrics p
