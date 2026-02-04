-- =============================================================================
-- ANALYTICAL QUERIES
-- =============================================================================

-- 1. Daily Sales Trend
SELECT 
    order_date,
    SUM(revenue) as total_revenue,
    COUNT(DISTINCT order_id) as total_orders,
    SUM(quantity) as total_items_sold
FROM sales.fact_sales
GROUP BY order_date
ORDER BY order_date DESC;

-- 2. Top 10 Product Categories by Revenue
SELECT 
    p.category,
    SUM(f.revenue) as total_revenue,
    COUNT(DISTINCT f.order_id) as order_count
FROM sales.fact_sales f
JOIN sales.dim_product p ON f.product_id = p.product_id
GROUP BY p.category
ORDER BY total_revenue DESC
LIMIT 10;

-- 3. Country-wise Revenue Distribution
SELECT 
    c.country,
    SUM(f.revenue) as total_revenue,
    COUNT(DISTINCT f.customer_id) as unique_customers
FROM sales.fact_sales f
JOIN sales.dim_customer c ON f.customer_id = c.customer_id
GROUP BY c.country
ORDER BY total_revenue DESC;

-- 4. Month-over-Month Revenue Growth
WITH monthly_revenue AS (
    SELECT 
        DATE_TRUNC('month', order_date) as sales_month,
        SUM(revenue) as revenue
    FROM sales.fact_sales
    GROUP BY 1
)
SELECT 
    sales_month,
    revenue,
    LAG(revenue) OVER (ORDER BY sales_month) as prev_month_revenue,
    ROUND(((revenue - LAG(revenue) OVER (ORDER BY sales_month)) / LAG(revenue) OVER (ORDER BY sales_month)) * 100, 2) as growth_percentage
FROM monthly_revenue;
