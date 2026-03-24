-- 04_customer_segments.sql
-- Customer segmentation and buying behavior analysis


-- 1. Revenue and profit by customer segment
SELECT
    Segment,
    COUNT(DISTINCT `Customer Id`)                         AS unique_customers,
    COUNT(DISTINCT `Order Id`)                            AS total_orders,
    ROUND(SUM(Sales), 2)                                AS total_sales,
    ROUND(SUM(Profit), 2)                               AS total_profit,
    ROUND(SUM(Sales) / COUNT(DISTINCT `Customer Id`), 2)  AS avg_revenue_per_customer
FROM staging_superstore
GROUP BY Segment
ORDER BY total_sales DESC;

-- 2. Top 10 customers by total revenue
SELECT
    `Customer Id`,
    `Customer Name`,
    Segment,
    COUNT(DISTINCT `Order id`)    AS total_orders,
    ROUND(SUM(Sales), 2)        AS total_sales,
    ROUND(SUM(Profit), 2)       AS total_profit
FROM staging_superstore
GROUP BY `Customer Id`, `Customer Name`, Segment
ORDER BY total_sales DESC
LIMIT 10;

-- 3. Top 10 customers by total profit contribution
SELECT
    `Customer Id`,
    `Customer Name`,
    Segment,
    ROUND(SUM(Profit), 2)   AS total_profit,
    ROUND(SUM(Sales), 2)    AS total_sales
FROM staging_superstore
GROUP BY `Customer Id`, `Customer Name`, Segment
ORDER BY total_profit DESC
LIMIT 10;

-- 4. Average order value by segment
SELECT
    Segment,
    ROUND(SUM(Sales) / COUNT(DISTINCT `Order Id`), 2) AS avg_order_value,
    ROUND(AVG(Discount), 4)                          AS avg_discount
FROM staging_superstore
GROUP BY Segment
ORDER BY avg_order_value DESC;

-- 5. Segment preference by category
SELECT
    Segment,
    Category,
    ROUND(SUM(Sales), 2)    AS total_sales,
    COUNT(DISTINCT `Order Id`) AS orders
FROM staging_superstore
GROUP BY Segment, Category
ORDER BY Segment, total_sales DESC;

-- 6. Customers with more than 10 orders (loyal customers)
SELECT
    `Customer Id`,
    `Customer Name`,
    segment,
    COUNT(DISTINCT `Order Id`)    AS total_orders,
    ROUND(SUM(Sales), 2)        AS total_sales
FROM staging_superstore
GROUP BY `Customer Id`, `Customer Name`, segment
HAVING total_orders > 10
ORDER BY total_orders DESC;
