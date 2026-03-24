-- 02_revenue_analysis.sql
-- Revenue trends, profit margins, and time-based analysis

Describe staging_superstore	;

-- 1. Total revenue and profit by year
SELECT
    YEAR(`Order Date`)        AS order_year,
    ROUND(SUM(Sales), 2)    AS total_sales,
    ROUND(SUM(Profit), 2)   AS total_profit,
    ROUND((SUM(Profit) / SUM(Sales)) * 100, 2) AS profit_margin_pct
FROM staging_superstore
GROUP BY YEAR(`Order Date`)
ORDER BY order_year;

-- 2. Monthly revenue trend (all years combined)
SELECT
    MONTH(`Order Date`)       AS order_month,
    ROUND(SUM(Sales), 2)    AS monthly_sales,
    ROUND(SUM(Profit), 2)   AS monthly_profit
FROM staging_superstore
GROUP BY MONTH(`Order Date`)
ORDER BY order_month;

-- 3. Monthly revenue trend (year-wise breakdown)
SELECT
    YEAR(`Order Date`)        AS order_year,
    MONTH(`Order Date`)       AS order_month,
    ROUND(SUM(Sales), 2)    AS monthly_sales,
    ROUND(SUM(Profit), 2)   AS monthly_profit
FROM staging_superstore
GROUP BY YEAR(`Order Date`), MONTH(`Order date`)
ORDER BY order_year, order_month;

-- 4. Quarterly revenue
SELECT
    YEAR(`Order Date`)                    AS order_year,
    QUARTER(`Order Date`)                 AS quarter,
    ROUND(SUM(Sales), 2)                AS quarterly_sales,
    ROUND(SUM(Profit), 2)               AS quarterly_profit
FROM staging_superstore
GROUP BY YEAR(`Order Date`), QUARTER(`Order Date`)
ORDER BY order_year, quarter;

-- 5. Revenue and profit by category
SELECT
    Category,
    ROUND(SUM(Sales), 2)    AS total_sales,
    ROUND(SUM(Profit), 2)   AS total_profit,
    ROUND((SUM(Profit) / SUM(Sales)) * 100, 2) AS profit_margin_pct
FROM staging_superstore
GROUP BY Category
ORDER BY total_sales DESC;

-- 6. Orders with negative profit (loss-making transactions)
SELECT
    `Order Id`,
    `Product name`,
    Sales,
    Profit,
    Discount,
    Category
FROM staging_superstore
WHERE Profit < 0
ORDER BY Profit ASC
LIMIT 20;

-- 7. Discount impact on profit margin
SELECT
    CASE
        WHEN Discount = 0           THEN 'No Discount'
        WHEN Discount <= 0.10       THEN 'Low (0–10%)'
        WHEN Discount <= 0.20       THEN 'Medium (11–20%)'
        WHEN Discount <= 0.30       THEN 'High (21–30%)'
        ELSE 'Very High (>30%)'
    END AS discount_band,
    COUNT(*)                            AS order_count,
    ROUND(SUM(Sales), 2)                AS total_sales,
    ROUND(SUM(Profit), 2)               AS total_profit,
    ROUND(AVG(Profit), 2)               AS avg_profit_per_order
FROM staging_superstore
GROUP BY discount_band
ORDER BY total_profit DESC;
