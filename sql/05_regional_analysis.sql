-- 05_regional_analysis.sql
-- Region and state-level sales performance


-- 1. Sales and profit by region
SELECT
    Region,
    COUNT(DISTINCT `Order Id`)                            AS total_orders,
    ROUND(SUM(Sales), 2)                                AS total_sales,
    ROUND(SUM(Profit), 2)                               AS total_profit,
    ROUND((SUM(Profit) / SUM(Sales)) * 100, 2)          AS profit_margin_pct,
    ROUND(AVG(Discount), 4)                             AS avg_discount
FROM staging_superstore
GROUP BY Region
ORDER BY total_sales DESC;

-- 2. Top 10 states by sales
SELECT
    State,
    Region,
    ROUND(SUM(Sales), 2)    AS total_sales,
    ROUND(SUM(Profit), 2)   AS total_profit,
    COUNT(DISTINCT `Order Id`) AS order_count
FROM staging_superstore
GROUP BY State, Region
ORDER BY total_sales DESC
LIMIT 10;

-- 3. Bottom 10 states by profit
SELECT
    State,
    Region,
    ROUND(SUM(Profit), 2)   AS total_profit,
    ROUND(SUM(Sales), 2)    AS total_sales
FROM staging_superstore
GROUP BY State, Region
ORDER BY total_profit ASC
LIMIT 10;

-- 4. Top 10 cities by sales
SELECT
    City,
    State,
    Region,
    ROUND(SUM(Sales), 2)    AS total_sales,
    ROUND(SUM(Profit), 2)   AS total_profit
FROM staging_superstore
GROUP BY City, State, Region
ORDER BY total_sales DESC
LIMIT 10;

-- 5. Category performance by region
SELECT
    Region,
    Category,
    ROUND(SUM(Sales), 2)    AS total_sales,
    ROUND(SUM(Profit), 2)   AS total_profit
FROM staging_superstore
GROUP BY Region, Category
ORDER BY Region, total_sales DESC;

-- 6. Shipping mode usage by region
SELECT
    Region,
    `Ship Mode`,
    COUNT(DISTINCT `Order Id`)    AS orders,
    ROUND(SUM(Sales), 2)        AS total_sales
FROM staging_superstore
GROUP BY Region, `Ship Mode`
ORDER BY Region, Orders DESC;
