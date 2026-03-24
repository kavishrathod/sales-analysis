-- 03_top_products.sql
-- Top performing products and sub-categories

Describe staging_superstore	;

-- 1. Top 10 products by total sales
SELECT
    `Product name`,
    Category,
    `Sub-Category`,
    ROUND(SUM(Sales), 2)    AS total_sales,
    ROUND(SUM(Profit), 2)   AS total_profit,
    SUM(Quantity)           AS total_units_sold
FROM staging_superstore
GROUP BY `Product name`, Category, `Sub-Category`
ORDER BY total_sales DESC
LIMIT 10;

-- 2. Top 10 products by total profit
SELECT
    `Product name`,
    Category,
    `Sub-Category`,
    ROUND(SUM(Profit), 2)   AS total_profit,
    ROUND(SUM(Sales), 2)    AS total_sales
FROM staging_superstore
GROUP BY `Product Name`, Category, `Sub-Category`
ORDER BY total_profit DESC
LIMIT 10;

-- 3. Worst 10 products by profit (loss leaders)
SELECT
    `Product Name`,
    Category,
    `Sub-Category`,
    ROUND(SUM(Profit), 2)   AS total_profit,
    ROUND(SUM(Sales), 2)    AS total_sales
FROM staging_superstore
GROUP BY `Product name`, Category, `Sub-Category`
ORDER BY total_profit ASC
LIMIT 10;

-- 4. Sub-category performance: sales, profit, profit margin
SELECT
    `Sub-Category`,
    Category,
    ROUND(SUM(Sales), 2)    AS total_sales,
    ROUND(SUM(Profit), 2)   AS total_profit,
    ROUND((SUM(Profit) / SUM(Sales)) * 100, 2) AS profit_margin_pct,
    SUM(Quantity)           AS total_units
FROM staging_superstore
GROUP BY `Sub-Category`, Category
ORDER BY total_sales DESC;

-- 5. Sub-categories with negative profit
SELECT
    `Sub-Category`,
    Category,
    ROUND(SUM(Profit), 2)   AS total_profit,
    ROUND(SUM(Sales), 2)    AS total_sales,
    COUNT(*)                AS transaction_count
FROM staging_superstore
GROUP BY `Sub-Category`, Category
HAVING total_profit < 0
ORDER BY total_profit ASC;

-- 6. Most ordered sub-categories (by quantity)
SELECT
    `Sub-Category`,
    SUM(Quantity)           AS total_units_sold,
    ROUND(SUM(Sales), 2)    AS total_sales
FROM staging_superstore
GROUP BY `Sub-Category`
ORDER BY total_units_sold DESC;
