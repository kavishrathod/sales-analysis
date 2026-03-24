-- 01_basic_exploration.sql
-- Basic dataset exploration queries
-- Dataset: Superstore Sales


-- 1. Total number of records
SELECT COUNT(*) AS total_records
FROM staging_superstore;

DESCRIBE staging_superstore;

-- 2. Date range of orders
SELECT 
    MIN(`Order Date`) AS earliest_order,
    MAX(`Order Date`) AS latest_order
FROM staging_superstore;

-- 3. Distinct categories and sub-categories
SELECT DISTINCT Category, `Sub-Category`
FROM staging_superstore
ORDER BY Category, `Sub-Category`;

-- 4. Number of unique customers
SELECT COUNT(DISTINCT `Customer id`) AS unique_customers
FROM staging_superstore;

-- 5. Number of unique orders
SELECT COUNT(DISTINCT `Order Id`) AS unique_orders
FROM staging_superstore;

-- 6. Check for NULL values in key columns
SELECT
    SUM(CASE WHEN Sales IS NULL THEN 1 ELSE 0 END)       AS null_sales,
    SUM(CASE WHEN Profit IS NULL THEN 1 ELSE 0 END)      AS null_profit,
    SUM(CASE WHEN Quantity IS NULL THEN 1 ELSE 0 END)    AS null_quantity,
    SUM(CASE WHEN `Customer Id` IS NULL THEN 1 ELSE 0 END) AS null_customer_id
FROM staging_superstore;

-- 7. Overview: total sales, profit, quantity
SELECT
    ROUND(SUM(Sales), 2)    AS total_sales,
    ROUND(SUM(Profit), 2)   AS total_profit,
    SUM(Quantity)           AS total_units_sold,
    ROUND(AVG(Discount), 4) AS avg_discount
FROM staging_superstore;
