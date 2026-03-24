-- star_schema_ddl.sql
-- Star Schema Design for Superstore Sales Data Warehouse
-- Database: MySQL
-- Schema: 1 Fact Table + 5 Dimension Tables
--
-- Column names match staging_superstore loaded via
-- load_staging.py — dates already DATE type, no STR_TO_DATE needed
-- Safe to run multiple times — cleans before loading

USE superstore_dw;

-- STEP 1: CREATE TABLES

CREATE TABLE IF NOT EXISTS dim_date (
    date_key        INT           PRIMARY KEY,
    full_date       DATE          NOT NULL,
    day_of_month    TINYINT       NOT NULL,
    day_name        VARCHAR(10)   NOT NULL,
    week_number     TINYINT       NOT NULL,
    month_number    TINYINT       NOT NULL,
    month_name      VARCHAR(10)   NOT NULL,
    quarter         TINYINT       NOT NULL,
    year            SMALLINT      NOT NULL,
    is_weekend      BOOLEAN       NOT NULL DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key    INT           PRIMARY KEY AUTO_INCREMENT,
    customer_id     VARCHAR(20)   NOT NULL UNIQUE,
    customer_name   VARCHAR(100)  NOT NULL,
    segment         VARCHAR(30)   NOT NULL,
    created_at      TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_key     INT           PRIMARY KEY AUTO_INCREMENT,
    product_id      VARCHAR(30)   NOT NULL UNIQUE,
    product_name    VARCHAR(255)  NOT NULL,
    category        VARCHAR(50)   NOT NULL,
    sub_category    VARCHAR(50)   NOT NULL,
    created_at      TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_location (
    location_key    INT           PRIMARY KEY AUTO_INCREMENT,
    postal_code     VARCHAR(10),
    city            VARCHAR(100)  NOT NULL,
    state           VARCHAR(100)  NOT NULL,
    region          VARCHAR(30)   NOT NULL,
    country         VARCHAR(50)   NOT NULL DEFAULT 'United States'
);

CREATE TABLE IF NOT EXISTS dim_shipping (
    shipping_key    INT           PRIMARY KEY AUTO_INCREMENT,
    ship_mode       VARCHAR(30)   NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id         BIGINT        PRIMARY KEY AUTO_INCREMENT,
    order_date_key  INT           NOT NULL,
    ship_date_key   INT           NOT NULL,
    customer_key    INT           NOT NULL,
    product_key     INT           NOT NULL,
    location_key    INT           NOT NULL,
    shipping_key    INT           NOT NULL,
    order_id        VARCHAR(30)   NOT NULL,
    row_id          INT,
    quantity        INT           NOT NULL,
    sales           DECIMAL(10,4) NOT NULL,
    discount        DECIMAL(5,4)  NOT NULL DEFAULT 0,
    profit          DECIMAL(10,4) NOT NULL,
    profit_margin   DECIMAL(8,4)  AS (CASE WHEN sales > 0 THEN (profit / sales) * 100 ELSE 0 END) STORED,
    CONSTRAINT fk_order_date  FOREIGN KEY (order_date_key)  REFERENCES dim_date(date_key),
    CONSTRAINT fk_ship_date   FOREIGN KEY (ship_date_key)   REFERENCES dim_date(date_key),
    CONSTRAINT fk_customer    FOREIGN KEY (customer_key)    REFERENCES dim_customer(customer_key),
    CONSTRAINT fk_product     FOREIGN KEY (product_key)     REFERENCES dim_product(product_key),
    CONSTRAINT fk_location    FOREIGN KEY (location_key)    REFERENCES dim_location(location_key),
    CONSTRAINT fk_shipping    FOREIGN KEY (shipping_key)    REFERENCES dim_shipping(shipping_key),
    INDEX idx_order_date  (order_date_key),
    INDEX idx_customer    (customer_key),
    INDEX idx_product     (product_key),
    INDEX idx_location    (location_key),
    INDEX idx_order_id    (order_id)
);

-- STEP 2: CLEAN BEFORE LOADING
-- Prevents duplicates if file is run more than once

SET FOREIGN_KEY_CHECKS = 0;
TRUNCATE TABLE fact_sales;
TRUNCATE TABLE dim_customer;
TRUNCATE TABLE dim_product;
TRUNCATE TABLE dim_location;
TRUNCATE TABLE dim_shipping;
TRUNCATE TABLE dim_date;
SET FOREIGN_KEY_CHECKS = 1;

-- STEP 3: POPULATE dim_date (2014-2018)
-- Single INSERT — no loop, no timeout risk

INSERT INTO dim_date (
    date_key, full_date, day_of_month, day_name,
    week_number, month_number, month_name, quarter, year, is_weekend
)
SELECT
    DATE_FORMAT(d, '%Y%m%d'),
    d,
    DAY(d),
    DAYNAME(d),
    WEEK(d),
    MONTH(d),
    MONTHNAME(d),
    QUARTER(d),
    YEAR(d),
    DAYOFWEEK(d) IN (1, 7)
FROM (
    SELECT DATE_ADD('2014-01-01', INTERVAL n DAY) AS d
    FROM (
        SELECT a.N + b.N * 10 + c.N * 100 + d.N * 1000 AS n
        FROM
            (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
             UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a,
            (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
             UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) b,
            (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
             UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) c,
            (SELECT 0 AS N UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
             UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) d
    ) numbers
    WHERE DATE_ADD('2014-01-01', INTERVAL n DAY) <= '2018-12-31'
) dates;

-- STEP 4: POPULATE DIMENSION TABLES
-- Column names match DESCRIBE staging_superstore exactly

-- dim_customer
INSERT INTO dim_customer (customer_id, customer_name, segment)
SELECT DISTINCT
    `Customer ID`,
    `Customer Name`,
    `Segment`
FROM staging_superstore;

-- dim_product
-- MIN() handles same Product ID with slightly different Product Names
INSERT INTO dim_product (product_id, product_name, category, sub_category)
SELECT
    `Product ID`,
    MIN(`Product Name`)    AS product_name,
    MIN(`Category`)        AS category,
    MIN(`Sub-Category`)    AS sub_category
FROM staging_superstore
GROUP BY `Product ID`;

-- dim_location
-- GROUP BY city+state with MIN() to avoid duplicate city+state combos
INSERT INTO dim_location (postal_code, city, state, region)
SELECT
    MIN(`Postal Code`)  AS postal_code,
    `City`,
    `State`,
    MIN(`Region`)       AS region
FROM staging_superstore
GROUP BY `City`, `State`;

-- dim_shipping
INSERT INTO dim_shipping (ship_mode)
SELECT DISTINCT `Ship Mode`
FROM staging_superstore;

-- STEP 5: LOAD FACT TABLE
-- Dates already DATE type — just DATE_FORMAT for key conversion

INSERT INTO fact_sales (
    order_date_key, ship_date_key,
    customer_key, product_key, location_key, shipping_key,
    order_id, row_id,
    quantity, sales, discount, profit
)
SELECT
    DATE_FORMAT(s.`Order Date`, '%Y%m%d'),
    DATE_FORMAT(s.`Ship Date`,  '%Y%m%d'),
    c.customer_key,
    p.product_key,
    l.location_key,
    sh.shipping_key,
    s.`Order ID`,
    s.`Row ID`,
    s.`Quantity`,
    s.`Sales`,
    s.`Discount`,
    s.`Profit`
FROM staging_superstore s
JOIN dim_customer c  ON c.customer_id = s.`Customer ID`
JOIN dim_product  p  ON p.product_id  = s.`Product ID`
JOIN dim_location l  ON l.city = s.`City` AND l.state = s.`State`
JOIN dim_shipping sh ON sh.ship_mode  = s.`Ship Mode`;

-- STEP 6: VERIFY — fact_sales must show 9994

SELECT 'dim_customer' AS table_name, COUNT(*) AS rows_loaded FROM dim_customer
UNION ALL
SELECT 'dim_product',  COUNT(*) FROM dim_product
UNION ALL
SELECT 'dim_location', COUNT(*) FROM dim_location
UNION ALL
SELECT 'dim_shipping', COUNT(*) FROM dim_shipping
UNION ALL
SELECT 'dim_date',     COUNT(*) FROM dim_date
UNION ALL
SELECT 'fact_sales',   COUNT(*) FROM fact_sales;

-- STEP 7: SAMPLE ANALYTICAL QUERIES

-- 1. Monthly revenue using dim_date
SELECT
    d.year,
    d.month_name,
    ROUND(SUM(f.sales), 2)  AS total_sales,
    ROUND(SUM(f.profit), 2) AS total_profit
FROM fact_sales f
JOIN dim_date d ON f.order_date_key = d.date_key
GROUP BY d.year, d.month_number, d.month_name
ORDER BY d.year, d.month_number;

-- 2. Top 5 products by profit
SELECT
    p.product_name,
    p.category,
    ROUND(SUM(f.profit), 2) AS total_profit
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY p.product_key, p.product_name, p.category
ORDER BY total_profit DESC
LIMIT 5;

-- 3. Revenue by segment and region
SELECT
    c.segment,
    l.region,
    ROUND(SUM(f.sales), 2) AS total_sales
FROM fact_sales f
JOIN dim_customer c ON f.customer_key = c.customer_key
JOIN dim_location l ON f.location_key = l.location_key
GROUP BY c.segment, l.region
ORDER BY c.segment, total_sales DESC;