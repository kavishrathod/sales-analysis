import pandas as pd
import mysql.connector

# ── Config ───────────────────────────────────────
MYSQL_USER     = "root"
MYSQL_PASSWORD = "369369369"
MYSQL_HOST     = "localhost"
MYSQL_DB       = "superstore_dw"
CSV_PATH       = "data/superstore_sales.csv"
# ─────────────────────────────────────────────────

df = pd.read_csv(CSV_PATH, encoding="latin-1")
print(f"CSV loaded: {len(df)} rows")

conn = mysql.connector.connect(
    host=MYSQL_HOST, user=MYSQL_USER,
    password=MYSQL_PASSWORD, database=MYSQL_DB
)
cursor = conn.cursor()

# Drop and recreate clean staging table
cursor.execute("DROP TABLE IF EXISTS staging_superstore")
cursor.execute("""
    CREATE TABLE staging_superstore (
        `Row ID`        INT,
        `Order ID`      VARCHAR(30),
        `Order Date`    DATE,
        `Ship Date`     DATE,
        `Ship Mode`     VARCHAR(30),
        `Customer ID`   VARCHAR(20),
        `Customer Name` VARCHAR(100),
        `Segment`       VARCHAR(30),
        `Country`       VARCHAR(50),
        `City`          VARCHAR(100),
        `State`         VARCHAR(100),
        `Postal Code`   VARCHAR(10),
        `Region`        VARCHAR(20),
        `Product ID`    VARCHAR(30),
        `Category`      VARCHAR(50),
        `Sub-Category`  VARCHAR(50),
        `Product Name`  VARCHAR(255),
        `Sales`         DECIMAL(10,4),
        `Quantity`      INT,
        `Discount`      DECIMAL(5,4),
        `Profit`        DECIMAL(10,4),
        PRIMARY KEY (`Row ID`)
    )
""")

insert_sql = """
    INSERT INTO staging_superstore VALUES
    (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""

# 21 columns — must match CREATE TABLE order exactly
rows = []
for _, row in df.iterrows():
    rows.append((
        int(row["Row ID"]),          # 1  Row ID
        row["Order ID"],             # 2  Order ID
        pd.to_datetime(row["Order Date"]).date(),  # 3  Order Date
        pd.to_datetime(row["Ship Date"]).date(),   # 4  Ship Date
        row["Ship Mode"],            # 5  Ship Mode
        row["Customer ID"],          # 6  Customer ID
        row["Customer Name"],        # 7  Customer Name
        row["Segment"],              # 8  Segment
        row["Country"],              # 9  Country
        row["City"],                 # 10 City
        row["State"],                # 11 State  ← was missing
        str(row["Postal Code"]),     # 12 Postal Code
        row["Region"],               # 13 Region
        row["Product ID"],           # 14 Product ID
        row["Category"],             # 15 Category
        row["Sub-Category"],         # 16 Sub-Category
        row["Product Name"],         # 17 Product Name
        float(row["Sales"]),         # 18 Sales
        int(row["Quantity"]),        # 19 Quantity
        float(row["Discount"]),      # 20 Discount
        float(row["Profit"]),        # 21 Profit
    ))

cursor.executemany(insert_sql, rows)
conn.commit()

print(f"✅ Loaded {len(rows)} rows into staging_superstore")
cursor.close()
conn.close()