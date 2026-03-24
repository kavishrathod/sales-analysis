"""
sales_pipeline_dag.py
=====================
Apache Airflow DAG — Superstore Sales Analytics Pipeline

Schedule : Daily at 6:00 AM
Pipeline :
    1. data_quality_check   → validate source CSV
    2. load_to_mysql        → load clean data into MySQL star schema
    3. run_sql_reports      → execute reporting queries, export to CSV
    4. notify_completion    → log pipeline summary

Setup:
    pip install apache-airflow apache-airflow-providers-mysql pandas
    Place this file in: ~/airflow/dags/
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python  import PythonOperator
from airflow.operators.bash    import BashOperator
from airflow.models            import Variable
from airflow.utils.dates       import days_ago

import pandas as pd
import mysql.connector
import json
import os
import logging

logger = logging.getLogger(__name__)

# ── Default Args ─────────────────────────────────────────────
default_args = {
    "owner"           : "kavish_rathod",
    "depends_on_past" : False,
    "email_on_failure": False,
    "email_on_retry"  : False,
    "retries"         : 1,
    "retry_delay"     : timedelta(minutes=5),
}

# ── DAG Definition ───────────────────────────────────────────
with DAG(
    dag_id              = "superstore_sales_pipeline",
    default_args        = default_args,
    description         = "Daily Superstore sales data quality, load & reporting pipeline",
    schedule_interval   = "0 6 * * *",       # Every day at 6:00 AM
    start_date          = days_ago(1),
    catchup             = False,
    max_active_runs     = 1,
    tags                = ["sales", "data-engineering", "mysql", "superstore"],
) as dag:

    # ── Task 1: Data Quality Check ───────────────────────────
    def run_data_quality(**context):
        """
        Runs all DQ checks on the source CSV.
        Pushes pass/fail summary to XCom for downstream tasks.
        Raises exception if any FAIL checks found — stops pipeline.
        """
        DATA_PATH  = Variable.get("superstore_data_path", default_var="data/superstore_sales.csv")
        REPORT_DIR = Variable.get("superstore_report_dir", default_var="data_engineering/quality")

        df = pd.read_csv(DATA_PATH, encoding="latin-1")
        df["Order Date"] = pd.to_datetime(df["Order Date"])
        df["Ship Date"]  = pd.to_datetime(df["Ship Date"])

        checks_failed = []

        # Null checks on critical columns
        for col in ["Order ID", "Customer ID", "Product ID", "Sales", "Profit", "Quantity"]:
            nulls = int(df[col].isnull().sum())
            if nulls > 0:
                checks_failed.append(f"NULL values in {col}: {nulls}")

        # Sales must be > 0
        neg_sales = int((df["Sales"] <= 0).sum())
        if neg_sales > 0:
            checks_failed.append(f"Sales <= 0: {neg_sales} rows")

        # Ship date must be >= Order date
        bad_ship = int((df["Ship Date"] < df["Order Date"]).sum())
        if bad_ship > 0:
            checks_failed.append(f"Ship Date < Order Date: {bad_ship} rows")

        # Discount range
        bad_disc = int(((df["Discount"] < 0) | (df["Discount"] > 1)).sum())
        if bad_disc > 0:
            checks_failed.append(f"Discount out of [0,1]: {bad_disc} rows")

        summary = {
            "total_rows"    : len(df),
            "checks_failed" : checks_failed,
            "run_timestamp" : datetime.now().isoformat(),
        }

        os.makedirs(REPORT_DIR, exist_ok=True)
        with open(f"{REPORT_DIR}/dq_log.json", "w") as f:
            json.dump(summary, f, indent=2)

        # Push summary to XCom
        context["ti"].xcom_push(key="dq_summary", value=summary)

        if checks_failed:
            raise ValueError(f"❌ DQ FAILED — {len(checks_failed)} issue(s):\n" + "\n".join(checks_failed))

        logger.info(f"✅ DQ passed — {len(df):,} rows validated")

    task_dq = PythonOperator(
        task_id         = "data_quality_check",
        python_callable = run_data_quality,
        provide_context = True,
    )

    # ── Task 2: Load to MySQL ────────────────────────────────
    def load_to_mysql(**context):
        """
        Loads cleaned Superstore data into MySQL staging table.
        Uses INSERT IGNORE to handle reruns without duplicates.
        """
        DATA_PATH = Variable.get("superstore_data_path", default_var="data/superstore_sales.csv")

        # MySQL connection — store credentials in Airflow Variables (not hardcoded)
        conn = mysql.connector.connect(
            host     = Variable.get("mysql_host",     default_var="localhost"),
            port     = int(Variable.get("mysql_port", default_var="3306")),
            user     = Variable.get("mysql_user",     default_var="root"),
            password = Variable.get("mysql_password", default_var=""),
            database = Variable.get("mysql_db",       default_var="superstore_dw"),
        )
        cursor = conn.cursor()

        df = pd.read_csv(DATA_PATH, encoding="latin-1")

        # Create staging table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS staging_superstore (
                row_id          INT,
                order_id        VARCHAR(30),
                order_date      DATE,
                ship_date       DATE,
                ship_mode       VARCHAR(30),
                customer_id     VARCHAR(20),
                customer_name   VARCHAR(100),
                segment         VARCHAR(30),
                country         VARCHAR(50),
                city            VARCHAR(100),
                state           VARCHAR(100),
                postal_code     VARCHAR(10),
                region          VARCHAR(20),
                product_id      VARCHAR(30),
                category        VARCHAR(50),
                sub_category    VARCHAR(50),
                product_name    VARCHAR(255),
                sales           DECIMAL(10,4),
                quantity        INT,
                discount        DECIMAL(5,4),
                profit          DECIMAL(10,4),
                PRIMARY KEY (row_id)
            )
        """)

        insert_sql = """
            INSERT IGNORE INTO staging_superstore VALUES (
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
            )
        """

        rows = []
        for _, row in df.iterrows():
            rows.append((
                row.get("Row ID"),
                row.get("Order ID"),
                pd.to_datetime(row.get("Order Date")).date(),
                pd.to_datetime(row.get("Ship Date")).date(),
                row.get("Ship Mode"),
                row.get("Customer ID"),
                row.get("Customer Name"),
                row.get("Segment"),
                row.get("Country"),
                row.get("City"),
                row.get("State"),
                str(row.get("Postal Code", "")),
                row.get("Region"),
                row.get("Product ID"),
                row.get("Category"),
                row.get("Sub-Category"),
                row.get("Product Name"),
                float(row.get("Sales", 0)),
                int(row.get("Quantity", 0)),
                float(row.get("Discount", 0)),
                float(row.get("Profit", 0)),
            ))

        cursor.executemany(insert_sql, rows)
        conn.commit()

        rows_loaded = cursor.rowcount
        logger.info(f"✅ Loaded {rows_loaded:,} rows into staging_superstore")

        context["ti"].xcom_push(key="rows_loaded", value=rows_loaded)

        cursor.close()
        conn.close()

    task_load = PythonOperator(
        task_id         = "load_to_mysql",
        python_callable = load_to_mysql,
        provide_context = True,
    )

    # ── Task 3: Run SQL Reports ──────────────────────────────
    def run_sql_reports(**context):
        """
        Runs key reporting queries against MySQL.
        Exports results to CSV in data_engineering/reports/
        """
        REPORT_DIR = Variable.get("superstore_report_dir", default_var="data_engineering/quality")
        reports_dir = REPORT_DIR.replace("quality", "reports")
        os.makedirs(reports_dir, exist_ok=True)

        conn = mysql.connector.connect(
            host     = Variable.get("mysql_host",     default_var="localhost"),
            port     = int(Variable.get("mysql_port", default_var="3306")),
            user     = Variable.get("mysql_user",     default_var="root"),
            password = Variable.get("mysql_password", default_var=""),
            database = Variable.get("mysql_db",       default_var="superstore_dw"),
        )

        queries = {
            "monthly_revenue": """
                SELECT
                    YEAR(order_date)   AS year,
                    MONTH(order_date)  AS month,
                    ROUND(SUM(sales), 2)  AS total_sales,
                    ROUND(SUM(profit), 2) AS total_profit
                FROM staging_superstore
                GROUP BY year, month
                ORDER BY year, month
            """,
            "top_10_products": """
                SELECT
                    product_name, category,
                    ROUND(SUM(sales), 2)  AS total_sales,
                    ROUND(SUM(profit), 2) AS total_profit
                FROM staging_superstore
                GROUP BY product_name, category
                ORDER BY total_sales DESC
                LIMIT 10
            """,
            "region_summary": """
                SELECT
                    region,
                    ROUND(SUM(sales), 2)   AS total_sales,
                    ROUND(SUM(profit), 2)  AS total_profit,
                    COUNT(DISTINCT order_id) AS orders
                FROM staging_superstore
                GROUP BY region
                ORDER BY total_sales DESC
            """,
        }

        for report_name, sql in queries.items():
            df_result = pd.read_sql(sql, conn)
            out_path  = f"{reports_dir}/{report_name}.csv"
            df_result.to_csv(out_path, index=False)
            logger.info(f"✅ Report saved: {out_path} ({len(df_result)} rows)")

        conn.close()

    task_reports = PythonOperator(
        task_id         = "run_sql_reports",
        python_callable = run_sql_reports,
        provide_context = True,
    )

    # ── Task 4: Notify Completion ────────────────────────────
    def notify_completion(**context):
        """
        Logs final pipeline summary. 
        Extend this to send Slack/email alerts in production.
        """
        dq_summary  = context["ti"].xcom_pull(task_ids="data_quality_check", key="dq_summary")
        rows_loaded = context["ti"].xcom_pull(task_ids="load_to_mysql",       key="rows_loaded")

        summary = {
            "pipeline"    : "superstore_sales_pipeline",
            "status"      : "SUCCESS",
            "run_date"    : datetime.now().isoformat(),
            "rows_loaded" : rows_loaded,
            "dq_rows"     : dq_summary.get("total_rows") if dq_summary else "N/A",
        }

        logger.info("=" * 50)
        logger.info("  PIPELINE COMPLETE")
        for k, v in summary.items():
            logger.info(f"  {k:<15}: {v}")
        logger.info("=" * 50)

    task_notify = PythonOperator(
        task_id         = "notify_completion",
        python_callable = notify_completion,
        provide_context = True,
    )

    # ── DAG Dependencies (linear pipeline) ──────────────────
    #
    #   data_quality_check
    #          │
    #    load_to_mysql
    #          │
    #    run_sql_reports
    #          │
    #   notify_completion
    #
    task_dq >> task_load >> task_reports >> task_notify
