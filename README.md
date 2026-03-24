# 📊 Sales Performance Analysis — SQL + Python EDA + Power BI + Data Engineering

> End-to-end sales analytics project on the Superstore Sales dataset.  
> Covers SQL querying, Python EDA, Power BI dashboard, star schema design, automated data quality checks, and an Apache Airflow pipeline DAG.

---

## 📁 Project Structure

```
sales-analysis/
│
├── data/
│   └── superstore_sales.csv              # Dataset (Kaggle Superstore)
│
├── sql/
│   ├── 01_basic_exploration.sql
│   ├── 02_revenue_analysis.sql
│   ├── 03_top_products.sql
│   ├── 04_customer_segments.sql
│   ├── 05_regional_analysis.sql
│   └── star_schema_ddl.sql               # ⭐ Star Schema warehouse design
│
├── notebooks/
│   └── eda_superstore.ipynb              # Jupyter EDA notebook (11 sections)
│
├── data_engineering/
│   ├── quality/
│   │   ├── data_quality_checks.py        # ⭐ 14 automated DQ checks
│   │   ├── dq_report.csv                 # Generated after running checks
│   │   └── dq_log.json                   # Run summary log
│   ├── reports/                          # Auto-generated CSVs from pipeline
│   └── airflow/
│       └── dags/
│           └── sales_pipeline_dag.py     # ⭐ Airflow DAG (DQ → Load → Report)
│
├── powerbi/
│   └── sales_dashboard_guide.md          # Step-by-step Power BI build guide
│
├── docs/
│   └── insights_summary.md               # Key business insights
│
├── requirements.txt
├── .gitignore
├── LICENSE
└── README.md
```

---

## 🎯 Objectives

- Analyze revenue trends, top products, and customer segments using **MySQL**
- Perform exploratory data analysis (EDA) in a **Jupyter Notebook**
- Design a **Star Schema** (fact + 5 dimension tables) for warehouse modeling
- Run automated **Data Quality checks** before any data is loaded
- Orchestrate the pipeline with an **Apache Airflow DAG**
- Visualize insights in a **Power BI dashboard**

---

## 🗃️ Dataset

**Source:** [Kaggle — Sample Superstore Sales Dataset](https://www.kaggle.com/datasets/vivek468/superstore-dataset-final)

| Column | Description |
|---|---|
| Order ID / Date | Order identifier and date |
| Customer ID / Segment | Customer details (Consumer / Corporate / Home Office) |
| Region / State / City | Geographic data |
| Category / Sub-Category | Product classification |
| Sales / Profit / Discount | Financial measures |
| Quantity / Ship Mode | Order details |

---

## 🗄️ SQL Analysis (MySQL)

**18 queries across 6 files**  
Concepts: `GROUP BY`, Aggregations, `JOIN`, Subqueries, `HAVING`, `CASE WHEN`, Stored Procedures

| File | Description |
|---|---|
| `01_basic_exploration.sql` | Row count, nulls, date range, dataset overview |
| `02_revenue_analysis.sql` | Monthly/yearly revenue, profit margins, discount impact |
| `03_top_products.sql` | Top/bottom products and sub-categories by revenue and profit |
| `04_customer_segments.sql` | Segment revenue, top customers, loyalty analysis |
| `05_regional_analysis.sql` | Region, state, city-level performance + shipping |
| `star_schema_ddl.sql` | ⭐ Full star schema DDL — fact + 5 dim tables + load queries |

---

## 📐 Star Schema Design

```
                    ┌──────────────┐
                    │  dim_date    │
                    │  (date_key)  │
                    └──────┬───────┘
                           │
┌──────────────┐    ┌──────┴───────┐    ┌──────────────┐
│ dim_customer │────│  fact_sales  │────│  dim_product │
│              │    │              │    │              │
│ customer_key │    │ order_date_key    │ product_key  │
│ customer_id  │    │ ship_date_key │   │ product_id   │
│ segment      │    │ customer_key │    │ category     │
└──────────────┘    │ product_key  │    │ sub_category │
                    │ location_key │    └──────────────┘
┌──────────────┐    │ shipping_key │    ┌──────────────┐
│ dim_location │────│              │────│ dim_shipping │
│              │    │ sales        │    │              │
│ city / state │    │ profit       │    │ ship_mode    │
│ region       │    │ discount     │    └──────────────┘
└──────────────┘    │ quantity     │
                    └──────────────┘
```

---

## 🔍 Data Quality Checks

**Script:** `data_engineering/quality/data_quality_checks.py`  
**Output:** `dq_report.csv` + `dq_log.json`

Runs **14 automated checks** across 4 dimensions:

| Category | Check |
|---|---|
| Completeness | Null checks on all critical columns |
| Validity | Sales > 0, Quantity > 0, Discount in [0,1], valid Segment/Region/Category |
| Consistency | Ship Date ≥ Order Date, dates in expected range, high-discount anomalies |
| Uniqueness | No duplicate rows, unique (Order ID + Product ID) |
| Outliers | IQR-based outlier flags on Sales and Profit |

Pipeline stops automatically if any `FAIL` check is detected.

```bash
python data_engineering/quality/data_quality_checks.py
```

---

## 🔄 Airflow Pipeline

**DAG:** `data_engineering/airflow/dags/sales_pipeline_dag.py`  
**Schedule:** Daily at 6:00 AM

```
data_quality_check → load_to_mysql → run_sql_reports → notify_completion
```

| Task | Description |
|---|---|
| `data_quality_check` | Validates source CSV; pushes DQ summary via XCom |
| `load_to_mysql` | Loads clean data into MySQL staging table (INSERT IGNORE) |
| `run_sql_reports` | Runs 3 reporting queries, exports CSVs to `reports/` |
| `notify_completion` | Logs final pipeline summary (extendable to Slack/email) |

> **Note:** Airflow credentials are stored as Airflow Variables — never hardcoded.

---

## 📊 Power BI Dashboard

5+ visuals: KPI Cards · Monthly Trend · Top Sub-Categories · Regional Map · Segment Donut · Category Matrix  
See [`powerbi/sales_dashboard_guide.md`](powerbi/sales_dashboard_guide.md) for full build steps including DAX measures.

---

## 🐍 Python EDA (Jupyter Notebook)

**Notebook:** `notebooks/eda_superstore.ipynb`

11 sections covering: data loading, quality checks, distributions, monthly trends, category analysis, segment analysis, regional performance, discount impact, and correlation heatmap.

```bash
pip install -r requirements.txt
jupyter notebook notebooks/eda_superstore.ipynb
```

---

## 💡 Key Insights

1. **Technology** is the highest-revenue and highest-profit category
2. **Tables & Bookcases** consistently produce negative profit — high discount risk
3. **Q4 (Nov–Dec)** sees the strongest sales spike across all years
4. **Consumer segment** accounts for ~50% of total revenue
5. **West region** leads in sales; **South** has the highest avg discount
6. Discounts above **30%** almost always result in a loss-making transaction

---

## 🛠️ Tech Stack

![MySQL](https://img.shields.io/badge/MySQL-Database-blue)
![Python](https://img.shields.io/badge/Python-3.10+-yellow)
![Jupyter](https://img.shields.io/badge/Jupyter-Notebook-orange)
![Airflow](https://img.shields.io/badge/Apache_Airflow-Pipeline-017CEE)
![Power BI](https://img.shields.io/badge/PowerBI-Dashboard-F2C811)
![Pandas](https://img.shields.io/badge/Pandas-EDA-green)

---

## 🚀 How to Run

```bash
# 1. Clone repo
git clone https://github.com/your-username/sales-analysis.git
cd sales-analysis

# 2. Download dataset from Kaggle → place at data/superstore_sales.csv

# 3. Install Python dependencies
pip install -r requirements.txt

# 4. Run Data Quality checks
python data_engineering/quality/data_quality_checks.py

# 5. Load data into MySQL (run star_schema_ddl.sql in MySQL Workbench)
# Then run SQL files in sql/ folder

# 6. Open EDA notebook
jupyter notebook notebooks/eda_superstore.ipynb

# 7. (Optional) Set up Airflow and copy DAG
cp data_engineering/airflow/dags/sales_pipeline_dag.py ~/airflow/dags/
airflow dags trigger superstore_sales_pipeline
```

---

## 👤 Author

**Kavish Rathod**  
B.E. Electronics & Telecommunications | Aspiring Data Analyst / Data Engineer  
[LinkedIn](https://linkedin.com/in/your-profile) • [GitHub](https://github.com/your-username)

---

## 📄 License

MIT License — see [LICENSE](LICENSE)
