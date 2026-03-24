"""
data_quality_checks.py
======================
Automated Data Quality checks for the Superstore Sales dataset.
Runs 12 checks across completeness, validity, consistency & uniqueness.
Logs results to: data_engineering/quality/dq_report.csv

Usage:
    python data_engineering/quality/data_quality_checks.py
"""

import pandas as pd
import numpy as np
import os
import json
from datetime import datetime

# ── Config ──────────────────────────────────────────────────
DATA_PATH   = "data/superstore_sales.csv"
REPORT_DIR  = "data_engineering/quality"
REPORT_PATH = f"{REPORT_DIR}/dq_report.csv"
LOG_PATH    = f"{REPORT_DIR}/dq_log.json"

os.makedirs(REPORT_DIR, exist_ok=True)

# ── Load Data ────────────────────────────────────────────────
print("=" * 58)
print("  SUPERSTORE — DATA QUALITY CHECKS")
print(f"  Run at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 58)

df = pd.read_csv(DATA_PATH, encoding="latin-1")
df["Order Date"] = pd.to_datetime(df["Order Date"])
df["Ship Date"]  = pd.to_datetime(df["Ship Date"])

results = []

def log_check(check_id, category, check_name, column, status, records_failed, total_records, details=""):
    emoji  = "✅" if status == "PASS" else ("⚠️ " if status == "WARN" else "❌")
    pct    = round((records_failed / total_records) * 100, 2) if total_records > 0 else 0
    print(f"  {emoji} [{check_id}] {check_name:<40} → {status}  ({records_failed} / {total_records} rows | {pct}%)")
    if details:
        print(f"       └─ {details}")
    results.append({
        "check_id"       : check_id,
        "category"       : category,
        "check_name"     : check_name,
        "column"         : column,
        "status"         : status,
        "records_failed" : records_failed,
        "total_records"  : total_records,
        "fail_pct"       : pct,
        "details"        : details,
        "run_timestamp"  : datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })

total = len(df)

# ── 1. COMPLETENESS ──────────────────────────────────────────
print("\n── 1. COMPLETENESS ──────────────────────────────────")

critical_cols = ["Order ID", "Order Date", "Customer ID", "Product ID", "Sales", "Profit", "Quantity"]
for col in critical_cols:
    nulls = df[col].isnull().sum()
    status = "PASS" if nulls == 0 else ("WARN" if nulls / total < 0.05 else "FAIL")
    log_check("C01", "Completeness", f"No nulls in {col}", col, status, nulls, total)

# ── 2. VALIDITY ──────────────────────────────────────────────
print("\n── 2. VALIDITY ──────────────────────────────────────")

# Sales must be > 0
neg_sales = (df["Sales"] <= 0).sum()
log_check("V01", "Validity", "Sales > 0", "Sales", "PASS" if neg_sales == 0 else "FAIL", neg_sales, total)

# Quantity must be positive integer
neg_qty = (df["Quantity"] <= 0).sum()
log_check("V02", "Validity", "Quantity > 0", "Quantity", "PASS" if neg_qty == 0 else "FAIL", neg_qty, total)

# Discount must be between 0 and 1
bad_disc = ((df["Discount"] < 0) | (df["Discount"] > 1)).sum()
log_check("V03", "Validity", "Discount in [0, 1]", "Discount",
          "PASS" if bad_disc == 0 else "FAIL", bad_disc, total)

# Valid segments
valid_segments = {"Consumer", "Corporate", "Home Office"}
invalid_seg = (~df["Segment"].isin(valid_segments)).sum()
log_check("V04", "Validity", "Segment in known values", "Segment",
          "PASS" if invalid_seg == 0 else "FAIL", invalid_seg, total,
          f"Expected: {valid_segments}")

# Valid regions
valid_regions = {"West", "East", "Central", "South"}
invalid_reg = (~df["Region"].isin(valid_regions)).sum()
log_check("V05", "Validity", "Region in known values", "Region",
          "PASS" if invalid_reg == 0 else "FAIL", invalid_reg, total,
          f"Expected: {valid_regions}")

# Valid categories
valid_cats = {"Technology", "Office Supplies", "Furniture"}
invalid_cat = (~df["Category"].isin(valid_cats)).sum()
log_check("V06", "Validity", "Category in known values", "Category",
          "PASS" if invalid_cat == 0 else "FAIL", invalid_cat, total)

# ── 3. CONSISTENCY ───────────────────────────────────────────
print("\n── 3. CONSISTENCY ───────────────────────────────────")

# Ship Date must be >= Order Date
bad_ship = (df["Ship Date"] < df["Order Date"]).sum()
log_check("CS01", "Consistency", "Ship Date >= Order Date", "Ship Date",
          "PASS" if bad_ship == 0 else "FAIL", bad_ship, total)

# Order date within expected range (2014–2018)
bad_year = ((df["Order Date"].dt.year < 2014) | (df["Order Date"].dt.year > 2018)).sum()
log_check("CS02", "Consistency", "Order Date in 2014–2018", "Order Date",
          "PASS" if bad_year == 0 else "WARN", bad_year, total)

# High discount with positive profit — flag as anomaly for review
high_disc_pos_profit = ((df["Discount"] > 0.5) & (df["Profit"] > 0)).sum()
log_check("CS03", "Consistency", "High discount (>50%) with positive profit", "Discount/Profit",
          "PASS" if high_disc_pos_profit == 0 else "WARN", high_disc_pos_profit, total,
          "These rows may be data entry anomalies worth investigating")

# ── 4. UNIQUENESS ────────────────────────────────────────────
print("\n── 4. UNIQUENESS ────────────────────────────────────")

# Duplicate rows (all columns)
full_dupes = df.duplicated().sum()
log_check("U01", "Uniqueness", "No fully duplicate rows", "ALL",
          "PASS" if full_dupes == 0 else "FAIL", full_dupes, total)

# Duplicate (Order ID + Product ID) — should be unique per line item
order_product_dupes = df.duplicated(subset=["Order ID", "Product ID"]).sum()
log_check("U02", "Uniqueness", "Unique (Order ID + Product ID)", "Order ID / Product ID",
          "PASS" if order_product_dupes == 0 else "WARN", order_product_dupes, total,
          "Same product ordered twice in one order is unusual")

# ── 5. OUTLIER FLAGS ─────────────────────────────────────────
print("\n── 5. OUTLIER FLAGS (Informational) ─────────────────")

# Sales outliers using IQR
Q1, Q3 = df["Sales"].quantile(0.25), df["Sales"].quantile(0.75)
IQR = Q3 - Q1
sales_outliers = ((df["Sales"] < Q1 - 3*IQR) | (df["Sales"] > Q3 + 3*IQR)).sum()
log_check("O01", "Outliers", "Sales within 3×IQR bounds", "Sales",
          "PASS" if sales_outliers == 0 else "WARN", sales_outliers, total,
          f"IQR range: ${Q1:.2f} – ${Q3:.2f}")

# Profit outliers
Q1p, Q3p = df["Profit"].quantile(0.25), df["Profit"].quantile(0.75)
IQRp = Q3p - Q1p
profit_outliers = ((df["Profit"] < Q1p - 3*IQRp) | (df["Profit"] > Q3p + 3*IQRp)).sum()
log_check("O02", "Outliers", "Profit within 3×IQR bounds", "Profit",
          "PASS" if profit_outliers == 0 else "WARN", profit_outliers, total,
          f"IQR range: ${Q1p:.2f} – ${Q3p:.2f}")

# ── Summary ──────────────────────────────────────────────────
report_df = pd.DataFrame(results)
passed  = (report_df["status"] == "PASS").sum()
warned  = (report_df["status"] == "WARN").sum()
failed  = (report_df["status"] == "FAIL").sum()
total_checks = len(report_df)

print("\n" + "=" * 58)
print("  SUMMARY")
print("=" * 58)
print(f"  Total Checks : {total_checks}")
print(f"  ✅ PASS      : {passed}")
print(f"  ⚠️  WARN      : {warned}")
print(f"  ❌ FAIL      : {failed}")
print(f"  Overall Score: {round((passed / total_checks) * 100, 1)}%")
print("=" * 58)

# ── Save Reports ─────────────────────────────────────────────
report_df.to_csv(REPORT_PATH, index=False)
print(f"\n✅ Report saved → {REPORT_PATH}")

log_summary = {
    "run_timestamp" : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    "dataset_rows"  : total,
    "total_checks"  : total_checks,
    "passed"        : int(passed),
    "warned"        : int(warned),
    "failed"        : int(failed),
    "score_pct"     : round((passed / total_checks) * 100, 1)
}
with open(LOG_PATH, "w") as f:
    json.dump(log_summary, f, indent=2)
print(f"✅ Log saved    → {LOG_PATH}")
