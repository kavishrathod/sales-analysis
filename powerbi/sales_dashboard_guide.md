# Power BI Dashboard — Step-by-Step Build Guide
## Sales Performance Analysis | Superstore Dataset

---

## Step 1: Load the Data

1. Open **Power BI Desktop**
2. Click **Home → Get Data → Text/CSV**
3. Select `data/superstore_sales.csv`
4. Click **Transform Data** to open Power Query Editor

---

## Step 2: Data Transformation (Power Query)

In Power Query Editor, apply the following:

| Action | Steps |
|---|---|
| Check column types | Ensure `Order Date` and `Ship Date` are `Date` type |
| Remove duplicates | Right-click `Order ID` → Remove Duplicates |
| Rename columns | Remove spaces if needed (e.g., `Sub-Category` → `SubCategory`) |
| Add `Year` column | Add Column → Custom Column → `Date.Year([Order Date])` |
| Add `Month` column | Add Column → Custom Column → `Date.Month([Order Date])` |
| Add `Month Name` | Add Column → Custom Column → `Date.MonthName([Order Date])` |

Click **Close & Apply**.

---

## Step 3: Create DAX Measures

Go to **Modeling → New Measure** and add each of the following:

```dax
Total Sales = SUM(superstore[Sales])

Total Profit = SUM(superstore[Profit])

Total Orders = DISTINCTCOUNT(superstore[Order ID])

Avg Discount = AVERAGE(superstore[Discount])

Profit Margin % = DIVIDE(SUM(superstore[Profit]), SUM(superstore[Sales])) * 100
```

---

## Step 4: Build the Dashboard

### 🔷 Visual 1 — KPI Cards (Top Row)
- Insert 4 **Card** visuals
- Assign one measure to each: `Total Sales`, `Total Profit`, `Total Orders`, `Avg Discount`
- Format: Bold font, colored title bar (blue/green/orange/gray)

---

### 🔷 Visual 2 — Monthly Sales Trend (Line Chart)
- Chart type: **Line Chart**
- X-axis: `Order Date` (Month hierarchy or `Month Name`)
- Y-axis: `Total Sales`
- Secondary Y-axis (optional): `Total Profit`
- Title: *Monthly Sales & Profit Trend*
- Enable data labels

---

### 🔷 Visual 3 — Top 10 Sub-Categories by Sales (Bar Chart)
- Chart type: **Clustered Bar Chart**
- Y-axis: `Sub-Category`
- X-axis: `Total Sales`
- Add Top N filter: Visual-level filter → `Sub-Category` → Top 10 by `Total Sales`
- Title: *Top 10 Sub-Categories by Revenue*
- Color: Use a gradient (light to dark blue)

---

### 🔷 Visual 4 — Sales by Region (Map or Bar Chart)

**Option A — Map Visual:**
- Chart type: **Filled Map** or **Map**
- Location: `State`
- Color Saturation: `Total Sales`
- Tooltips: Add `Total Profit` and `Total Orders`

**Option B — Clustered Bar Chart:**
- X-axis: `Region`
- Y-axis: `Total Sales` and `Total Profit` (clustered)
- Title: *Regional Sales Performance*

---

### 🔷 Visual 5 — Sales by Segment (Donut Chart)
- Chart type: **Donut Chart**
- Legend: `Segment`
- Values: `Total Sales`
- Title: *Revenue by Customer Segment*
- Enable percentage labels

---

### 🔷 Visual 6 (Bonus) — Category × Profit Margin Matrix
- Chart type: **Matrix**
- Rows: `Category`
- Columns: `Region`
- Values: `Profit Margin %`
- Format: Conditional formatting → Color scale (red = low, green = high)

---

## Step 5: Add Slicers (Filters)

Add the following **Slicer** visuals to make the dashboard interactive:

| Slicer | Field |
|---|---|
| Year | `Year` |
| Category | `Category` |
| Region | `Region` |
| Segment | `Segment` |

Place slicers in a top panel or sidebar. Use **Dropdown** style for space efficiency.

---

## Step 6: Design & Formatting Tips

- **Theme:** Use a built-in theme (View → Themes → *Executive* or *Frontier*)
- **Color palette:** Stick to 2–3 colors (e.g., blue `#2E75B6`, green `#70AD47`, orange `#ED7D31`)
- **Font:** Segoe UI, size 10–12 for labels
- **Title bar:** Add a text box at the top with the dashboard title
- **Borders:** Add subtle borders to cards (Format → Border)
- **Alignment:** Use View → Snap to Grid for clean alignment

---

## Step 7: Publish (Optional)

1. **Save** the `.pbix` file in the `powerbi/` folder
2. Click **Home → Publish → My Workspace**
3. Share the link or export as PDF: **File → Export → PDF**

---

## Dashboard Layout Preview

```
┌─────────────────────────────────────────────────────────┐
│        SALES PERFORMANCE ANALYSIS — SUPERSTORE          │
│        [Year ▾] [Category ▾] [Region ▾] [Segment ▾]    │
├──────────┬──────────┬──────────┬────────────────────────┤
│  $2.3M   │  $286K   │  9,994   │      11.6% Margin      │
│  Sales   │  Profit  │ Orders   │      Avg Discount      │
├──────────────────────────┬─────────────────────────────┤
│  Monthly Sales Trend     │  Sales by Segment (Donut)   │
│  (Line Chart)            │                             │
├──────────────────────────┼─────────────────────────────┤
│  Top Sub-Categories      │  Regional Performance       │
│  (Horizontal Bar)        │  (Map / Bar Chart)          │
├──────────────────────────┴─────────────────────────────┤
│  Category × Region Profit Matrix                        │
└─────────────────────────────────────────────────────────┘
```
