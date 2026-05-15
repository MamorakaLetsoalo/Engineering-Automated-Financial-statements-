# Power BI Setup Guide
## FinModel Pro — Connecting Power BI to Pipeline Exports

---

## Step 1: Download CSV Exports from Databricks

After running the pipeline, download these files from your Databricks
file browser at `/FileStore/finmodel_pro/exports/`:

- `historical_annual.csv`
- `annual_forecast.csv`
- `scenarios.csv`
- `monthly_forecast.csv`

Save them to a local folder, e.g. `C:\FinModelPro\exports\`

---

## Step 2: Open Power BI Desktop

1. Open **Power BI Desktop** (free download: powerbi.microsoft.com)
2. Click **Get Data** → **Text/CSV**
3. Import each CSV file above
4. Repeat for all 4 files

---

## Step 3: Build the Data Model

In **Model view**, create these relationships:

```
historical_annual[year] → annual_forecast[year]  (1:1)
annual_forecast[year]   → scenarios[year]         (1:many)
monthly_forecast        → (no relationship needed, standalone)
```

---

## Step 4: Create Measures (DAX)

In the **Data pane**, right-click a table → **New measure**:

```dax
-- Latest Revenue
Latest Revenue =
CALCULATE(
    MAX(annual_forecast[revenue]),
    annual_forecast[scenario] = "Base"
)

-- Revenue Growth YoY
Revenue Growth % =
DIVIDE(
    [Latest Revenue] - CALCULATE(MAX(historical_annual[revenue])),
    CALCULATE(MAX(historical_annual[revenue]))
)

-- EBITDA Margin (Forecast)
EBITDA Margin % =
DIVIDE(
    CALCULATE(SUM(annual_forecast[ebitda]), annual_forecast[scenario] = "Base"),
    CALCULATE(SUM(annual_forecast[revenue]), annual_forecast[scenario] = "Base")
)

-- FCF (Base scenario)
FCF Base =
CALCULATE(
    SUM(annual_forecast[fcf]),
    annual_forecast[scenario] = "Base"
)
```

---

## Step 5: Recommended Visuals

### Page 1 — Executive Dashboard
| Visual | Data | Config |
|--------|------|--------|
| Card | Latest Revenue | Format: Currency |
| Card | EBITDA Margin % | Format: % |
| Card | FCF Base | Format: Currency |
| Line chart | historical_annual[revenue] + annual_forecast[revenue] | X=year, Y=revenue |
| Column chart | annual_forecast[net_income] by scenario | X=scenario, Y=net_income |

### Page 2 — Scenario Comparison
| Visual | Data |
|--------|------|
| Table | scenarios[year, scenario, revenue, ebitda_margin, net_income, fcf] |
| Clustered bar | scenarios[revenue] grouped by scenario |
| Line | scenarios[ebitda_margin] by year, colored by scenario |

### Page 3 — Monthly Trend
| Visual | Data |
|--------|------|
| Line chart | monthly_forecast[revenue, revenue_lower, revenue_upper] by date |
| Area chart | monthly_forecast[fcf] by date |

---

## Step 6: Scenario Slicer

1. Add a **Slicer** visual
2. Field: `scenarios[scenario]`
3. Style: **Dropdown** or **Tile**
4. All scenario-based visuals will filter when user selects Base/Bull/Bear

---

## Step 7: Publish (Optional)

To share your report:
1. **Publish** to Power BI Service (requires free account)
2. Or export as **PDF** (File → Export → PDF)
3. Or share the `.pbix` file directly

---

## Notes

- Power BI free tier **cannot** connect live to Databricks Delta tables
  (requires Power BI Premium / Databricks premium connector)
- The CSV import approach works completely free
- Refresh data: re-download CSVs from Databricks after each pipeline run,
  then click **Refresh** in Power BI Desktop
