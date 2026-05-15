# Data Catalog & Data Model
## FinModel Pro — Automated Financial Modelling Platform

---

## Overview

This catalog documents every table, column, business rule, and lineage
relationship in the FinModel Pro data platform. It is the authoritative
reference for analysts, data engineers, and stakeholders.

**Last updated:** 2024-01-15
**Owner:** Data Engineering Team
**Review cycle:** Quarterly or on schema change

---

## Data Lifecycle

```
External Sources                  Databricks Delta Lake              Serving
─────────────────     ──────────────────────────────────────     ─────────────
Synthetic Generator ──▶ BRONZE (raw)  ──▶  SILVER (clean)  ──▶  Streamlit App
FRED API           ──▶  (immutable)       (enriched KPIs)   ──▶  Power BI
                                               │
                                               ▼
                                         GOLD (forecast)    ──▶  Excel Export
                                         (statements)       ──▶  CSV exports
```

**Retention policy:**
| Layer    | Retention | Reason                                  |
|----------|-----------|-----------------------------------------|
| Bronze   | 7 years   | Regulatory / audit requirement          |
| Silver   | 5 years   | Historical analysis                     |
| Gold     | 3 years   | Forecast superseded by new model runs   |
| Quarantine | 1 year  | Debug failed records                    |
| Audit logs | 7 years | Compliance                             |

---

## Bronze Layer — Raw Ingestion

### `bronze_company_financials`

**Path:** `/FileStore/finmodel_pro/bronze/company_financials`
**Format:** Delta Lake
**Partition:** `year`, `month`
**Write mode:** Append (each run adds a new batch)
**Source:** Synthetic generator / future: ERP system

| Column | Type | Nullable | Description | Business Rule |
|--------|------|----------|-------------|---------------|
| `date` | DATE | No | First day of the financial month | Always 1st of month (monthly granularity) |
| `company` | STRING | No | Company identifier | Always "AcmeCorp" in v1 |
| `revenue` | DOUBLE | No | Total monthly revenue (ZAR) | Must be > 0 |
| `cogs` | DOUBLE | No | Cost of goods sold (ZAR) | Must be < revenue |
| `gross_profit` | DOUBLE | No | Revenue minus COGS (ZAR) | Enforced: gross_profit = revenue − cogs ±R1 |
| `opex` | DOUBLE | Yes | Operating expenses excl. COGS (ZAR) | |
| `rd_expense` | DOUBLE | Yes | Research & development costs (ZAR) | |
| `sga_expense` | DOUBLE | Yes | Selling, general & administrative (ZAR) | |
| `ebitda` | DOUBLE | Yes | Earnings before interest, tax, D&A (ZAR) | |
| `depreciation` | DOUBLE | Yes | Non-cash D&A charge (ZAR) | |
| `ebit` | DOUBLE | Yes | Operating profit (ZAR) | |
| `interest_expense` | DOUBLE | Yes | Debt servicing cost (ZAR) | |
| `ebt` | DOUBLE | Yes | Earnings before tax (ZAR) | |
| `tax_expense` | DOUBLE | Yes | Income tax at 28% CIT (ZAR) | SA corporate tax rate |
| `net_income` | DOUBLE | Yes | Bottom-line profit (ZAR) | |
| `accounts_receivable` | DOUBLE | Yes | Debtors balance (ZAR) | ~DSO 45 days |
| `inventory` | DOUBLE | Yes | Stock on hand (ZAR) | ~DIO 60 days |
| `accounts_payable` | DOUBLE | Yes | Creditors balance (ZAR) | ~DPO 35 days |
| `capex` | DOUBLE | Yes | Capital expenditure (ZAR) | ~5% of revenue |
| `customers` | LONG | Yes | Active customer count (EOMonth) | |
| `avg_revenue_per_user` | DOUBLE | Yes | Revenue / customers (ZAR) | |
| `churn_rate` | DOUBLE | Yes | Monthly churn (0–1) | Must be in [0, 1] |
| `_ingested_at` | TIMESTAMP | No | Ingestion timestamp (UTC) | System-generated |
| `_run_id` | STRING | No | Pipeline run identifier | Format: run_YYYYMMDD_HHMMSS |
| `_source` | STRING | No | Data source identifier | "synthetic" or "fred_api" |
| `_schema_version` | STRING | No | Schema version at ingest time | e.g. "v1.2" |

---

### `bronze_macro_indicators`

**Path:** `/FileStore/finmodel_pro/bronze/macro_indicators`
**Source:** FRED API (St. Louis Fed) or synthetic fallback

| Column | Type | Description | FRED Series |
|--------|------|-------------|-------------|
| `date` | DATE | First of month | — |
| `inflation_rate` | DOUBLE | YoY CPI change (%) | CPIAUCSL |
| `interest_rate` | DOUBLE | Repo/fed funds rate (%) | FEDFUNDS |
| `gdp_growth` | DOUBLE | Real GDP growth (%) | A191RL1Q225SBEA |
| `usd_zar` | DOUBLE | USD/ZAR exchange rate | — |
| `unemployment` | DOUBLE | Unemployment rate (%) | UNRATE |

---

## Silver Layer — Cleaned & Enriched

### `silver_monthly_financials`

**Path:** `/FileStore/finmodel_pro/silver/monthly_financials`
**Partition:** `year`
**Lineage:** bronze_company_financials + bronze_macro_indicators
**Write mode:** Overwrite (idempotent — rerunning produces same result)

**Added columns (derived in silver transform):**

| Column | Formula | Description |
|--------|---------|-------------|
| `year` | `YEAR(date)` | Calendar year |
| `month` | `MONTH(date)` | Calendar month |
| `quarter` | `QUARTER(date)` | Fiscal quarter |
| `gross_margin` | `gross_profit / revenue` | Gross margin ratio |
| `ebitda_margin` | `ebitda / revenue` | EBITDA margin ratio |
| `ebit_margin` | `ebit / revenue` | EBIT margin ratio |
| `net_margin` | `net_income / revenue` | Net profit margin |
| `cogs_ratio` | `cogs / revenue` | Cost of sales ratio |
| `opex_ratio` | `opex / revenue` | Operating leverage ratio |
| `working_capital` | `AR + Inv − AP` | Net working capital |
| `dso` | `AR / revenue × 30` | Days sales outstanding |
| `dio` | `Inv / cogs × 30` | Days inventory outstanding |
| `dpo` | `AP / cogs × 30` | Days payable outstanding |
| `cash_conversion_cycle` | `DSO + DIO − DPO` | Efficiency metric |
| `revenue_mom` | `(rev − lag1_rev) / lag1_rev` | Month-over-month growth |
| `revenue_yoy` | `(rev − lag12_rev) / lag12_rev` | Year-over-year growth |
| `revenue_ttm` | `SUM(rev, 12 months rolling)` | Trailing 12-month revenue |
| `real_revenue_growth` | `revenue_yoy − inflation/100` | Inflation-adjusted growth |
| `interest_coverage` | `ebit / interest_expense` | Debt serviceability |
| `revenue_per_customer` | `revenue / customers` | ARPU |

---

### `silver_data_quality_log`

Records every DQ check performed per pipeline run. Query this to audit
data quality trends over time.

| Column | Description |
|--------|-------------|
| `run_id` | Which pipeline run performed the check |
| `table_name` | Which table was checked |
| `check_name` | Name of the specific check |
| `check_type` | null_check \| duplicate \| range \| business_rule \| freshness \| referential |
| `rows_checked` | Total rows evaluated |
| `rows_failed` | Rows that failed the check |
| `failure_pct` | Failed / total |
| `threshold` | Acceptable failure rate |
| `passed` | Boolean result |
| `details` | Human-readable failure description |

---

## Gold Layer — Forecasts & Financial Statements

### `gold_annual_forecast`

One row per (year, company, scenario). Contains forecast P&L + cash flow
for all three scenarios (Base / Bull / Bear).

**Key design decision:** Scenarios are stored as rows (not columns) so
Power BI / Streamlit can filter by scenario without pivoting.

### `gold_income_statement`

| Row group | Lines included |
|-----------|----------------|
| Revenue | Revenue, COGS |
| Gross profit | Gross profit, Gross margin % |
| Operating costs | OpEx, R&D, SG&A |
| EBITDA | EBITDA, EBITDA margin % |
| Below EBITDA | D&A, EBIT, Interest, EBT, Tax |
| Bottom line | Net income, Net margin % |

### `gold_balance_sheet`

**Balancing identity:** Total Assets = Total Liabilities + Total Equity
Validated in Notebook 04. Max tolerance: R10 (rounding).

| Section | Items |
|---------|-------|
| Current Assets | Cash, AR, Inventory |
| Fixed Assets | Net PP&E |
| Current Liabilities | Accounts Payable |
| Long-term Liabilities | Long-term Debt |
| Equity | Paid-in Capital, Retained Earnings |

### `gold_cash_flow_statement`

**Method:** Indirect (starts from net income)

| Section | Items |
|---------|-------|
| CFO | Net income, D&A add-back, ΔAR, ΔInventory, ΔAP |
| CFI | Capital expenditures |
| CFF | Debt changes, Dividends paid |
| Net | Net change in cash, Closing balance |

### `gold_audit_log`

Immutable event log. Every pipeline action is recorded here.
Never deleted. Used for compliance and debugging.

---

## Quarantine Table

**Path:** `/FileStore/finmodel_pro/quarantine/financials`

Records that failed hard DQ checks are moved here instead of being
silently dropped. Each quarantined record includes:
- `_quarantine_reason`: Which rule failed
- `_quarantined_at`: Timestamp
- `_run_id`: Which run quarantined it

**Process:** Analyst reviews quarantine weekly. Valid records can be
corrected and reingested. Invalid records are archived.

---

## Business Rules (Enforced in Code)

| Rule | Where enforced | Consequence of failure |
|------|---------------|------------------------|
| gross_profit = revenue − cogs ± R1 | DQ framework | Quarantine |
| revenue > 0 | DQ framework | Quarantine |
| churn_rate ∈ [0, 1] | DQ framework | DQ log warning |
| Assets = Liabilities + Equity ± R10 | Notebook 04 | Pipeline warning |
| Tax = max(EBT × 28%, 0) | Model config | Enforced in code |
| Dividend payout = 30% of net income | Model config | Configurable |
| Debt repayment = 5% p.a. | Model config | Configurable |
| Terminal growth < WACC | DCF engine | Runtime check |

---

## Lineage Diagram

```
[Synthetic Generator]           [FRED API]
        │                           │
        ▼                           ▼
bronze_company_financials   bronze_macro_indicators
        │                           │
        └───────────┬───────────────┘
                    ▼
             [DQ Framework]
                    │
          ┌─────────┴──────────┐
          ▼                    ▼
   clean records         quarantine table
          │
          ▼
  silver_monthly_financials ──▶ silver_annual_financials
          │
          ▼
  [Forecasting Engine — Prophet]
          │
          ▼
  gold_monthly_forecast ──▶ gold_annual_forecast ──▶ gold_scenario_forecasts
          │
          ▼
  [Statement Generator]
          │
  ┌───────┼───────────┐
  ▼       ▼           ▼
gold_    gold_      gold_
income_  balance_   cash_
stmt     sheet      flow
          │
          ▼
    [Export Layer]
     ┌────┴────┐
     ▼         ▼
  CSV files  Excel file
     │
     ▼
  Streamlit / Power BI
```

---

## Design Decisions & Tradeoffs

### Decision 1: Delta Lake over Parquet
**Why:** ACID transactions, schema enforcement, time travel (audit),
MERGE INTO for idempotent writes.
**Tradeoff:** Slightly higher storage overhead than raw Parquet.

### Decision 2: Medallion Architecture (Bronze/Silver/Gold)
**Why:** Separation of concerns. Bronze is immutable raw data.
Silver adds quality + enrichment. Gold is business-ready.
**Tradeoff:** More storage, but raw data is never lost.

### Decision 3: Row-per-scenario (not column-per-scenario)
**Why:** Enables simple filter() in BI tools. Adding a new scenario
requires no schema change.
**Tradeoff:** Dataset is 3× larger, but gold layer is small.

### Decision 4: Indirect method for cash flow
**Why:** Standard for FP&A teams. Directly auditable from net income.
**Tradeoff:** Requires accurate D&A and working capital data.

### Decision 5: Prophet over ARIMA for revenue forecasting
**Why:** Handles changepoints, annual seasonality, and external regressors
(macro) natively. More robust to structural breaks.
**Tradeoff:** Heavier dependency, slower to fit on very small datasets.

### Decision 6: Checkpointing at stage level
**Why:** Pipeline reruns skip completed stages. Critical for long-running
pipelines where one stage fails after 40 minutes of prior work.
**Tradeoff:** Must manually clear checkpoints when forcing a full rerun.

---

## Integration Model

```
┌─────────────────────────────────────────────────────┐
│                   Databricks                        │
│  ┌─────────┐  ┌──────────┐  ┌──────────────────┐   │
│  │ Bronze  │─▶│  Silver  │─▶│      Gold        │   │
│  │  Delta  │  │  Delta   │  │     Delta        │   │
│  └─────────┘  └──────────┘  └────────┬─────────┘   │
└────────────────────────────────────── │─────────────┘
                                        │
              ┌─────────────────────────┤
              │                         │
              ▼                         ▼
     ┌────────────────┐        ┌───────────────┐
     │   CSV Export   │        │  Excel Export │
     │  (DBFS→local)  │        │  (openpyxl)   │
     └───────┬────────┘        └───────────────┘
             │
     ┌───────┴────────────────────┐
     │                            │
     ▼                            ▼
┌──────────────┐          ┌──────────────┐
│  Streamlit   │          │   Power BI   │
│  (Python)    │          │  (Desktop)   │
└──────────────┘          └──────────────┘
```

**Streamlit connection:** Reads CSV exports from `./exports/` directory.
Set `EXPORT_DIR` env var to point to DBFS mount or local copy.

**Power BI connection:** Import mode using CSV exports.
For live connection: use Databricks JDBC driver (requires paid tier).
