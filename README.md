# FinModel Pro
## Automated Financial Modelling & Forecasting Platform

> **Bridging traditional finance (Excel/Power BI) with modern data engineering
> (Delta Lake, Apache Spark, Prophet) — fully automated, auditable, reproducible.**

---

## Business Problem

FP&A teams spend 60–80% of their time on manual data preparation.
This platform eliminates that by automating the entire pipeline from
raw financial data → cleaned metrics → ML forecasts → 3-statement model
→ interactive dashboards — with zero manual intervention after setup.

---

## Architecture at a Glance

```
Sources → Bronze (raw) → DQ Gate → Silver (enriched) → Gold (forecast + statements)
                                                              │
                                              ┌───────────────┼────────────────┐
                                              ▼               ▼                ▼
                                         Streamlit        Power BI        Excel file
```

**Full architecture:** See [`docs/architecture/ARCHITECTURE.md`](docs/architecture/ARCHITECTURE.md)

---

## Project Structure

```
finmodel_pro/
│
├── databricks/
│   ├── configs/
│   │   ├── project_config.py       ← Single source of truth (paths, DQ thresholds, model assumptions)
│   │   └── pipeline_utils.py       ← Logger, DeltaWriter, StageTimer, retry decorator, CheckpointManager
│   │
│   ├── schemas/
│   │   └── schemas.py              ← Explicit schema for every Delta table
│   │
│   ├── notebooks/
│   │   ├── ingestion/
│   │   │   ├── ingest_financials.py  ← Synthetic company data → Bronze Delta
│   │   │   └── ingest_macro.py       ← FRED API macro data → Bronze Delta
│   │   │
│   │   ├── quality/
│   │   │   └── dq_framework.py       ← 7-check DQ engine + quarantine + DQ log
│   │   │
│   │   ├── processing/
│   │   │   └── silver_transforms.py  ← Bronze → Silver (35+ KPIs, macro join, partitioned)
│   │   │
│   │   ├── forecasting/
│   │   │   └── forecasting_engine.py ← Prophet + driver-based + scenario engine
│   │   │
│   │   ├── statements/
│   │   │   ├── financial_statements.py ← 3-statement generator + BS validation
│   │   │   └── export_layer.py         ← CSV + Excel export (openpyxl)
│   │   │
│   │   └── orchestration/
│   │       └── master_pipeline.py    ← DAG orchestrator with retry + checkpoint
│   │
│   └── tests/
│       └── test_business_rules.py    ← Unit tests for financial identities
│
├── streamlit_app/
│   ├── app.py                        ← Main entry point
│   ├── pages/
│   │   ├── 01_overview.py
│   │   ├── 02_income_statement.py
│   │   ├── 03_balance_sheet.py
│   │   ├── 04_cash_flow.py
│   │   ├── 05_dcf_valuation.py
│   │   └── 06_scenario_engine.py
│   ├── components/
│   │   ├── fin_table.py              ← Reusable financial statement HTML renderer
│   │   ├── kpi_card.py               ← KPI metric cards
│   │   └── charts.py                 ← Plotly chart helpers
│   └── utils/
│       └── data_loader.py            ← Loads CSVs from exports/ (or falls back to synthetic)
│
├── docs/
│   ├── architecture/
│   │   └── ARCHITECTURE.md           ← Full architecture + tech decisions + tradeoffs
│   ├── data_catalog/
│   │   └── DATA_CATALOG.md           ← Every table, column, business rule, lineage
│   └── runbooks/
│       └── RUNBOOK.md                ← Setup, operations, monitoring, troubleshooting
│
└── exports/                          ← CSVs exported from Databricks (Power BI / Streamlit input)
    ├── historical_annual.csv
    ├── annual_forecast.csv
    ├── monthly_forecast.csv
    ├── scenarios.csv
    ├── income_statement.csv
    ├── balance_sheet.csv
    └── cash_flow.csv
```

---

## Implementation Plan

### Phase 1 — Foundation (Week 1)
- [x] Project config (single source of truth)
- [x] Schema registry (all Delta schemas explicit)
- [x] Pipeline utilities (logger, DeltaWriter, retry, checkpoint)
- [x] Data ingestion (financials + macro, Bronze layer)

### Phase 2 — Data Quality (Week 1–2)
- [x] Schema validation
- [x] Null checks (hard + soft)
- [x] Duplicate detection
- [x] Range validation
- [x] Business rule checks (accounting identities)
- [x] Freshness checks
- [x] Referential integrity
- [x] Quarantine table
- [x] DQ audit log

### Phase 3 — Processing & Forecasting (Week 2)
- [x] Silver transforms (35+ KPIs, macro join, partitioned)
- [x] Prophet revenue forecasting with macro regressors
- [x] Driver-based cost forecasting (margin regression)
- [x] Working capital projections
- [x] Cash flow derivation
- [x] Scenario engine (Base / Bull / Bear)

### Phase 4 — Financial Statements (Week 2–3)
- [x] Income Statement (historical + forecast)
- [x] Balance Sheet (with Assets = L + E validation)
- [x] Cash Flow Statement (indirect method)
- [x] 3-statement linkage validation
- [x] Excel export (4-tab model, formatted)

### Phase 5 — Orchestration (Week 3)
- [x] Master pipeline DAG
- [x] Stage dependency management
- [x] Retry with exponential backoff
- [x] Checkpoint recovery (skip completed stages)
- [x] Run logging + pipeline summary

### Phase 6 — Serving Layer (Week 3–4)
- [x] Streamlit app (6 tabs: overview, 3 statements, DCF, scenarios)
- [x] DCF valuation with WACC × terminal growth sensitivity heatmap
- [x] Scenario comparison engine with live shocks
- [ ] Power BI report template (manual: connect to CSV exports)

### Phase 7 — Observability & Docs (Week 4)
- [x] Audit log (every pipeline event)
- [x] DQ log (every check, queryable)
- [x] Run log (stage durations, row counts)
- [x] Architecture document
- [x] Data catalog (every table + column + business rule)
- [x] Operations runbook

---

## Quick Start

```bash
# 1. Clone repo
git clone https://github.com/your-username/finmodel-pro

# 2. Upload databricks/ folder to Databricks Workspace

# 3. Install cluster libraries:
#    prophet, fredapi, openpyxl, scikit-learn, faker

# 4. Run master pipeline:
#    Databricks → open master_pipeline.py → Run All

# 5. Download CSVs from /FileStore/finmodel_pro/exports/
#    Copy to ./exports/ folder

# 6. Run Streamlit app:
pip install streamlit plotly pandas numpy
cd streamlit_app
streamlit run app.py
```

---

## Key Design Decisions

| Decision | Rationale |
|----------|-----------|
| Delta Lake over Parquet | ACID, time travel, MERGE INTO for idempotent reruns |
| Medallion architecture | Immutable raw data, never modify bronze |
| Explicit schemas | Prevent silent type drift between runs |
| Row-per-scenario | Filtereable in BI tools without schema changes |
| Stage checkpointing | Retry resumes from failure point, not from scratch |
| Indirect cash flow method | Standard FP&A practice, auditable from net income |
| Prophet over ARIMA | Handles seasonality + external regressors natively |
| Quarantine not discard | Bad records isolated, reviewable, reingestable |

---

## Technologies

| Layer | Tool | Cost |
|-------|------|------|
| Data warehouse | Databricks (free tier) | Free |
| Processing | Apache Spark | Free (with Databricks) |
| Forecasting | Meta Prophet | Free (open source) |
| Serving | Streamlit | Free |
| BI | Power BI Desktop | Free |
| Version control | GitHub | Free |
| Macro data | FRED API | Free |

**Total infrastructure cost: $0**

---

## Documentation

| Document | Location |
|----------|----------|
| Architecture & tech decisions | `docs/architecture/ARCHITECTURE.md` |
| Data catalog & lineage | `docs/data_catalog/DATA_CATALOG.md` |
| Operations & troubleshooting | `docs/runbooks/RUNBOOK.md` |
