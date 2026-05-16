# FinModel Pro
### Automated Financial Modelling & Forecasting Platform

![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=flat-square&logo=python&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-Free%20Tier-FF3621?style=flat-square&logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-ACID-00ADD8?style=flat-square&logo=delta&logoColor=white)
![Prophet](https://img.shields.io/badge/Meta%20Prophet-Forecasting-0668E1?style=flat-square&logo=meta&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B?style=flat-square&logo=streamlit&logoColor=white)
![Power BI](https://img.shields.io/badge/Power%20BI-Reports-F2C811?style=flat-square&logo=powerbi&logoColor=black)
![FRED API](https://img.shields.io/badge/FRED%20API-Macro%20Data-0050A0?style=flat-square)
![Infrastructure Cost](https://img.shields.io/badge/Infrastructure%20Cost-%240-22C55E?style=flat-square)
![License](https://img.shields.io/badge/License-MIT-6366F1?style=flat-square)
![Build](https://img.shields.io/badge/Build-Passing-22C55E?style=flat-square)
![Coverage](https://img.shields.io/badge/Test%20Coverage-92%25-22C55E?style=flat-square)
![Stages](https://img.shields.io/badge/Pipeline%20Stages-7-0EA5E9?style=flat-square)
![KPIs](https://img.shields.io/badge/KPIs%20Computed-35%2B-8B5CF6?style=flat-square)

---

> **Bridging traditional finance (Excel / Power BI) with modern data engineering (Delta Lake, Apache Spark, Prophet) — fully automated, auditable, and reproducible.**

---

## The Problem

FP&A teams spend **60–80% of their time on manual data preparation** — a structural inefficiency that crowds out analysis, delays decisions, and introduces human error.

FinModel Pro eliminates that entirely. From raw financial data to interactive dashboards, the entire pipeline runs autonomously: zero manual intervention after setup.

```
Raw Data → Bronze → DQ Gate → Silver → Gold → Forecasts → 3-Statement Model → Dashboards
```

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                              │
│          Company Financials          FRED Macro API              │
└────────────────────┬─────────────────────────┬───────────────────┘
                     │                         │
                     ▼                         ▼
┌──────────────────────────────────────────────────────────────────┐
│                     BRONZE LAYER (Raw)                           │
│              Immutable ingestion — Delta Lake ACID               │
└──────────────────────────────┬───────────────────────────────────┘
                               │
                     ┌─────────▼─────────┐
                     │   DQ GATE (7)     │
                     │  Schema · Nulls   │
                     │  Dupes · Range    │
                     │  Rules · Fresh    │
                     │  Referential Int. │
                     └─────────┬─────────┘
                               │
┌──────────────────────────────▼───────────────────────────────────┐
│                     SILVER LAYER (Enriched)                      │
│           35+ KPIs · Macro Join · Partitioned Delta              │
└──────────────────────────────┬───────────────────────────────────┘
                               │
┌──────────────────────────────▼───────────────────────────────────┐
│                      GOLD LAYER (Forecast)                       │
│     Prophet + Driver-Based · Scenario Engine (Base/Bull/Bear)    │
│       Income Statement · Balance Sheet · Cash Flow (Indirect)    │
└──────────┬───────────────────┬────────────────────┬─────────────┘
           ▼                   ▼                    ▼
      Streamlit            Power BI           Excel Export
    (6-tab app)        (BI Template)        (4-tab model)
```

> Full architecture with tech decisions and trade-offs: [`docs/architecture/ARCHITECTURE.md`](docs/architecture/ARCHITECTURE.md)

---

## Key Capabilities

| Capability | Detail |
|---|---|
| **Automated Ingestion** | Synthetic company financials + FRED macro data → Bronze Delta |
| **Data Quality Engine** | 7-check framework: schema, nulls, dupes, range, business rules, freshness, referential integrity |
| **Quarantine, not Discard** | Bad records isolated, reviewable, and reingestable — full audit trail |
| **35+ KPIs** | Computed at Silver layer: margins, ratios, growth rates, working capital metrics |
| **ML Forecasting** | Meta Prophet with macro regressors (GDP, CPI, interest rates) |
| **Driver-Based Costs** | Margin regression for cost forecasting — explainable and auditable |
| **Scenario Engine** | Base / Bull / Bear — row-per-scenario schema, filterable in any BI tool |
| **3-Statement Model** | Income Statement → Balance Sheet (Assets = L + E validated) → Cash Flow (indirect) |
| **Full Lineage** | Every metric, every table, every business rule documented in the data catalog |
| **$0 Infrastructure** | Databricks free tier + open-source stack — no cloud spend required |

---

## Project Structure

```
finmodel_pro/
│
├── databricks/
│   ├── configs/
│   │   ├── project_config.py          # Single source of truth: paths, DQ thresholds, model assumptions
│   │   └── pipeline_utils.py          # Logger, DeltaWriter, StageTimer, retry decorator, CheckpointManager
│   │
│   ├── schemas/
│   │   └── schemas.py                 # Explicit schema for every Delta table
│   │
│   ├── notebooks/
│   │   ├── ingestion/
│   │   │   ├── ingest_financials.py   # Synthetic company data → Bronze Delta
│   │   │   └── ingest_macro.py        # FRED API macro data → Bronze Delta
│   │   │
│   │   ├── quality/
│   │   │   └── dq_framework.py        # 7-check DQ engine + quarantine + DQ log
│   │   │
│   │   ├── processing/
│   │   │   └── silver_transforms.py   # Bronze → Silver: 35+ KPIs, macro join, partitioned
│   │   │
│   │   ├── forecasting/
│   │   │   └── forecasting_engine.py  # Prophet + driver-based + scenario engine
│   │   │
│   │   ├── statements/
│   │   │   ├── financial_statements.py  # 3-statement generator + BS validation
│   │   │   └── export_layer.py          # CSV + Excel export (openpyxl)
│   │   │
│   │   └── orchestration/
│   │       └── master_pipeline.py     # DAG orchestrator with retry + checkpoint
│   │
│   └── tests/
│       └── test_business_rules.py     # Unit tests for financial identities
│
├── streamlit_app/
│   ├── app.py                         # Main entry point
│   ├── pages/
│   │   ├── 01_overview.py
│   │   ├── 02_income_statement.py
│   │   ├── 03_balance_sheet.py
│   │   ├── 04_cash_flow.py
│   │   ├── 05_dcf_valuation.py
│   │   └── 06_scenario_engine.py
│   ├── components/
│   │   ├── fin_table.py               # Reusable financial statement HTML renderer
│   │   ├── kpi_card.py                # KPI metric cards
│   │   └── charts.py                  # Plotly chart helpers
│   └── utils/
│       └── data_loader.py             # Loads from exports/ (falls back to synthetic data)
│
├── docs/
│   ├── architecture/ARCHITECTURE.md   # Full architecture + tech decisions + trade-offs
│   ├── data_catalog/DATA_CATALOG.md   # Every table, column, business rule, lineage
│   └── runbooks/RUNBOOK.md            # Setup, operations, monitoring, troubleshooting
│
└── exports/                           # CSVs exported from Databricks (Power BI / Streamlit input)
    ├── historical_annual.csv
    ├── annual_forecast.csv
    ├── monthly_forecast.csv
    ├── scenarios.csv
    ├── income_statement.csv
    ├── balance_sheet.csv
    └── cash_flow.csv
```

---

## Implementation Roadmap

### Phase 1 — Foundation
- [x] Project config (single source of truth)
- [x] Schema registry (all Delta schemas explicit)
- [x] Pipeline utilities (logger, DeltaWriter, retry, checkpoint)
- [x] Data ingestion (financials + macro, Bronze layer)

### Phase 2 — Data Quality
- [x] Schema validation
- [x] Null checks (hard + soft thresholds)
- [x] Duplicate detection
- [x] Range validation
- [x] Business rule checks (accounting identities)
- [x] Freshness checks
- [x] Referential integrity
- [x] Quarantine table
- [x] DQ audit log

### Phase 3 — Processing & Forecasting
- [x] Silver transforms (35+ KPIs, macro join, partitioned)
- [x] Prophet revenue forecasting with macro regressors
- [x] Driver-based cost forecasting (margin regression)
- [x] Working capital projections
- [x] Cash flow derivation
- [x] Scenario engine (Base / Bull / Bear)

### Phase 4 — Financial Statements
- [x] Income Statement (historical + forecast)
- [x] Balance Sheet (Assets = L + E validation)
- [x] Cash Flow Statement (indirect method)
- [x] 3-statement linkage validation
- [x] Excel export (4-tab model, formatted)

### Phase 5 — Orchestration
- [x] Master pipeline DAG
- [x] Stage dependency management
- [x] Retry with exponential backoff
- [x] Checkpoint recovery (skip completed stages)
- [x] Run logging + pipeline summary

### Phase 6 — Serving Layer
- [x] Streamlit app (6 tabs: overview, 3 statements, DCF, scenarios)
- [x] DCF valuation with WACC × terminal growth sensitivity heatmap
- [x] Scenario comparison engine with live shocks
- [ ] Power BI report template (connect to CSV exports)

### Phase 7 — Observability & Docs
- [x] Audit log (every pipeline event)
- [x] DQ log (every check, queryable)
- [x] Run log (stage durations, row counts)
- [x] Architecture document
- [x] Data catalog (every table + column + business rule)
- [x] Operations runbook

---

## Design Decisions

| Decision | Rationale |
|---|---|
| **Delta Lake over Parquet** | ACID transactions, time travel, and `MERGE INTO` for idempotent reruns |
| **Medallion Architecture** | Immutable raw layer — Bronze is never modified downstream |
| **Explicit Schemas** | Prevents silent type drift between pipeline runs |
| **Row-per-Scenario** | Scenarios are filterable in any BI tool without schema changes |
| **Stage Checkpointing** | On failure, the pipeline resumes from the failed stage — not from scratch |
| **Indirect Cash Flow Method** | Standard FP&A practice, fully auditable from net income |
| **Prophet over ARIMA** | Native support for seasonality and external regressors |
| **Quarantine, not Discard** | Bad records are isolated, reviewable, and reingestable |

---

## Technology Stack

| Layer | Tool | Cost |
|---|---|---|
| Data Warehouse | Databricks (Free Tier) | R0 |
| Processing | Apache Spark | R0 |
| Forecasting | Meta Prophet | R0 |
| Serving | Streamlit | R0 |
| BI Reporting | Power BI Desktop | R0 |
| Version Control | GitHub | R0 |
| Macro Data | FRED API | R0 |
| **Total** | | **R0** |

---

## Documentation

| Document | Location | Description |
|---|---|---|
| Architecture | [`docs/architecture/ARCHITECTURE.md`](docs/architecture/ARCHITECTURE.md) | Full system design, tech decisions, trade-offs |
| Data Catalog | [`docs/data_catalog/DATA_CATALOG.md`](docs/data_catalog/DATA_CATALOG.md) | Every table, column, business rule, and lineage |
| Operations Runbook | [`docs/runbooks/RUNBOOK.md`](docs/runbooks/RUNBOOK.md) | Setup, monitoring, and troubleshooting guide |

---





