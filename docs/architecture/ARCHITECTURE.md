# Architecture Document
## FinModel Pro — Automated Financial Modelling Platform

---

## 1. Business Problem

**Context:**
FP&A (Financial Planning & Analysis) teams in banks, investment firms, and
corporate finance departments spend 60–80% of their time on manual data
preparation — copying numbers into Excel, updating formulas, and rebuilding
the same forecast every month.

**The problem this platform solves:**

| Pain Point | This Platform |
|------------|---------------|
| Manual monthly Excel updates (4–8 hours/cycle) | Fully automated pipeline runs on schedule |
| No audit trail on model changes | Every run logged, config snapshotted, Delta time travel |
| Forecast disconnected from actuals | Prophet model continuously trained on latest actuals |
| Scenario analysis is slow and error-prone | Scenarios computed automatically (Base/Bull/Bear) |
| Financial statements require manual reconciliation | 3-statement model auto-validated on every run |
| No data quality enforcement | Explicit DQ framework with quarantine and alerting |

**Target users:**
- CFO / Finance Director (executive dashboards, scenario summaries)
- FP&A Analysts (drill-down into drivers, assumption overrides)
- Data Engineers (pipeline monitoring, audit logs)

---

## 2. End-to-End Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                                        │
│   Synthetic Financial Generator    │    FRED API (macro indicators)         │
│   (revenue, costs, balance sheet)  │    (inflation, rates, GDP)             │
└──────────────┬─────────────────────┴──────────────┬────────────────────────┘
               │                                     │
               ▼                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      INGESTION LAYER                                        │
│   Notebook: 01_ingest_financials.py  │  Notebook: 02_ingest_macro.py        │
│   • Retry logic (3 attempts)         │  • FRED API with fallback            │
│   • Schema enforcement               │  • Monthly resampling                │
│   • Metadata tagging (_run_id etc.)  │  • Null interpolation                │
│                         │                          │                        │
│                         └──────────────────────────┘                       │
│                                      │                                      │
└──────────────────────────────────────┼──────────────────────────────────────┘
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│              BRONZE DELTA LAYER  (Raw, Immutable)                           │
│   bronze_company_financials          │  bronze_macro_indicators             │
│   Partition: year, month             │  Partition: year                     │
│   Retention: 7 years                 │  Retention: 7 years                  │
└──────────────────────────────────────┼──────────────────────────────────────┘
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│              DATA QUALITY GATE  (dq_framework.py)                           │
│   Schema validation → Null checks → Duplicate detection                     │
│   Range validation → Business rules → Freshness → Ref. integrity           │
│              │                                                              │
│        ┌─────┴──────┐                                                       │
│        ▼            ▼                                                       │
│  Clean records   Quarantine table  +  DQ log (silver_data_quality_log)     │
└──────────────────────────────────────┼──────────────────────────────────────┘
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│              SILVER DELTA LAYER  (Cleaned, Enriched)                        │
│   silver_monthly_financials          │  silver_annual_financials            │
│   • 35+ derived KPIs                 │  • Annual aggregates                 │
│   • Macro join                       │  • Avg margins, growth rates         │
│   • YoY / MoM growth                 │                                      │
│   Partition: year                    │  Partition: year                     │
└──────────────────────────────────────┼──────────────────────────────────────┘
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│              FORECASTING ENGINE  (forecasting_engine.py)                    │
│   Prophet revenue model              │  Driver-based cost forecasting       │
│   + macro regressors                 │  + efficiency improvement trend      │
│   3-year monthly horizon             │  Working capital ratio projections   │
│                  │                                │                         │
│                  └────────────────────────────────┘                        │
│                                      │                                      │
│               Scenario engine (Base / Bull / Bear)                          │
└──────────────────────────────────────┼──────────────────────────────────────┘
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│              GOLD DELTA LAYER  (Business-Ready)                             │
│   gold_monthly_forecast              │  gold_annual_forecast                │
│   gold_income_statement              │  gold_balance_sheet                  │
│   gold_cash_flow_statement           │  gold_scenario_forecasts             │
│   gold_dcf_valuation                 │  gold_audit_log                      │
└──────────────────────────────────────┼──────────────────────────────────────┘
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│              SERVING LAYER                                                  │
│                                                                             │
│   ┌────────────────────┐    ┌──────────────┐    ┌──────────────────────┐   │
│   │  Streamlit App     │    │  Power BI    │    │  Excel Export        │   │
│   │  • Income stmt     │    │  • KPI       │    │  • 4-tab model       │   │
│   │  • Balance sheet   │    │    dashboard │    │  • Colour-coded      │   │
│   │  • Cash flow       │    │  • Forecast  │    │  • Scenario sheet    │   │
│   │  • DCF valuation   │    │    vs actual │    │                      │   │
│   │  • Scenario engine │    │  • Scenario  │    │                      │   │
│   │  • Sensitivity     │    │    comparison│    │                      │   │
│   └────────────────────┘    └──────────────┘    └──────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
                                       ▲
┌─────────────────────────────────────────────────────────────────────────────┐
│              ORCHESTRATION  (master_pipeline.py)                            │
│   Stage sequencing → Dependency management → Retry logic                   │
│   Checkpoint recovery → Run logging → Task value passing                   │
└─────────────────────────────────────────────────────────────────────────────┘
                                       ▲
┌─────────────────────────────────────────────────────────────────────────────┐
│              OBSERVABILITY  (Audit log + DQ log + Run log)                  │
│   Every run logged → Config snapshotted → Errors captured                  │
│   DQ trends queryable → Stage durations tracked                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Data Lifecycle (Governance, Reproducibility, Scalability)

### Governance
- **Immutable bronze layer**: Raw data is never modified after ingestion
- **Schema registry** (`schemas.py`): All table schemas defined centrally
- **Audit log**: Every pipeline action recorded with `run_id`, timestamp, actor
- **Quarantine**: Bad data isolated, not silently dropped
- **Config snapshot**: Full config dict saved per run for reproducibility

### Reproducibility
- **Deterministic seeds**: Synthetic data uses fixed `random_seed=42`
- **Schema versioning**: `_schema_version` column on every table
- **Delta time travel**: Any previous pipeline state queryable via
  `spark.read.format("delta").option("versionAsOf", N).load(path)`
- **Run ID**: Every record tagged with the pipeline run that created it

### Scalability
- **Partitioning**: Bronze by `(year, month)`, Silver/Gold by `year`
- **Incremental loads**: `DeltaWriter.incremental_write()` only adds new partitions
- **Z-ordering**: Auto-applied on large tables for query performance
- **Lazy evaluation**: Spark DataFrames not materialised until `.write()`

---

## 4. Orchestration

### Dependency Graph
```
ingest_financials ──┐
                    ├──▶ dq_validation ──▶ silver_transforms ──▶ forecasting ──▶ statements ──▶ export
ingest_macro      ──┘
```

### Retry Strategy
- Each stage: up to 3 retries with exponential backoff (30s, 60s, 90s)
- Completed stages: checkpointed to DBFS JSON, skipped on retry
- Fatal stages: DQ validation, transforms, forecasting
- Non-fatal stages: export (pipeline reports warning but continues)

### Execution Modes
| Mode | Command | Use case |
|------|---------|----------|
| Full run | Default | Monthly scheduled run |
| Forced refresh | `force_full_refresh=true` | Reprocess all data |
| Resume from stage | `start_from_stage=05_forecasting` | Debug partial failure |
| Single stage | Run notebook directly | Development |

---

## 5. Tech Stack

| Component | Technology | Reason |
|-----------|------------|--------|
| Data warehouse | Databricks Delta Lake (free tier) | ACID, time travel, partitioning |
| Processing | Apache Spark (via Databricks) | Distributed, scalable |
| Orchestration | Databricks Workflows + master notebook | Native, no extra infra |
| Forecasting | Meta Prophet | Handles seasonality + regressors |
| Financial modelling | Python (pandas, numpy) | Full control over model logic |
| Excel export | openpyxl | No Excel installation needed |
| Serving (interactive) | Streamlit | Free, Python-native, shareable URL |
| Serving (BI) | Power BI Desktop | Free tier, connects via CSV |
| Version control | GitHub | Notebooks as .py files |

---

## 6. Security & Access

- API keys stored as Databricks Secrets (not in code)
- `FRED_API_KEY` read from env var `os.getenv("FRED_API_KEY")`
- DBFS paths are cluster-scoped — no external network exposure
- Audit log provides accountability trail for all data changes
