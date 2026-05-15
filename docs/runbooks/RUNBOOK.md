# Operations Runbook
## FinModel Pro — Pipeline Operations Guide

---

## 1. First-Time Setup

### Step 1 — Upload files to Databricks

Upload the entire project to Databricks Repos or Workspace:

```
Databricks Workspace
└── /Repos/your-username/finmodel-pro/
    └── databricks/
        ├── configs/
        │   ├── project_config.py
        │   └── pipeline_utils.py
        ├── schemas/
        │   └── schemas.py
        └── notebooks/
            ├── ingestion/
            ├── processing/
            ├── forecasting/
            ├── statements/
            ├── quality/
            └── orchestration/
                └── master_pipeline.py
```

### Step 2 — Install libraries on your cluster

In your Databricks cluster → Libraries → Install New:

```
PyPI packages:
  prophet
  fredapi
  openpyxl
  scikit-learn
  faker
  delta-spark
```

Or add to cluster init script:
```bash
pip install prophet fredapi openpyxl scikit-learn faker
```

### Step 3 — Set FRED API key (optional)

Databricks cluster → Edit → Environment Variables:
```
FRED_API_KEY = your_key_here
```

Get a free key at: https://fred.stlouisfed.org/docs/api/api_key.html
If not set, synthetic macro data is used automatically.

### Step 4 — Run the master pipeline

Open `notebooks/orchestration/master_pipeline.py` and click **Run All**.

Expected output:
```
╔══════════════════════════════════════════════════════╗
║        FinModel Pro — Pipeline Orchestrator          ║
╠══════════════════════════════════════════════════════╣
║  Run ID   : run_20240115_143000                     ║
...
╚══════════════════════════════════════════════════════╝

✅ [14:30:01] [orchestrator] Running stage: 01_ingest_financials
✅ [14:30:15] [orchestrator] Stage '01_ingest_financials' succeeded in 14.2s
...
╔══════════════════════════════════════════════════════╗
║          Pipeline Complete — SUCCESS                 ║
```

---

## 2. Scheduled Runs (Databricks Workflows)

### Create a Job

1. Databricks → **Workflows** → **Create Job**
2. Name: `FinModel Pro Monthly`
3. Task type: **Notebook**
4. Notebook path: `notebooks/orchestration/master_pipeline`
5. Cluster: your cluster
6. Schedule: `0 6 1 * *` (6am on the 1st of each month)
7. Parameters:
   ```json
   {
     "force_full_refresh": "false",
     "start_from_stage": ""
   }
   ```

### Job notifications
- On failure: email to `data-team@company.com`
- On success: optional Slack webhook

---

## 3. Common Operations

### Force a full pipeline rerun
```python
# In master_pipeline.py widget or job parameter:
force_full_refresh = "true"
```
This clears all checkpoints and reruns every stage.

### Resume from a specific stage after failure
```python
# If stages 01–04 completed but 05_forecasting failed:
start_from_stage = "05_forecasting"
```
Stages 01–04 are skipped via checkpoint; forecasting runs fresh.

### Rerun only the export stage
```python
start_from_stage = "07_export"
```

### Check what data is in Bronze
```sql
-- In a Databricks SQL cell:
SELECT date, company, revenue, gross_margin_avg
FROM bronze_company_financials
ORDER BY date DESC
LIMIT 20
```

### Check DQ history
```sql
SELECT run_id, check_name, failure_pct, passed, checked_at
FROM silver_data_quality_log
WHERE passed = false
ORDER BY checked_at DESC
```

### View audit log
```sql
SELECT run_id, stage, event_type, message, recorded_at
FROM gold_audit_log
WHERE severity IN ('WARN', 'ERROR')
ORDER BY recorded_at DESC
LIMIT 50
```

### Time-travel to a previous run's data
```python
# Get version history
spark.sql("DESCRIBE HISTORY delta.`/FileStore/finmodel_pro/gold/annual_forecast`").show()

# Read version 3
df = (spark.read
      .format("delta")
      .option("versionAsOf", 3)
      .load("/FileStore/finmodel_pro/gold/annual_forecast"))
```

### View quarantined records
```sql
SELECT date, company, revenue, _quarantine_reason, _quarantined_at
FROM delta.`/FileStore/finmodel_pro/quarantine/financials`
ORDER BY _quarantined_at DESC
```

---

## 4. Download CSVs for Streamlit / Power BI

After the pipeline runs, export CSVs from DBFS to your local machine:

```python
# In a Databricks notebook cell:
import shutil

files = [
    "annual_forecast",
    "monthly_forecast",
    "scenarios",
    "historical_annual",
    "income_statement",
    "balance_sheet",
    "cash_flow",
]

for f in files:
    src  = f"/dbfs/FileStore/finmodel_pro/exports/{f}.csv"
    dest = f"/tmp/{f}.csv"
    shutil.copy(src, dest)
    print(f"Copied: {f}.csv")
```

Then download via Databricks file browser or `dbutils.fs.cp`.

Place CSVs in the `./exports/` folder next to `streamlit_app/app.py`.

---

## 5. Running the Streamlit App

```bash
# Install dependencies
pip install streamlit plotly pandas numpy openpyxl

# Set exports path
export EXPORT_DIR=./exports

# Run app
cd streamlit_app
streamlit run app.py

# App opens at: http://localhost:8501
```

### Deploy to Streamlit Cloud (free)
1. Push project to GitHub
2. Go to share.streamlit.io
3. Connect repo → set main file: `streamlit_app/app.py`
4. Set secret: `EXPORT_DIR = ./exports`
5. Deploy — you get a free public URL

---

## 6. Troubleshooting

### Pipeline fails at DQ validation
```
RuntimeError: PIPELINE ABORTED: DQ failure rate 15.2% exceeds threshold 10.0%
```
**Fix:** Check DQ log for which checks failed. If data is valid but DQ thresholds
are too tight, adjust `DQ["dq_fail_threshold"]` in `project_config.py`.

### Prophet install fails
```
ModuleNotFoundError: No module named 'prophet'
```
**Fix:** Prophet requires `pystan`. Install via:
```
%pip install prophet --quiet
```
If still failing, install pystan separately: `%pip install pystan==2.19.1.1`

### Delta table not found
```
AnalysisException: Path does not exist
```
**Fix:** Run stages in order. Each stage depends on upstream stages.
Or run `master_pipeline.py` which handles ordering automatically.

### Excel export file too large
**Fix:** Limit historical years passed to Excel builder.
Default is all years; set `excel_max_years=5` in export notebook.

### FRED API rate limit
```
ValueError: API limit reached
```
**Fix:** FRED free tier allows 120 calls/minute. The retry decorator
(3 attempts, 30s delay) handles transient limits. If persistent,
use synthetic macro data (remove `FRED_API_KEY`).

---

## 7. Monitoring Queries

### Pipeline run health (last 30 days)
```sql
SELECT
    DATE(recorded_at) AS run_date,
    COUNT(CASE WHEN event_type = 'pipeline_complete' AND message LIKE '%SUCCESS%' THEN 1 END) AS successes,
    COUNT(CASE WHEN event_type = 'pipeline_complete' AND message LIKE '%FAILED%'  THEN 1 END) AS failures
FROM gold_audit_log
WHERE recorded_at >= CURRENT_DATE - 30
GROUP BY 1
ORDER BY 1 DESC
```

### DQ trend — gross profit identity failures
```sql
SELECT
    DATE(checked_at) AS check_date,
    failure_pct * 100 AS failure_pct
FROM silver_data_quality_log
WHERE check_name = 'gross_profit_identity'
ORDER BY check_date DESC
```

### Average pipeline duration
```sql
SELECT
    AVG(CAST(REGEXP_EXTRACT(message, '([0-9.]+)s', 1) AS DOUBLE)) AS avg_duration_seconds
FROM gold_audit_log
WHERE event_type = 'pipeline_complete'
```
