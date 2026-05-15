# Databricks notebook source
# MAGIC %md
# MAGIC # 📡 Notebook: Macro Data Ingestion (Bronze Layer)
# MAGIC Fetches macroeconomic indicators from FRED API or World Bank.
# MAGIC Falls back to SA-calibrated synthetic data if no API key is set.

# COMMAND ----------
%pip install fredapi wbdata --quiet

# COMMAND ----------
import os, sys
import pandas as pd
import numpy as np
from datetime import datetime
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()

# ── Pull config (when run standalone use defaults) ────────────────────────────
try:
    sys.path.insert(0, "../configs")
    from project_config import PATHS, INGESTION, SCHEMA_VERSIONS, get_run_id
except ImportError:
    PATHS = {"bronze": "/FileStore/finmodel_pro/bronze",
             "audit":  "/FileStore/finmodel_pro/audit"}
    INGESTION = {"start_date": "2018-01-01", "end_date": "2023-12-31",
                 "fred_api_key_env": "FRED_API_KEY"}
    def get_run_id(): return datetime.utcnow().strftime("run_%Y%m%d_%H%M%S")

try:
    RUN_ID = dbutils.widgets.get("run_id")
except:
    RUN_ID = get_run_id()

FRED_API_KEY = os.getenv(INGESTION["fred_api_key_env"], "")
START        = INGESTION["start_date"]
END          = INGESTION["end_date"]

print(f"Run ID : {RUN_ID}")
print(f"Period : {START} → {END}")
print(f"FRED   : {'✅ Key found' if FRED_API_KEY else '⚠️  No key — using synthetic SA macro'}")

# COMMAND ----------
# MAGIC %md ## 1. World Bank SA Macro (primary — real SA data)

# COMMAND ----------
def fetch_worldbank_sa(start: str, end: str) -> pd.DataFrame | None:
    """
    Fetch South Africa macro data from World Bank Open Data API.
    Returns monthly-resampled DataFrame or None on failure.
    """
    try:
        import wbdata
        indicators = {
            "FP.CPI.TOTL.ZG" : "inflation_rate",   # CPI % YoY
            "NY.GDP.MKTP.KD.ZG": "gdp_growth",     # Real GDP growth %
            "SL.UEM.TOTL.ZS" : "unemployment",      # Unemployment %
        }
        start_dt = datetime.strptime(start, "%Y-%m-%d")
        end_dt   = datetime.strptime(end,   "%Y-%m-%d")
        df = wbdata.get_dataframe(indicators, country="ZA",
                                   data_date=(start_dt, end_dt))
        df = df.reset_index()
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date").set_index("date")
        df = df.resample("MS").interpolate(method="linear").reset_index()

        # SARB repo rate (synthetic — World Bank doesn't expose it easily)
        n = len(df)
        df["interest_rate"] = np.clip(
            np.random.normal(6.8, 1.2, n), 3.5, 12.0
        ).round(2)
        df["usd_zar"] = np.clip(
            np.linspace(13.5, 19.2, n) + np.random.normal(0, 0.8, n), 12.0, 22.0
        ).round(2)

        df = df.rename(columns={"date": "date"})
        df = df[(df["date"] >= start) & (df["date"] <= end)]
        print(f"✅ World Bank SA data: {len(df)} records")
        return df[["date","inflation_rate","gdp_growth",
                   "unemployment","interest_rate","usd_zar"]]
    except Exception as e:
        print(f"⚠️  World Bank fetch failed: {e}")
        return None


def fetch_fred(start: str, end: str, api_key: str) -> pd.DataFrame | None:
    """FRED fallback (US macro — less relevant but real data)."""
    try:
        from fredapi import Fred
        fred = Fred(api_key=api_key)
        inflation = fred.get_series("CPIAUCSL", start, end).pct_change(12).mul(100)
        interest  = fred.get_series("FEDFUNDS",  start, end)
        gdp       = fred.get_series("A191RL1Q225SBEA", start, end)
        unemp     = fred.get_series("UNRATE",    start, end)

        df = pd.DataFrame({
            "inflation_rate": inflation,
            "interest_rate" : interest,
            "gdp_growth"    : gdp,
            "unemployment"  : unemp,
        }).resample("MS").mean()

        df["usd_zar"] = np.linspace(13.5, 19.2, len(df))
        df = df.reset_index().rename(columns={"index": "date"})
        df = df[(df["date"] >= start) & (df["date"] <= end)]
        print(f"✅ FRED data: {len(df)} records")
        return df
    except Exception as e:
        print(f"⚠️  FRED fetch failed: {e}")
        return None


def generate_synthetic_sa_macro(start: str, end: str) -> pd.DataFrame:
    """SA-calibrated synthetic macro — used when both APIs unavailable."""
    np.random.seed(43)
    dates = pd.date_range(start=start, end=end, freq="MS")
    n = len(dates)
    return pd.DataFrame({
        "date"          : dates,
        "inflation_rate": np.clip(np.random.normal(5.5, 1.5, n), 2.0, 12.0).round(2),
        "interest_rate" : np.clip(np.random.normal(6.8, 1.2, n), 3.5, 12.0).round(2),
        "gdp_growth"    : np.clip(np.random.normal(1.5, 1.2, n), -4.0,  6.0).round(2),
        "usd_zar"       : np.clip(np.linspace(13.5,19.2,n)+np.random.normal(0,.8,n),12,22).round(2),
        "unemployment"  : np.clip(np.random.normal(32.0, 2.0, n), 26.0, 38.0).round(2),
    })

# COMMAND ----------
# MAGIC %md ## 2. Fetch with fallback chain

# COMMAND ----------
df_macro = fetch_worldbank_sa(START, END)

if df_macro is None and FRED_API_KEY:
    df_macro = fetch_fred(START, END, FRED_API_KEY)

if df_macro is None:
    print("Using synthetic SA macro data")
    df_macro = generate_synthetic_sa_macro(START, END)

df_macro["date"] = pd.to_datetime(df_macro["date"])
df_macro = df_macro.fillna(method="ffill").fillna(method="bfill")

# Metadata columns
df_macro["_ingested_at"]    = datetime.utcnow()
df_macro["_run_id"]         = RUN_ID
df_macro["_source"]         = "world_bank_sa" if "World Bank" in str(df_macro) else "synthetic_sa"
df_macro["_schema_version"] = "v1.2"

print(f"\n✅ Macro dataset ready: {len(df_macro)} rows")
print(df_macro[["date","inflation_rate","interest_rate","gdp_growth","usd_zar"]].tail())

# COMMAND ----------
# MAGIC %md ## 3. Write to Bronze Delta

# COMMAND ----------
sdf = spark.createDataFrame(df_macro)
(sdf.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema","true")
    .partitionBy("_run_id")
    .save(f"{PATHS['bronze']}/macro_indicators"))

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS bronze_macro_indicators
    USING DELTA LOCATION '{PATHS['bronze']}/macro_indicators'
""")

count = spark.read.format("delta").load(f"{PATHS['bronze']}/macro_indicators").count()
print(f"✅ bronze_macro_indicators: {count} rows written")

try:
    dbutils.jobs.taskValues.set("macro_rows", count)
    dbutils.jobs.taskValues.set("run_id", RUN_ID)
except: pass
