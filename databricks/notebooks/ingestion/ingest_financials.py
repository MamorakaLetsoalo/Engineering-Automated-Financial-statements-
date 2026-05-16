# Databricks notebook source
# MAGIC %md
# MAGIC # 📥 Notebook 1: Data Ingestion (Bronze Layer)
# MAGIC **Pipeline:** Raw data → Bronze Delta tables
# MAGIC
# MAGIC Sources:
# MAGIC - Synthetic company financials (revenue, costs, transactions)
# MAGIC - FRED API (macro: inflation, interest rates, GDP growth)

# COMMAND ----------
# MAGIC %md ## 1. Install dependencies

# COMMAND ----------
%pip install fredapi pandas numpy faker delta-spark --quiet

# COMMAND ----------
# MAGIC %md ## 2. Configuration

# COMMAND ----------
import os

# ─────────────────────────────────────────────
# CONFIG — update FRED_API_KEY if you have one
# Get a free key at: https://fred.stlouisfed.org/docs/api/api_key.html
# ─────────────────────────────────────────────
FRED_API_KEY = os.getenv("FRED_API_KEY", "YOUR_FRED_API_KEY_HERE")

BRONZE_PATH  = "/FileStore/financial_model/bronze"
COMPANY_NAME = "ML Corp"
START_DATE   = "2018-01-01"
END_DATE     = "2023-12-31"

print(f"Config loaded — Company: {COMPANY_NAME} | Period: {START_DATE} → {END_DATE}")

# COMMAND ----------
# MAGIC %md ## 3. Generate synthetic company financial data

# COMMAND ----------
import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random

fake = Faker()
np.random.seed(42)
random.seed(42)

def generate_monthly_financials(start: str, end: str, company: str) -> pd.DataFrame:
    """
    Generate realistic monthly P&L + balance sheet drivers.
    Uses trend + seasonality + noise to mimic real business data.
    """
    dates = pd.date_range(start=start, end=end, freq="MS")
    n = len(dates)

    # ── Revenue: upward trend + seasonal peak Q4 + noise ──────────────────
    trend        = np.linspace(500_000, 1_200_000, n)
    seasonality  = 80_000 * np.sin(2 * np.pi * (np.arange(n) % 12) / 12 - np.pi / 2)
    noise        = np.random.normal(0, 25_000, n)
    revenue      = np.maximum(trend + seasonality + noise, 100_000)

    # ── Cost drivers ───────────────────────────────────────────────────────
    cogs_ratio       = np.random.uniform(0.38, 0.45, n)   # 38–45% of revenue
    opex_ratio       = np.random.uniform(0.22, 0.28, n)   # 22–28% of revenue
    rd_ratio         = np.random.uniform(0.08, 0.12, n)   # 8–12% of revenue
    sga_ratio        = np.random.uniform(0.10, 0.14, n)   # 10–14% of revenue

    cogs  = revenue * cogs_ratio
    opex  = revenue * opex_ratio
    rd    = revenue * rd_ratio
    sga   = revenue * sga_ratio

    gross_profit  = revenue - cogs
    ebitda        = gross_profit - opex - rd - sga
    depreciation  = revenue * np.random.uniform(0.03, 0.05, n)
    ebit          = ebitda - depreciation
    interest_exp  = np.random.uniform(8_000, 20_000, n)
    ebt           = ebit - interest_exp
    tax_rate      = 0.28  # South African corporate tax rate
    tax           = np.maximum(ebt * tax_rate, 0)
    net_income    = ebt - tax

    # ── Balance sheet drivers ──────────────────────────────────────────────
    accounts_receivable = revenue * np.random.uniform(0.10, 0.15, n)  # ~45 day DSO
    inventory           = cogs   * np.random.uniform(0.12, 0.18, n)   # ~2 month inventory
    accounts_payable    = cogs   * np.random.uniform(0.08, 0.12, n)   # ~30-45 day DPO
    capex               = revenue * np.random.uniform(0.04, 0.07, n)

    # ── Customer & unit economics ──────────────────────────────────────────
    customers         = (np.linspace(1_200, 4_500, n) + np.random.normal(0, 100, n)).astype(int)
    avg_revenue_user  = revenue / customers
    churn_rate        = np.random.uniform(0.02, 0.06, n)

    df = pd.DataFrame({
        "date"                : dates,
        "company"             : company,
        "revenue"             : revenue.round(2),
        "cogs"                : cogs.round(2),
        "gross_profit"        : gross_profit.round(2),
        "opex"                : opex.round(2),
        "rd_expense"          : rd.round(2),
        "sga_expense"         : sga.round(2),
        "ebitda"              : ebitda.round(2),
        "depreciation"        : depreciation.round(2),
        "ebit"                : ebit.round(2),
        "interest_expense"    : interest_exp.round(2),
        "ebt"                 : ebt.round(2),
        "tax_expense"         : tax.round(2),
        "net_income"          : net_income.round(2),
        "accounts_receivable" : accounts_receivable.round(2),
        "inventory"           : inventory.round(2),
        "accounts_payable"    : accounts_payable.round(2),
        "capex"               : capex.round(2),
        "customers"           : customers,
        "avg_revenue_per_user": avg_revenue_user.round(2),
        "churn_rate"          : churn_rate.round(4),
    })
    return df

df_financials = generate_monthly_financials(START_DATE, END_DATE, COMPANY_NAME)
print(f"✅ Generated {len(df_financials)} months of financial data")
display(df_financials.head(10))

# COMMAND ----------
# MAGIC %md ## 4. Fetch macroeconomic data (FRED API)

# COMMAND ----------
def fetch_macro_data(start: str, end: str, api_key: str) -> pd.DataFrame:
    """
    Fetch macro indicators from FRED.
    Falls back to synthetic data if no API key provided.
    """
    dates = pd.date_range(start=start, end=end, freq="MS")

    if api_key == "YOUR_FRED_API_KEY_HERE":
        print("⚠️  No FRED API key — generating synthetic macro data instead.")
        np.random.seed(99)
        n = len(dates)
        # Realistic SA / global macro ranges
        inflation_rate  = np.clip(np.random.normal(5.5, 1.2, n), 2.0, 12.0)       # CPIX %
        interest_rate   = np.clip(np.random.normal(7.0, 1.5, n), 3.5, 12.0)       # SA repo rate %
        gdp_growth      = np.clip(np.random.normal(1.8, 1.0, n), -3.0, 6.0)       # SA real GDP %
        usd_zar         = np.clip(np.random.normal(17.5, 1.8, n), 14.0, 22.0)     # USD/ZAR
        unemployment    = np.clip(np.random.normal(32.0, 2.0, n), 26.0, 38.0)     # SA unemployment %

        return pd.DataFrame({
            "date"           : dates,
            "inflation_rate" : inflation_rate.round(2),
            "interest_rate"  : interest_rate.round(2),
            "gdp_growth"     : gdp_growth.round(2),
            "usd_zar"        : usd_zar.round(2),
            "unemployment"   : unemployment.round(2),
        })

    try:
        from fredapi import Fred
        fred = Fred(api_key=api_key)
        series = {
            "inflation_rate" : fred.get_series("CPIAUCSL", start, end).pct_change(12).mul(100),
            "interest_rate"  : fred.get_series("FEDFUNDS",  start, end),
            "gdp_growth"     : fred.get_series("A191RL1Q225SBEA", start, end),
        }
        df = pd.DataFrame(series).resample("MS").mean().reset_index()
        df.columns = ["date"] + list(series.keys())
        df["usd_zar"]     = 17.5  # static fallback if not pulling FX
        df["unemployment"] = 32.0
        df = df[(df["date"] >= start) & (df["date"] <= end)]
        print("✅ FRED data fetched successfully")
        return df
    except Exception as e:
        print(f"⚠️  FRED fetch failed ({e}) — falling back to synthetic macro data")
        return fetch_macro_data(start, end, "YOUR_FRED_API_KEY_HERE")

df_macro = fetch_macro_data(START_DATE, END_DATE, FRED_API_KEY)
print(f"✅ Macro data: {len(df_macro)} records")
display(df_macro.head(10))

# COMMAND ----------
# MAGIC %md ## 5. Write to Bronze Delta tables

# COMMAND ----------
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

def write_bronze(df: pd.DataFrame, table_name: str, path: str):
    """Convert pandas → Spark → Delta (Bronze layer)."""
    sdf = spark.createDataFrame(df)
    (sdf.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"{path}/{table_name}"))
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS bronze_{table_name}
        USING DELTA LOCATION '{path}/{table_name}'
    """)
    count = spark.read.format("delta").load(f"{path}/{table_name}").count()
    print(f"✅ bronze_{table_name}: {count} rows written to {path}/{table_name}")

write_bronze(df_financials, "company_financials", BRONZE_PATH)
write_bronze(df_macro,      "macro_indicators",   BRONZE_PATH)

# COMMAND ----------
# MAGIC %md ## 6. Validate ingestion

# COMMAND ----------
print("=" * 55)
print("BRONZE LAYER — INGESTION SUMMARY")
print("=" * 55)
for tbl in ["company_financials", "macro_indicators"]:
    df_check = spark.read.format("delta").load(f"{BRONZE_PATH}/{tbl}").toPandas()
    nulls    = df_check.isnull().sum().sum()
    print(f"\n📋 bronze_{tbl}")
    print(f"   Rows      : {len(df_check)}")
    print(f"   Columns   : {df_check.shape[1]}")
    print(f"   Nulls     : {nulls}")
    print(f"   Date range: {df_check['date'].min()} → {df_check['date'].max()}")
print("\n✅ Bronze ingestion complete — proceed to Notebook 02")
