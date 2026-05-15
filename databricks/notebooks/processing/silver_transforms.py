# Databricks notebook source
# MAGIC %md
# MAGIC # 🔄 Notebook 2: Data Transformation (Silver Layer)
# MAGIC **Pipeline:** Bronze raw data → Silver cleaned + enriched metrics
# MAGIC
# MAGIC Transforms:
# MAGIC - Standardise & validate financial data
# MAGIC - Compute KPIs: margins, growth rates, ratios
# MAGIC - Join company data with macro indicators
# MAGIC - Flag anomalies

# COMMAND ----------
# MAGIC %md ## 1. Configuration

# COMMAND ----------
BRONZE_PATH = "/FileStore/financial_model/bronze"
SILVER_PATH = "/FileStore/financial_model/silver"

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import pandas as pd
import numpy as np

spark = SparkSession.builder.getOrCreate()
print("✅ Spark session ready")

# COMMAND ----------
# MAGIC %md ## 2. Load Bronze tables

# COMMAND ----------
df_fin   = spark.read.format("delta").load(f"{BRONZE_PATH}/company_financials")
df_macro = spark.read.format("delta").load(f"{BRONZE_PATH}/macro_indicators")

print(f"📋 company_financials : {df_fin.count()} rows")
print(f"📋 macro_indicators   : {df_macro.count()} rows")

# COMMAND ----------
# MAGIC %md ## 3. Validate & clean financials

# COMMAND ----------
def validate_financials(df):
    """
    Data quality checks on raw financials.
    Flags records that fail business rules.
    """
    df = df.withColumn(
        "dq_revenue_positive",
        F.col("revenue") > 0
    ).withColumn(
        "dq_cogs_lt_revenue",
        F.col("cogs") < F.col("revenue")
    ).withColumn(
        "dq_gross_profit_consistent",
        F.abs(F.col("gross_profit") - (F.col("revenue") - F.col("cogs"))) < 1.0
    ).withColumn(
        "dq_passed",
        F.col("dq_revenue_positive") &
        F.col("dq_cogs_lt_revenue") &
        F.col("dq_gross_profit_consistent")
    )

    total   = df.count()
    passing = df.filter(F.col("dq_passed") == True).count()
    failing = total - passing

    print(f"   ✅ Passing DQ checks : {passing}/{total}")
    if failing > 0:
        print(f"   ⚠️  Failing DQ checks : {failing}")
        df.filter(F.col("dq_passed") == False).select("date", "revenue", "cogs", "gross_profit").show()

    # Keep only passing records
    return df.filter(F.col("dq_passed") == True).drop(
        "dq_revenue_positive", "dq_cogs_lt_revenue",
        "dq_gross_profit_consistent", "dq_passed"
    )

print("Running data quality validation...")
df_fin_clean = validate_financials(df_fin)

# COMMAND ----------
# MAGIC %md ## 4. Compute financial KPIs (Silver enrichment)

# COMMAND ----------
# Window for YoY / MoM calculations
w_lag12 = Window.orderBy("date")
w_lag1  = Window.orderBy("date")

df_silver = (
    df_fin_clean

    # ── Margin ratios ────────────────────────────────────────────────────
    .withColumn("gross_margin",   F.col("gross_profit") / F.col("revenue"))
    .withColumn("ebitda_margin",  F.col("ebitda")       / F.col("revenue"))
    .withColumn("ebit_margin",    F.col("ebit")         / F.col("revenue"))
    .withColumn("net_margin",     F.col("net_income")   / F.col("revenue"))

    # ── Efficiency ratios ────────────────────────────────────────────────
    .withColumn("cogs_ratio",     F.col("cogs")          / F.col("revenue"))
    .withColumn("opex_ratio",     F.col("opex")          / F.col("revenue"))
    .withColumn("rd_ratio",       F.col("rd_expense")    / F.col("revenue"))
    .withColumn("sga_ratio",      F.col("sga_expense")   / F.col("revenue"))

    # ── Working capital ──────────────────────────────────────────────────
    .withColumn("working_capital",
        F.col("accounts_receivable") + F.col("inventory") - F.col("accounts_payable"))
    .withColumn("dso",   (F.col("accounts_receivable") / F.col("revenue"))   * 30)
    .withColumn("dio",   (F.col("inventory")           / F.col("cogs"))      * 30)
    .withColumn("dpo",   (F.col("accounts_payable")    / F.col("cogs"))      * 30)
    .withColumn("cash_conversion_cycle",
        F.col("dso") + F.col("dio") - F.col("dpo"))

    # ── Unit economics ───────────────────────────────────────────────────
    .withColumn("revenue_per_customer", F.col("revenue") / F.col("customers"))
    .withColumn("gross_profit_per_customer",
        F.col("gross_profit") / F.col("customers"))

    # ── MoM Growth ───────────────────────────────────────────────────────
    .withColumn("revenue_lag1",  F.lag("revenue",  1).over(w_lag1))
    .withColumn("revenue_mom",
        (F.col("revenue") - F.col("revenue_lag1")) / F.col("revenue_lag1"))

    # ── YoY Growth ───────────────────────────────────────────────────────
    .withColumn("revenue_lag12", F.lag("revenue", 12).over(w_lag12))
    .withColumn("revenue_yoy",
        (F.col("revenue") - F.col("revenue_lag12")) / F.col("revenue_lag12"))
    .withColumn("net_income_lag12", F.lag("net_income", 12).over(w_lag12))
    .withColumn("net_income_yoy",
        (F.col("net_income") - F.col("net_income_lag12")) / F.col("net_income_lag12"))

    # ── Trailing 12-month rolling revenue ───────────────────────────────
    .withColumn("revenue_ttm",
        F.sum("revenue").over(
            Window.orderBy("date").rowsBetween(-11, 0)
        ))

    # Clean up lag columns
    .drop("revenue_lag1", "revenue_lag12", "net_income_lag12")
)

print(f"✅ Silver financials computed: {df_silver.count()} rows, {len(df_silver.columns)} columns")

# COMMAND ----------
# MAGIC %md ## 5. Join macro indicators

# COMMAND ----------
df_silver_macro = df_silver.join(
    df_macro.select(
        "date", "inflation_rate", "interest_rate",
        "gdp_growth", "usd_zar", "unemployment"
    ),
    on="date",
    how="left"
)

# ── Macro-adjusted metrics ─────────────────────────────────────────────────
df_silver_macro = df_silver_macro \
    .withColumn("real_revenue_growth",
        F.col("revenue_yoy") - (F.col("inflation_rate") / 100)) \
    .withColumn("interest_coverage",
        F.col("ebit") / F.when(F.col("interest_expense") > 0,
                                F.col("interest_expense")).otherwise(1))

print(f"✅ Macro join complete: {df_silver_macro.count()} rows")

# COMMAND ----------
# MAGIC %md ## 6. Annual aggregates (for financial statements)

# COMMAND ----------
df_annual = df_silver_macro \
    .withColumn("year", F.year("date")) \
    .groupBy("year", "company") \
    .agg(
        # P&L totals
        F.sum("revenue")          .alias("revenue"),
        F.sum("cogs")             .alias("cogs"),
        F.sum("gross_profit")     .alias("gross_profit"),
        F.sum("opex")             .alias("opex"),
        F.sum("rd_expense")       .alias("rd_expense"),
        F.sum("sga_expense")      .alias("sga_expense"),
        F.sum("ebitda")           .alias("ebitda"),
        F.sum("depreciation")     .alias("depreciation"),
        F.sum("ebit")             .alias("ebit"),
        F.sum("interest_expense") .alias("interest_expense"),
        F.sum("ebt")              .alias("ebt"),
        F.sum("tax_expense")      .alias("tax_expense"),
        F.sum("net_income")       .alias("net_income"),
        F.sum("capex")            .alias("capex"),

        # Average margins (annual)
        F.avg("gross_margin")    .alias("gross_margin_avg"),
        F.avg("ebitda_margin")   .alias("ebitda_margin_avg"),
        F.avg("net_margin")      .alias("net_margin_avg"),

        # Balance sheet end-of-year (last month)
        F.last("accounts_receivable").alias("accounts_receivable"),
        F.last("inventory")          .alias("inventory"),
        F.last("accounts_payable")   .alias("accounts_payable"),
        F.last("working_capital")    .alias("working_capital"),

        # Unit economics
        F.last("customers")              .alias("customers_eoy"),
        F.avg("revenue_per_customer")    .alias("arpu"),
        F.avg("churn_rate")              .alias("churn_rate_avg"),

        # Macro averages
        F.avg("inflation_rate")  .alias("inflation_rate_avg"),
        F.avg("interest_rate")   .alias("interest_rate_avg"),
        F.avg("gdp_growth")      .alias("gdp_growth_avg"),
    ) \
    .orderBy("year")

print(f"✅ Annual aggregates: {df_annual.count()} years")
display(df_annual)

# COMMAND ----------
# MAGIC %md ## 7. Write Silver Delta tables

# COMMAND ----------
def write_silver(sdf, table_name: str, path: str):
    (sdf.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"{path}/{table_name}"))
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS silver_{table_name}
        USING DELTA LOCATION '{path}/{table_name}'
    """)
    print(f"✅ silver_{table_name}: {sdf.count()} rows → {path}/{table_name}")

write_silver(df_silver_macro, "monthly_financials", SILVER_PATH)
write_silver(df_annual,       "annual_financials",  SILVER_PATH)

# COMMAND ----------
# MAGIC %md ## 8. Summary

# COMMAND ----------
print("=" * 55)
print("SILVER LAYER — TRANSFORMATION SUMMARY")
print("=" * 55)
summary = df_annual.toPandas()
for _, row in summary.iterrows():
    print(f"\n  Year {int(row['year'])}")
    print(f"   Revenue       : R{row['revenue']:>14,.0f}")
    print(f"   Gross Margin  : {row['gross_margin_avg']*100:>6.1f}%")
    print(f"   EBITDA Margin : {row['ebitda_margin_avg']*100:>6.1f}%")
    print(f"   Net Margin    : {row['net_margin_avg']*100:>6.1f}%")
    print(f"   Customers EOY : {int(row['customers_eoy']):>10,}")
print("\n✅ Silver transformation complete — proceed to Notebook 03")
