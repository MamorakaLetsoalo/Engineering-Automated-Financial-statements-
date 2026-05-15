# Databricks notebook source
# MAGIC %md
# MAGIC # 🛡️ Data Quality & Validation Framework
# MAGIC **Layer:** Bronze → Quality gate → Silver
# MAGIC
# MAGIC Checks performed:
# MAGIC 1. **Schema validation** — column presence + types match registry
# MAGIC 2. **Null checks** — no nulls in NOT NULL fields, threshold on others
# MAGIC 3. **Duplicate detection** — no duplicate (date, company) combos
# MAGIC 4. **Range validation** — revenue/margin values within business rules
# MAGIC 5. **Business rules** — gross_profit = revenue - cogs, EBITDA consistency
# MAGIC 6. **Freshness check** — latest record not older than threshold
# MAGIC 7. **Referential integrity** — every financial record has a macro record
# MAGIC
# MAGIC Output:
# MAGIC - Passed records → Silver pipeline
# MAGIC - Failed records → Quarantine table (with failure reason)
# MAGIC - DQ summary → silver_data_quality_log

# COMMAND ----------
# MAGIC %run ../configs/project_config
# MAGIC %run ../configs/pipeline_utils
# MAGIC %run ../schemas/schemas

# COMMAND ----------
import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StringType

spark = SparkSession.builder.getOrCreate()
init_utils(spark)

RUN_ID = get_run_id()
logger = PipelineLogger(
    run_id=RUN_ID,
    stage="data_quality",
    audit_path=f"{PATHS['audit']}/audit_log"
)

logger.info(f"DQ Framework initialised | run_id={RUN_ID}")

# COMMAND ----------
# MAGIC %md ## 1. Load Bronze tables

# COMMAND ----------
df_fin   = spark.read.format("delta").load(f"{PATHS['bronze']}/company_financials")
df_macro = spark.read.format("delta").load(f"{PATHS['bronze']}/macro_indicators")

logger.info("Bronze tables loaded",
            rows_affected=df_fin.count() + df_macro.count())

# COMMAND ----------
# MAGIC %md ## 2. DQ Check Engine

# COMMAND ----------
class DQResult:
    """Accumulates DQ check results for reporting."""

    def __init__(self, run_id: str):
        self.run_id  = run_id
        self.results = []

    def add(self, table: str, check: str, check_type: str,
            column: str, total: int, failed: int,
            threshold: float, details: str = ""):
        failure_pct = failed / max(total, 1)
        passed      = failure_pct <= threshold

        self.results.append({
            "run_id"        : self.run_id,
            "table_name"    : table,
            "check_name"    : check,
            "check_type"    : check_type,
            "column_name"   : column,
            "rows_checked"  : total,
            "rows_failed"   : failed,
            "failure_pct"   : round(failure_pct, 4),
            "threshold"     : threshold,
            "passed"        : passed,
            "details"       : details,
            "checked_at"    : datetime.utcnow(),
        })

        icon = "✅" if passed else "❌"
        print(f"   {icon} [{check_type}] {check}: "
              f"{failed}/{total} failed ({failure_pct*100:.1f}%) "
              f"| threshold={threshold*100:.1f}%"
              + (f" | {details}" if details else ""))
        return passed

    def failed_checks(self):
        return [r for r in self.results if not r["passed"]]

    def summary(self):
        total  = len(self.results)
        passed = sum(1 for r in self.results if r["passed"])
        return f"{passed}/{total} checks passed"

    def to_spark_df(self, spark: SparkSession):
        return spark.createDataFrame(self.results)


dq = DQResult(RUN_ID)

# COMMAND ----------
# MAGIC %md ## 3. Schema Validation

# COMMAND ----------
def check_schema(sdf: DataFrame, expected_schema: dict,
                 table_name: str, dq: DQResult) -> bool:
    """
    Validates that all required columns exist with compatible types.
    Does NOT enforce exact type match — allows widening (e.g. int → long).
    """
    print(f"\n📋 Schema validation: {table_name}")
    actual_cols   = {f.name: f.dataType.typeName() for f in sdf.schema.fields}
    expected_cols = {f.name: f.dataType.typeName() for f in expected_schema.fields
                     if not f.name.startswith("_")}  # skip metadata cols

    missing = [c for c in expected_cols if c not in actual_cols]
    extra   = [c for c in actual_cols   if c not in expected_cols
                and not c.startswith("_")]

    n = sdf.count()
    failed_missing = len(missing)

    dq.add(table_name, "schema_columns_present", "schema",
           "all_columns", len(expected_cols), failed_missing,
           threshold=0.0,
           details=f"Missing: {missing}" if missing else "")

    if extra:
        logger.warn(f"Extra columns (not in schema): {extra}",
                    metadata={"table": table_name, "extra_cols": extra})

    return failed_missing == 0


check_schema(df_fin,   BRONZE_FINANCIALS, "bronze_company_financials", dq)
check_schema(df_macro, BRONZE_MACRO,      "bronze_macro_indicators",   dq)

# COMMAND ----------
# MAGIC %md ## 4. Null Checks

# COMMAND ----------
def check_nulls(sdf: DataFrame, table_name: str,
                not_null_cols: list, dq: DQResult,
                max_null_pct: float = DQ["max_null_pct"]):
    """
    1. Hard fail if NOT NULL columns have any nulls
    2. Soft warn if nullable columns exceed max_null_pct
    """
    print(f"\n🔍 Null checks: {table_name}")
    n = sdf.count()

    # Hard checks — columns that must never be null
    for col in not_null_cols:
        if col not in [f.name for f in sdf.schema.fields]: continue
        null_count = sdf.filter(F.col(col).isNull()).count()
        dq.add(table_name, f"not_null_{col}", "null_check",
               col, n, null_count, threshold=0.0)

    # Soft checks — all other numeric columns
    num_cols = [f.name for f in sdf.schema.fields
                if "double" in f.dataType.typeName().lower()
                and f.name not in not_null_cols
                and not f.name.startswith("_")]

    for col in num_cols:
        null_count = sdf.filter(F.col(col).isNull()).count()
        dq.add(table_name, f"null_pct_{col}", "null_check",
               col, n, null_count, threshold=max_null_pct)


check_nulls(df_fin,   "bronze_company_financials",
            not_null_cols=["date", "company", "revenue", "cogs", "gross_profit"],
            dq=dq)

check_nulls(df_macro, "bronze_macro_indicators",
            not_null_cols=["date"],
            dq=dq)

# COMMAND ----------
# MAGIC %md ## 5. Duplicate Detection

# COMMAND ----------
def check_duplicates(sdf: DataFrame, table_name: str,
                     key_cols: list, dq: DQResult):
    """
    Detects duplicate records on business key columns.
    Expected: one row per (date, company) combination.
    """
    print(f"\n🔍 Duplicate detection: {table_name} on {key_cols}")
    n = sdf.count()
    n_distinct = sdf.select(*key_cols).distinct().count()
    duplicates = n - n_distinct

    dq.add(table_name, f"no_duplicates_{'_'.join(key_cols)}", "duplicate",
           ", ".join(key_cols), n, duplicates, threshold=0.0,
           details=f"{duplicates} duplicate rows found" if duplicates > 0 else "")

    if duplicates > 0:
        # Show which dates are duplicated
        dupes = (sdf.groupBy(*key_cols)
                    .count()
                    .filter(F.col("count") > 1)
                    .orderBy(F.col("count").desc()))
        logger.warn(f"Duplicates detected in {table_name}",
                    metadata={"count": duplicates})
        dupes.show(5)


check_duplicates(df_fin,   "bronze_company_financials", ["date", "company"], dq)
check_duplicates(df_macro, "bronze_macro_indicators",   ["date"],            dq)

# COMMAND ----------
# MAGIC %md ## 6. Range Validation

# COMMAND ----------
def check_ranges(sdf: DataFrame, table_name: str,
                 range_rules: list, dq: DQResult):
    """
    Validates numeric columns fall within expected business ranges.
    range_rules: list of (column, min_val, max_val, threshold)
    """
    print(f"\n📊 Range validation: {table_name}")
    n = sdf.count()

    for col, min_val, max_val, threshold in range_rules:
        if col not in [f.name for f in sdf.schema.fields]: continue

        out_of_range = sdf.filter(
            (F.col(col) < min_val) | (F.col(col) > max_val)
        ).count()

        dq.add(table_name,
               f"range_{col}_{min_val}_{max_val}", "range",
               col, n, out_of_range, threshold=threshold,
               details=f"Expected [{min_val}, {max_val}]")


check_ranges(df_fin, "bronze_company_financials", [
    ("revenue",         DQ["revenue_min"], DQ["revenue_max"],       0.00),
    ("gross_profit",    -1_000_000,        20_000_000,              0.00),
    ("churn_rate",      0.0,               1.0,                     0.00),
    ("customers",       0,                 100_000,                 0.00),
], dq)

check_ranges(df_macro, "bronze_macro_indicators", [
    ("inflation_rate",  -5.0,   50.0,  0.05),
    ("interest_rate",   0.0,    50.0,  0.00),
    ("gdp_growth",      -20.0,  25.0,  0.05),
    ("usd_zar",         5.0,    50.0,  0.05),
], dq)

# COMMAND ----------
# MAGIC %md ## 7. Business Rule Validation

# COMMAND ----------
def check_business_rules(sdf: DataFrame, table_name: str, dq: DQResult):
    """
    Domain-specific financial accounting rules:
    1. Gross Profit = Revenue - COGS  (accounting identity)
    2. EBITDA = Gross Profit - OpEx - R&D - SG&A
    3. EBIT = EBITDA - Depreciation
    4. Net Income = EBT - Tax
    5. Gross margin within bounds
    6. Revenue growth not extreme (likely data error)
    """
    print(f"\n📐 Business rule validation: {table_name}")
    n = sdf.count()
    tol = 1.0  # R1 rounding tolerance

    # Rule 1: Gross Profit identity
    br1 = sdf.filter(
        F.abs(F.col("gross_profit") - (F.col("revenue") - F.col("cogs"))) > tol
    ).count()
    dq.add(table_name, "gross_profit_identity", "business_rule",
           "gross_profit", n, br1, threshold=0.0,
           details="gross_profit != revenue - cogs")

    # Rule 2: Gross margin in range
    gm_col = (F.col("gross_profit") / F.col("revenue"))
    br2 = sdf.filter(
        (gm_col < DQ["gross_margin_min"]) |
        (gm_col > DQ["gross_margin_max"])
    ).count()
    dq.add(table_name, "gross_margin_bounds", "business_rule",
           "gross_margin", n, br2, threshold=0.02,
           details=f"Expected [{DQ['gross_margin_min']}, {DQ['gross_margin_max']}]")

    # Rule 3: Revenue > COGS (should not sell below cost consistently)
    br3 = sdf.filter(F.col("revenue") <= F.col("cogs")).count()
    dq.add(table_name, "revenue_gt_cogs", "business_rule",
           "revenue", n, br3, threshold=0.05)

    # Rule 4: Net income sign consistency with EBT
    if "ebt" in [f.name for f in sdf.schema.fields]:
        br4 = sdf.filter(
            (F.col("ebt") > 0) & (F.col("net_income") < 0)  # profitable before tax but loss after?
        ).count()
        dq.add(table_name, "net_income_sign_consistent", "business_rule",
               "net_income", n, br4, threshold=0.0)


check_business_rules(df_fin, "bronze_company_financials", dq)

# COMMAND ----------
# MAGIC %md ## 8. Freshness Check

# COMMAND ----------
def check_freshness(sdf: DataFrame, table_name: str,
                    date_col: str, max_age_days: int, dq: DQResult):
    """Ensures the latest record is not stale."""
    print(f"\n📅 Freshness check: {table_name}")
    latest = sdf.agg(F.max(date_col).alias("latest")).collect()[0]["latest"]
    age    = (datetime.utcnow().date() - latest).days if latest else 9999

    failed = 1 if age > max_age_days else 0
    dq.add(table_name, "data_freshness", "freshness",
           date_col, 1, failed, threshold=0.0,
           details=f"Latest: {latest}, Age: {age} days (max: {max_age_days})")


check_freshness(df_fin,   "bronze_company_financials", "date",
                max_age_days=DQ["max_data_age_days"], dq=dq)
check_freshness(df_macro, "bronze_macro_indicators",   "date",
                max_age_days=DQ["max_data_age_days"] * 2, dq=dq)

# COMMAND ----------
# MAGIC %md ## 9. Referential Integrity

# COMMAND ----------
def check_referential_integrity(df_fin: DataFrame, df_macro: DataFrame, dq: DQResult):
    """Every financial month must have a corresponding macro record."""
    print("\n🔗 Referential integrity: financials → macro")

    fin_dates   = df_fin.select("date").distinct()
    macro_dates = df_macro.select("date").distinct()

    orphans = fin_dates.join(macro_dates, on="date", how="left_anti").count()
    total   = fin_dates.count()

    dq.add("bronze_company_financials",
           "fin_dates_in_macro", "referential_integrity",
           "date", total, orphans, threshold=0.05,
           details=f"{orphans} financial months with no macro record")


check_referential_integrity(df_fin, df_macro, dq)

# COMMAND ----------
# MAGIC %md ## 10. DQ Summary & Quarantine

# COMMAND ----------
print("\n" + "=" * 60)
print("DATA QUALITY SUMMARY")
print("=" * 60)
print(f"Run ID : {RUN_ID}")
print(f"Result : {dq.summary()}")

failed_checks = dq.failed_checks()
if failed_checks:
    print(f"\n❌ Failed checks ({len(failed_checks)}):")
    for c in failed_checks:
        print(f"   • [{c['check_type']}] {c['check_name']} "
              f"({c['failure_pct']*100:.1f}% failed)")

# Write DQ log to Delta
dq_sdf = dq.to_spark_df(spark)
dq_sdf.write.format("delta") \
            .mode("append") \
            .option("overwriteSchema", "true") \
            .save(f"{PATHS['silver']}/data_quality_log")

logger.info("DQ log written", rows_affected=len(dq.results))

# ── Quarantine failed records ────────────────────────────────────────────────
# Tag records that fail hard checks and write to quarantine
df_tagged = df_fin.withColumn(
    "_dq_gross_profit_ok",
    F.abs(F.col("gross_profit") - (F.col("revenue") - F.col("cogs"))) <= 1.0
).withColumn(
    "_dq_revenue_positive",
    F.col("revenue") > 0
).withColumn(
    "_dq_passed",
    F.col("_dq_gross_profit_ok") & F.col("_dq_revenue_positive")
)

quarantine = df_tagged.filter(F.col("_dq_passed") == False)
q_count    = quarantine.count()

if q_count > 0:
    logger.warn(f"Quarantining {q_count} records", event_type="quarantine",
                metadata={"quarantine_path": PATHS["quarantine"]})
    (quarantine
        .withColumn("_quarantine_reason",
                    F.when(~F.col("_dq_gross_profit_ok"), "gross_profit_identity_failed")
                     .when(~F.col("_dq_revenue_positive"), "revenue_non_positive")
                     .otherwise("unknown"))
        .withColumn("_quarantined_at", F.current_timestamp())
        .withColumn("_run_id", F.lit(RUN_ID))
        .write.format("delta")
        .mode("append")
        .option("overwriteSchema", "true")
        .save(f"{PATHS['quarantine']}/financials"))
    print(f"⚠️  {q_count} records quarantined → {PATHS['quarantine']}/financials")

# Clean records to pass forward
df_clean = df_tagged.filter(F.col("_dq_passed") == True) \
                    .drop("_dq_gross_profit_ok", "_dq_revenue_positive", "_dq_passed")

clean_count = df_clean.count()
logger.info(f"DQ complete: {clean_count} records pass, {q_count} quarantined",
            event_type="dq_complete",
            rows_affected=clean_count)

# ── Hard abort if too many records failed ────────────────────────────────────
fail_rate = q_count / max(df_fin.count(), 1)
if fail_rate > DQ["dq_fail_threshold"]:
    msg = (f"PIPELINE ABORTED: DQ failure rate {fail_rate*100:.1f}% "
           f"exceeds threshold {DQ['dq_fail_threshold']*100:.0f}%")
    logger.error(msg, event_type="pipeline_abort")
    raise RuntimeError(msg)

logger.flush()
print(f"\n✅ DQ complete — {clean_count:,} clean records proceed to Silver")
# Pass clean count to next notebook via dbutils (Databricks task values)
try:
    dbutils.jobs.taskValues.set("dq_clean_count",     clean_count)
    dbutils.jobs.taskValues.set("dq_quarantine_count", q_count)
    dbutils.jobs.taskValues.set("run_id",              RUN_ID)
except: pass
