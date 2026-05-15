"""
schemas.py
==========
Explicit schema definitions for every Delta table in the pipeline.

Design decision: Define schemas here rather than inferring from data.
Schema inference causes silent type drift between pipeline runs.
Explicit schemas = reproducibility + early failure on bad data.
"""

from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType,
    DoubleType, FloatType,
    DateType, TimestampType, BooleanType
)

# ─────────────────────────────────────────────────────────────────────────────
# BRONZE SCHEMAS
# ─────────────────────────────────────────────────────────────────────────────

BRONZE_FINANCIALS = StructType([
    StructField("date",                 DateType(),   nullable=False),
    StructField("company",              StringType(), nullable=False),
    StructField("revenue",              DoubleType(), nullable=False),
    StructField("cogs",                 DoubleType(), nullable=False),
    StructField("gross_profit",         DoubleType(), nullable=False),
    StructField("opex",                 DoubleType(), nullable=True),
    StructField("rd_expense",           DoubleType(), nullable=True),
    StructField("sga_expense",          DoubleType(), nullable=True),
    StructField("ebitda",               DoubleType(), nullable=True),
    StructField("depreciation",         DoubleType(), nullable=True),
    StructField("ebit",                 DoubleType(), nullable=True),
    StructField("interest_expense",     DoubleType(), nullable=True),
    StructField("ebt",                  DoubleType(), nullable=True),
    StructField("tax_expense",          DoubleType(), nullable=True),
    StructField("net_income",           DoubleType(), nullable=True),
    StructField("accounts_receivable",  DoubleType(), nullable=True),
    StructField("inventory",            DoubleType(), nullable=True),
    StructField("accounts_payable",     DoubleType(), nullable=True),
    StructField("capex",                DoubleType(), nullable=True),
    StructField("customers",            LongType(),   nullable=True),
    StructField("avg_revenue_per_user", DoubleType(), nullable=True),
    StructField("churn_rate",           DoubleType(), nullable=True),
    # Metadata
    StructField("_ingested_at",         TimestampType(), nullable=False),
    StructField("_run_id",              StringType(),    nullable=False),
    StructField("_source",              StringType(),    nullable=False),
    StructField("_schema_version",      StringType(),    nullable=False),
])

BRONZE_MACRO = StructType([
    StructField("date",             DateType(),   nullable=False),
    StructField("inflation_rate",   DoubleType(), nullable=True),
    StructField("interest_rate",    DoubleType(), nullable=True),
    StructField("gdp_growth",       DoubleType(), nullable=True),
    StructField("usd_zar",          DoubleType(), nullable=True),
    StructField("unemployment",     DoubleType(), nullable=True),
    # Metadata
    StructField("_ingested_at",     TimestampType(), nullable=False),
    StructField("_run_id",          StringType(),    nullable=False),
    StructField("_source",          StringType(),    nullable=False),
    StructField("_schema_version",  StringType(),    nullable=False),
])

PIPELINE_RUN_LOG = StructType([
    StructField("run_id",           StringType(),    nullable=False),
    StructField("stage",            StringType(),    nullable=False),
    StructField("status",           StringType(),    nullable=False),  # running|success|failed|skipped
    StructField("started_at",       TimestampType(), nullable=False),
    StructField("completed_at",     TimestampType(), nullable=True),
    StructField("duration_seconds", DoubleType(),    nullable=True),
    StructField("rows_processed",   LongType(),      nullable=True),
    StructField("rows_failed",      LongType(),      nullable=True),
    StructField("error_message",    StringType(),    nullable=True),
    StructField("config_snapshot",  StringType(),    nullable=True),  # JSON blob
])

# ─────────────────────────────────────────────────────────────────────────────
# SILVER SCHEMAS
# ─────────────────────────────────────────────────────────────────────────────

SILVER_MONTHLY = StructType([
    StructField("date",                    DateType(),   nullable=False),
    StructField("year",                    IntegerType(),nullable=False),
    StructField("month",                   IntegerType(),nullable=False),
    StructField("quarter",                 IntegerType(),nullable=False),
    StructField("company",                 StringType(), nullable=False),
    # P&L
    StructField("revenue",                 DoubleType(), nullable=False),
    StructField("cogs",                    DoubleType(), nullable=False),
    StructField("gross_profit",            DoubleType(), nullable=False),
    StructField("opex",                    DoubleType(), nullable=True),
    StructField("rd_expense",              DoubleType(), nullable=True),
    StructField("sga_expense",             DoubleType(), nullable=True),
    StructField("ebitda",                  DoubleType(), nullable=True),
    StructField("depreciation",            DoubleType(), nullable=True),
    StructField("ebit",                    DoubleType(), nullable=True),
    StructField("interest_expense",        DoubleType(), nullable=True),
    StructField("ebt",                     DoubleType(), nullable=True),
    StructField("tax_expense",             DoubleType(), nullable=True),
    StructField("net_income",              DoubleType(), nullable=True),
    # Margin KPIs
    StructField("gross_margin",            DoubleType(), nullable=True),
    StructField("ebitda_margin",           DoubleType(), nullable=True),
    StructField("ebit_margin",             DoubleType(), nullable=True),
    StructField("net_margin",              DoubleType(), nullable=True),
    StructField("cogs_ratio",              DoubleType(), nullable=True),
    StructField("opex_ratio",              DoubleType(), nullable=True),
    # Working capital
    StructField("accounts_receivable",     DoubleType(), nullable=True),
    StructField("inventory",               DoubleType(), nullable=True),
    StructField("accounts_payable",        DoubleType(), nullable=True),
    StructField("working_capital",         DoubleType(), nullable=True),
    StructField("dso",                     DoubleType(), nullable=True),
    StructField("dio",                     DoubleType(), nullable=True),
    StructField("dpo",                     DoubleType(), nullable=True),
    StructField("cash_conversion_cycle",   DoubleType(), nullable=True),
    # Growth
    StructField("revenue_mom",             DoubleType(), nullable=True),
    StructField("revenue_yoy",             DoubleType(), nullable=True),
    StructField("net_income_yoy",          DoubleType(), nullable=True),
    StructField("revenue_ttm",             DoubleType(), nullable=True),
    # Unit economics
    StructField("customers",               LongType(),   nullable=True),
    StructField("revenue_per_customer",    DoubleType(), nullable=True),
    StructField("churn_rate",              DoubleType(), nullable=True),
    # Macro
    StructField("inflation_rate",          DoubleType(), nullable=True),
    StructField("interest_rate",           DoubleType(), nullable=True),
    StructField("gdp_growth",              DoubleType(), nullable=True),
    StructField("usd_zar",                 DoubleType(), nullable=True),
    StructField("real_revenue_growth",     DoubleType(), nullable=True),
    StructField("interest_coverage",       DoubleType(), nullable=True),
    # Metadata
    StructField("_processed_at",           TimestampType(), nullable=False),
    StructField("_run_id",                 StringType(),    nullable=False),
    StructField("_schema_version",         StringType(),    nullable=False),
])

DQ_LOG = StructType([
    StructField("run_id",           StringType(),    nullable=False),
    StructField("table_name",       StringType(),    nullable=False),
    StructField("check_name",       StringType(),    nullable=False),
    StructField("check_type",       StringType(),    nullable=False),  # null|duplicate|range|business_rule
    StructField("column_name",      StringType(),    nullable=True),
    StructField("rows_checked",     LongType(),      nullable=False),
    StructField("rows_failed",      LongType(),      nullable=False),
    StructField("failure_pct",      DoubleType(),    nullable=False),
    StructField("threshold",        DoubleType(),    nullable=True),
    StructField("passed",           BooleanType(),   nullable=False),
    StructField("details",          StringType(),    nullable=True),
    StructField("checked_at",       TimestampType(), nullable=False),
])

# ─────────────────────────────────────────────────────────────────────────────
# GOLD SCHEMAS
# ─────────────────────────────────────────────────────────────────────────────

GOLD_ANNUAL_FORECAST = StructType([
    StructField("year",                IntegerType(), nullable=False),
    StructField("company",             StringType(),  nullable=False),
    StructField("scenario",            StringType(),  nullable=False),
    StructField("revenue",             DoubleType(),  nullable=False),
    StructField("cogs",                DoubleType(),  nullable=True),
    StructField("gross_profit",        DoubleType(),  nullable=True),
    StructField("opex",                DoubleType(),  nullable=True),
    StructField("rd_expense",          DoubleType(),  nullable=True),
    StructField("sga_expense",         DoubleType(),  nullable=True),
    StructField("ebitda",              DoubleType(),  nullable=True),
    StructField("depreciation",        DoubleType(),  nullable=True),
    StructField("ebit",                DoubleType(),  nullable=True),
    StructField("interest_expense",    DoubleType(),  nullable=True),
    StructField("ebt",                 DoubleType(),  nullable=True),
    StructField("tax_expense",         DoubleType(),  nullable=True),
    StructField("net_income",          DoubleType(),  nullable=True),
    StructField("cfo",                 DoubleType(),  nullable=True),
    StructField("capex",               DoubleType(),  nullable=True),
    StructField("fcf",                 DoubleType(),  nullable=True),
    StructField("gross_margin",        DoubleType(),  nullable=True),
    StructField("ebitda_margin",       DoubleType(),  nullable=True),
    StructField("net_margin",          DoubleType(),  nullable=True),
    StructField("revenue_growth_yoy",  DoubleType(),  nullable=True),
    # Metadata
    StructField("is_forecast",         BooleanType(), nullable=False),
    StructField("forecast_model",      StringType(),  nullable=True),
    StructField("_forecast_date",      DateType(),    nullable=False),
    StructField("_run_id",             StringType(),  nullable=False),
    StructField("_schema_version",     StringType(),  nullable=False),
])

AUDIT_LOG = StructType([
    StructField("run_id",              StringType(),    nullable=False),
    StructField("event_type",          StringType(),    nullable=False),  # pipeline_start|stage_complete|dq_fail|export
    StructField("stage",               StringType(),    nullable=True),
    StructField("table_name",          StringType(),    nullable=True),
    StructField("rows_affected",       LongType(),      nullable=True),
    StructField("message",             StringType(),    nullable=True),
    StructField("severity",            StringType(),    nullable=False),  # INFO|WARN|ERROR
    StructField("metadata_json",       StringType(),    nullable=True),
    StructField("recorded_at",         TimestampType(), nullable=False),
])

# ─────────────────────────────────────────────────────────────────────────────
# SCHEMA REGISTRY  (lookup by table name)
# ─────────────────────────────────────────────────────────────────────────────
SCHEMA_REGISTRY = {
    "bronze_company_financials" : BRONZE_FINANCIALS,
    "bronze_macro_indicators"   : BRONZE_MACRO,
    "bronze_pipeline_run_log"   : PIPELINE_RUN_LOG,
    "silver_monthly_financials" : SILVER_MONTHLY,
    "silver_data_quality_log"   : DQ_LOG,
    "gold_annual_forecast"      : GOLD_ANNUAL_FORECAST,
    "gold_audit_log"            : AUDIT_LOG,
}

def get_schema(table_name: str):
    """Retrieve schema by table name. Raises KeyError if not registered."""
    if table_name not in SCHEMA_REGISTRY:
        raise KeyError(
            f"Schema not found for '{table_name}'. "
            f"Registered tables: {list(SCHEMA_REGISTRY.keys())}"
        )
    return SCHEMA_REGISTRY[table_name]
