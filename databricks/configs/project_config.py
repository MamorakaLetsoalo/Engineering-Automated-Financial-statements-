"""
project_config.py
=================
Single source of truth for all project-wide configuration.
All notebooks import from here — no magic strings scattered across code.

Design decision: Python dict over YAML/JSON so it's importable directly
inside Databricks notebooks with zero file I/O overhead.
"""

from datetime import datetime

# ─────────────────────────────────────────────────────────────────────────────
# PROJECT IDENTITY
# ─────────────────────────────────────────────────────────────────────────────
PROJECT = {
    "name"        : "FinModel Pro",
    "version"     : "1.0.0",
    "company"     : "ML Corp",
    "currency"    : "ZAR",
    "fiscal_year_end": "December",
    "environment" : "dev",          # dev | staging | prod
    "created_by"  : "data_team",
    "created_at"  : "2024-01-01",
}

# ─────────────────────────────────────────────────────────────────────────────
# STORAGE PATHS  (Databricks DBFS)
# ─────────────────────────────────────────────────────────────────────────────
BASE_PATH = "/FileStore/finmodel_pro"

PATHS = {
    "bronze"         : f"{BASE_PATH}/bronze",
    "silver"         : f"{BASE_PATH}/silver",
    "gold"           : f"{BASE_PATH}/gold",
    "exports"        : f"{BASE_PATH}/exports",
    "logs"           : f"{BASE_PATH}/logs",
    "audit"          : f"{BASE_PATH}/audit",
    "checkpoints"    : f"{BASE_PATH}/checkpoints",
    "schemas"        : f"{BASE_PATH}/schemas",
    "quarantine"     : f"{BASE_PATH}/quarantine",   # failed DQ records
}

# ─────────────────────────────────────────────────────────────────────────────
# TABLE REGISTRY  (Delta table names)
# ─────────────────────────────────────────────────────────────────────────────
TABLES = {
    # Bronze
    "bronze_financials"     : "bronze_company_financials",
    "bronze_macro"          : "bronze_macro_indicators",
    "bronze_run_log"        : "bronze_pipeline_run_log",

    # Silver
    "silver_monthly"        : "silver_monthly_financials",
    "silver_annual"         : "silver_annual_financials",
    "silver_dq_log"         : "silver_data_quality_log",

    # Gold
    "gold_monthly_forecast" : "gold_monthly_forecast",
    "gold_annual_forecast"  : "gold_annual_forecast",
    "gold_scenarios"        : "gold_scenario_forecasts",
    "gold_income_stmt"      : "gold_income_statement",
    "gold_balance_sheet"    : "gold_balance_sheet",
    "gold_cash_flow"        : "gold_cash_flow_statement",
    "gold_dcf"              : "gold_dcf_valuation",
    "gold_audit_log"        : "gold_audit_log",
}

# ─────────────────────────────────────────────────────────────────────────────
# DATA INGESTION
# ─────────────────────────────────────────────────────────────────────────────
INGESTION = {
    "start_date"        : "2018-01-01",
    "end_date"          : "2023-12-31",
    "forecast_years"    : 3,
    "fred_api_key_env"  : "FRED_API_KEY",       # env var name, not the key itself
    "fred_series": {
        "inflation"     : "CPIAUCSL",
        "interest_rate" : "FEDFUNDS",
        "gdp_growth"    : "A191RL1Q225SBEA",
        "unemployment"  : "UNRATE",
    },
    # Synthetic data seeds
    "random_seed"       : 42,
    "n_customers_start" : 1_200,
    "n_customers_end"   : 4_500,
    "revenue_start"     : 500_000,
    "revenue_end"       : 1_200_000,
}

# ─────────────────────────────────────────────────────────────────────────────
# DATA QUALITY THRESHOLDS
# ─────────────────────────────────────────────────────────────────────────────
DQ = {
    # Completeness
    "max_null_pct"              : 0.05,     # fail if >5% nulls in any column
    # Freshness
    "max_data_age_days"         : 45,       # warn if latest record >45 days old
    # Revenue bounds
    "revenue_min"               : 50_000,
    "revenue_max"               : 50_000_000,
    # Margin sanity
    "gross_margin_min"          : 0.10,
    "gross_margin_max"          : 0.95,
    "ebitda_margin_min"         : -0.50,
    "ebitda_margin_max"         : 0.80,
    # Balance sheet
    "bs_tolerance"              : 10.0,     # R10 allowed rounding error
    # Forecast
    "max_revenue_growth_yoy"    : 2.00,     # flag if >200% YoY — likely error
    "min_revenue_growth_yoy"    : -0.80,    # flag if < -80% YoY
    # Pipeline
    "dq_fail_threshold"         : 0.10,     # abort pipeline if >10% records fail DQ
}

# ─────────────────────────────────────────────────────────────────────────────
# FINANCIAL MODEL ASSUMPTIONS
# ─────────────────────────────────────────────────────────────────────────────
MODEL = {
    # Tax
    "corporate_tax_rate"        : 0.28,     # South African CIT rate
    "dividend_payout_ratio"     : 0.30,

    # Working capital ratios (days)
    "target_dso_days"           : 45,
    "target_dio_days"           : 60,
    "target_dpo_days"           : 35,

    # Capex
    "capex_pct_revenue"         : 0.05,

    # Debt
    "annual_debt_repayment_pct" : 0.05,
    "implied_interest_rate"     : 0.07,

    # DCF defaults
    "default_wacc"              : 0.12,
    "default_terminal_growth"   : 0.025,
    "dcf_confidence_interval"   : 0.80,
}

# ─────────────────────────────────────────────────────────────────────────────
# PROPHET FORECASTING
# ─────────────────────────────────────────────────────────────────────────────
PROPHET = {
    "changepoint_prior_scale"   : 0.15,
    "seasonality_mode"          : "multiplicative",
    "interval_width"            : 0.80,
    "yearly_seasonality"        : True,
    "weekly_seasonality"        : False,
    "daily_seasonality"         : False,
}

# ─────────────────────────────────────────────────────────────────────────────
# SCENARIO DEFINITIONS
# ─────────────────────────────────────────────────────────────────────────────
SCENARIOS = {
    "Base": {"revenue_adj": 1.00, "margin_adj":  0.000, "label": "Base Case"},
    "Bull": {"revenue_adj": 1.12, "margin_adj":  0.020, "label": "Bull Case (+12% rev, +2pp margin)"},
    "Bear": {"revenue_adj": 0.88, "margin_adj": -0.025, "label": "Bear Case (-12% rev, -2.5pp margin)"},
}

# ─────────────────────────────────────────────────────────────────────────────
# PARTITIONING STRATEGY
# ─────────────────────────────────────────────────────────────────────────────
PARTITIONING = {
    "bronze": ["year", "month"],     # partition by time for incremental loads
    "silver": ["year"],
    "gold"  : ["year"],
}

# ─────────────────────────────────────────────────────────────────────────────
# ORCHESTRATION
# ─────────────────────────────────────────────────────────────────────────────
PIPELINE = {
    "max_retries"           : 3,
    "retry_delay_seconds"   : 30,
    "timeout_minutes"       : 60,
    "stages": [
        "01_ingest_financials",
        "02_ingest_macro",
        "03_validate_bronze",
        "04_silver_transforms",
        "05_forecasting",
        "06_financial_statements",
        "07_export",
    ],
    # Which stages can be skipped if upstream data unchanged
    "incremental_stages"    : ["04_silver_transforms", "05_forecasting"],
}

# ─────────────────────────────────────────────────────────────────────────────
# OBSERVABILITY
# ─────────────────────────────────────────────────────────────────────────────
MONITORING = {
    "log_level"             : "INFO",
    "alert_on_dq_fail"      : True,
    "alert_on_pipeline_fail": True,
    "metrics_to_track"      : [
        "rows_ingested", "rows_failed_dq", "rows_quarantined",
        "pipeline_duration_seconds", "forecast_mape",
        "bs_balance_error", "cf_reconciliation_error",
    ],
}

# ─────────────────────────────────────────────────────────────────────────────
# SCHEMA VERSIONS  (for reproducibility tracking)
# ─────────────────────────────────────────────────────────────────────────────
SCHEMA_VERSIONS = {
    "bronze_financials" : "v1.2",
    "silver_monthly"    : "v1.1",
    "gold_forecast"     : "v1.0",
}

def get_run_id() -> str:
    """Generate unique run ID for audit trail."""
    return datetime.utcnow().strftime("run_%Y%m%d_%H%M%S")

def get_config_snapshot() -> dict:
    """Return full config as dict for audit logging."""
    return {
        "project"    : PROJECT,
        "paths"      : PATHS,
        "dq"         : DQ,
        "model"      : MODEL,
        "scenarios"  : SCENARIOS,
        "pipeline"   : PIPELINE,
        "captured_at": datetime.utcnow().isoformat(),
    }
