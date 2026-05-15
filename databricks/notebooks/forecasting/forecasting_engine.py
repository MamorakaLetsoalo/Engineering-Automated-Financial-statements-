# Databricks notebook source
# MAGIC %md
# MAGIC # 📈 Notebook 3: Forecasting Engine (Gold Layer)
# MAGIC **Pipeline:** Silver historical data → Gold forecasts (3-year horizon)
# MAGIC
# MAGIC Models:
# MAGIC - Revenue: Prophet (trend + seasonality) + driver-based
# MAGIC - Costs: margin-based regression from revenue
# MAGIC - Working capital: ratio-based projections
# MAGIC - Cash flow: derived from P&L + capex + working capital changes

# COMMAND ----------
# MAGIC %md ## 1. Install & import

# COMMAND ----------
%pip install prophet scikit-learn --quiet

# COMMAND ----------
import pandas as pd
import numpy as np
from prophet import Prophet
from sklearn.linear_model import LinearRegression
import warnings
warnings.filterwarnings("ignore")

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()

SILVER_PATH = "/FileStore/financial_model/silver"
GOLD_PATH   = "/FileStore/financial_model/gold"
FORECAST_YEARS = 3

print(f"✅ Libraries loaded — forecasting {FORECAST_YEARS} years ahead")

# COMMAND ----------
# MAGIC %md ## 2. Load Silver data

# COMMAND ----------
df_monthly = (spark.read.format("delta")
              .load(f"{SILVER_PATH}/monthly_financials")
              .toPandas()
              .sort_values("date"))

df_annual  = (spark.read.format("delta")
              .load(f"{SILVER_PATH}/annual_financials")
              .toPandas()
              .sort_values("year"))

df_monthly["date"] = pd.to_datetime(df_monthly["date"])
print(f"✅ Monthly: {len(df_monthly)} records | Annual: {len(df_annual)} years")

# COMMAND ----------
# MAGIC %md ## 3. Revenue forecast (Prophet)

# COMMAND ----------
def forecast_revenue_prophet(df: pd.DataFrame, periods: int = 36) -> pd.DataFrame:
    """
    Uses Meta's Prophet to model:
    - Overall trend
    - Annual seasonality
    - Monthly seasonality
    Returns forecast with uncertainty intervals.
    """
    prophet_df = df[["date", "revenue"]].rename(columns={"date": "ds", "revenue": "y"})

    model = Prophet(
        yearly_seasonality=True,
        weekly_seasonality=False,
        daily_seasonality=False,
        seasonality_mode="multiplicative",   # revenue scales with trend
        changepoint_prior_scale=0.15,        # moderate flexibility
        interval_width=0.80,                 # 80% confidence interval
    )

    # Add macro regressors
    prophet_df["inflation"]  = df["inflation_rate"].fillna(df["inflation_rate"].mean())
    prophet_df["interest"]   = df["interest_rate"].fillna(df["interest_rate"].mean())
    model.add_regressor("inflation")
    model.add_regressor("interest")

    model.fit(prophet_df)

    future = model.make_future_dataframe(periods=periods, freq="MS")

    # Extend macro regressors for forecast period (use last known values + small trend)
    last_inflation = df["inflation_rate"].iloc[-1]
    last_interest  = df["interest_rate"].iloc[-1]
    future["inflation"] = np.where(
        future["ds"] > prophet_df["ds"].max(),
        last_inflation + np.random.normal(0, 0.3, len(future)),
        prophet_df["inflation"].reindex(range(len(future)), fill_value=last_inflation)
    )
    future["interest"] = np.where(
        future["ds"] > prophet_df["ds"].max(),
        last_interest + np.random.normal(0, 0.2, len(future)),
        prophet_df["interest"].reindex(range(len(future)), fill_value=last_interest)
    )
    # Fill NaN in regressors
    future["inflation"] = future["inflation"].fillna(last_inflation)
    future["interest"]  = future["interest"].fillna(last_interest)

    forecast = model.predict(future)
    forecast = forecast[["ds", "yhat", "yhat_lower", "yhat_upper",
                          "trend", "yearly"]].rename(columns={"ds": "date"})
    forecast.columns = ["date", "revenue_forecast", "revenue_lower",
                         "revenue_upper", "revenue_trend", "revenue_seasonal"]

    print(f"✅ Prophet revenue forecast: {len(forecast)} periods")
    return forecast

revenue_forecast = forecast_revenue_prophet(df_monthly, periods=FORECAST_YEARS * 12)

# Separate historical fit vs future forecast
cutoff = df_monthly["date"].max()
rev_future = revenue_forecast[revenue_forecast["date"] > cutoff].copy()
print(f"   Forecast horizon: {rev_future['date'].min().date()} → {rev_future['date'].max().date()}")
print(f"   Year 1 revenue forecast: R{rev_future.head(12)['revenue_forecast'].sum():,.0f}")

# COMMAND ----------
# MAGIC %md ## 4. Cost & margin forecasts (driver-based)

# COMMAND ----------
def forecast_costs(revenue_series: pd.Series,
                   historical_df: pd.DataFrame) -> pd.DataFrame:
    """
    Project costs using historical margin ratios with regression.
    Each cost line is modelled as a % of revenue, with a slight
    efficiency improvement trend built in.
    """
    n = len(revenue_series)

    # Fit linear regression on historical margins vs time
    hist = historical_df.copy()
    hist["t"] = np.arange(len(hist))

    def regress_ratio(series, t):
        reg = LinearRegression().fit(t.values.reshape(-1, 1), series.values)
        t_future = np.arange(len(hist), len(hist) + n).reshape(-1, 1)
        pred = reg.predict(t_future)
        # Cap improvement at ±3pp over forecast horizon
        base = series.mean()
        pred = np.clip(pred, base - 0.03, base + 0.03)
        return pred

    cogs_ratio  = regress_ratio(hist["cogs_ratio"],  hist["t"])
    opex_ratio  = regress_ratio(hist["opex_ratio"],  hist["t"])
    rd_ratio    = regress_ratio(hist["rd_ratio"],    hist["t"])
    sga_ratio   = regress_ratio(hist["sga_ratio"],   hist["t"])

    # Apply slight efficiency gains (management assumption)
    cogs_ratio  = cogs_ratio  * np.linspace(1.0, 0.97, n)  # 3% COGS improvement
    opex_ratio  = opex_ratio  * np.linspace(1.0, 0.95, n)  # 5% opex leverage

    rev   = revenue_series.values
    cogs  = rev * cogs_ratio
    opex  = rev * opex_ratio
    rd    = rev * rd_ratio
    sga   = rev * sga_ratio

    gross_profit = rev - cogs
    ebitda       = gross_profit - opex - rd - sga
    depr_ratio   = hist["depreciation"].mean() / hist["revenue"].mean()
    depreciation = rev * depr_ratio
    ebit         = ebitda - depreciation

    # Forecast interest using last known + rate trend
    interest = np.full(n, historical_df["interest_expense"].mean())
    ebt      = ebit - interest
    tax      = np.maximum(ebt * 0.28, 0)
    net_inc  = ebt - tax

    return pd.DataFrame({
        "revenue"         : rev.round(2),
        "cogs"            : cogs.round(2),
        "gross_profit"    : gross_profit.round(2),
        "opex"            : opex.round(2),
        "rd_expense"      : rd.round(2),
        "sga_expense"     : sga.round(2),
        "ebitda"          : ebitda.round(2),
        "depreciation"    : depreciation.round(2),
        "ebit"            : ebit.round(2),
        "interest_expense": interest.round(2),
        "ebt"             : ebt.round(2),
        "tax_expense"     : tax.round(2),
        "net_income"      : net_inc.round(2),
        "gross_margin"    : (gross_profit / rev).round(4),
        "ebitda_margin"   : (ebitda / rev).round(4),
        "net_margin"      : (net_inc / rev).round(4),
    })

cost_df = forecast_costs(rev_future["revenue_forecast"], df_monthly)
print(f"✅ Cost forecasts generated: {len(cost_df)} months")

# COMMAND ----------
# MAGIC %md ## 5. Working capital & cash flow forecast

# COMMAND ----------
def forecast_cashflow(pl_df: pd.DataFrame,
                      historical_df: pd.DataFrame) -> pd.DataFrame:
    """
    Builds cash flow statement from:
    - Net income
    - D&A add-back
    - Working capital changes
    - Capex (% of revenue)
    """
    hist = historical_df.copy()

    # Ratio-based working capital projections
    dso_avg = (hist["accounts_receivable"] / hist["revenue"] * 30).mean()
    dio_avg = (hist["inventory"]           / hist["cogs"]    * 30).mean()
    dpo_avg = (hist["accounts_payable"]    / hist["cogs"]    * 30).mean()
    capex_ratio = (hist["capex"] / hist["revenue"]).mean()

    ar     = pl_df["revenue"] / 30 * dso_avg
    inv    = pl_df["cogs"]    / 30 * dio_avg
    ap     = pl_df["cogs"]    / 30 * dpo_avg
    capex  = pl_df["revenue"] * capex_ratio

    # Change in working capital (month-over-month)
    wc     = ar + inv - ap
    d_wc   = wc.diff().fillna(0)

    # Operating cash flow
    cfo = pl_df["net_income"] + pl_df["depreciation"] - d_wc

    # Free cash flow
    fcf = cfo - capex

    # Cumulative cash position (starting from R0)
    cum_cash = fcf.cumsum()

    return pd.DataFrame({
        "net_income"           : pl_df["net_income"].round(2),
        "depreciation_addback" : pl_df["depreciation"].round(2),
        "change_in_wc"         : (-d_wc).round(2),
        "cfo"                  : cfo.round(2),
        "capex"                : (-capex).round(2),   # negative = outflow
        "fcf"                  : fcf.round(2),
        "cumulative_cash"      : cum_cash.round(2),
        "accounts_receivable"  : ar.round(2),
        "inventory"            : inv.round(2),
        "accounts_payable"     : ap.round(2),
    })

cf_df = forecast_cashflow(cost_df, df_monthly)
print(f"✅ Cash flow forecast: {len(cf_df)} months")

# COMMAND ----------
# MAGIC %md ## 6. Assemble monthly Gold forecast table

# COMMAND ----------
gold_monthly = rev_future[["date"]].copy().reset_index(drop=True)

# Combine all forecast components
for df_part in [cost_df.reset_index(drop=True),
                cf_df.reset_index(drop=True),
                rev_future[["revenue_lower", "revenue_upper",
                             "revenue_trend"]].reset_index(drop=True)]:
    for col in df_part.columns:
        gold_monthly[col] = df_part[col]

gold_monthly["company"]  = df_monthly["company"].iloc[0]
gold_monthly["is_forecast"] = True
gold_monthly["forecast_date"] = pd.Timestamp.today().date().isoformat()

print(f"✅ Gold monthly forecast: {len(gold_monthly)} rows, {len(gold_monthly.columns)} columns")

# COMMAND ----------
# MAGIC %md ## 7. Annual Gold forecast (for financial statements)

# COMMAND ----------
gold_monthly["year"] = pd.to_datetime(gold_monthly["date"]).dt.year

gold_annual = gold_monthly.groupby("year").agg(
    company         = ("company",          "first"),
    revenue         = ("revenue",          "sum"),
    cogs            = ("cogs",             "sum"),
    gross_profit    = ("gross_profit",     "sum"),
    opex            = ("opex",             "sum"),
    rd_expense      = ("rd_expense",       "sum"),
    sga_expense     = ("sga_expense",      "sum"),
    ebitda          = ("ebitda",           "sum"),
    depreciation    = ("depreciation",     "sum"),
    ebit            = ("ebit",             "sum"),
    interest_expense= ("interest_expense", "sum"),
    ebt             = ("ebt",             "sum"),
    tax_expense     = ("tax_expense",      "sum"),
    net_income      = ("net_income",       "sum"),
    cfo             = ("cfo",              "sum"),
    capex           = ("capex",            "sum"),
    fcf             = ("fcf",              "sum"),
    gross_margin    = ("gross_margin",     "mean"),
    ebitda_margin   = ("ebitda_margin",    "mean"),
    net_margin      = ("net_margin",       "mean"),
    accounts_receivable = ("accounts_receivable", "last"),
    inventory           = ("inventory",           "last"),
    accounts_payable    = ("accounts_payable",     "last"),
    is_forecast     = ("is_forecast",      "first"),
).reset_index()

print(f"✅ Gold annual forecast:")
for _, r in gold_annual.iterrows():
    print(f"   {int(r['year'])}: Revenue R{r['revenue']:>14,.0f} | "
          f"EBITDA {r['ebitda_margin']*100:.1f}% | "
          f"Net {r['net_margin']*100:.1f}% | "
          f"FCF R{r['fcf']:>12,.0f}")

# COMMAND ----------
# MAGIC %md ## 8. Scenario variants (base / bull / bear)

# COMMAND ----------
def build_scenario(gold_annual: pd.DataFrame,
                   revenue_adj: float,
                   margin_adj: float,
                   label: str) -> pd.DataFrame:
    """
    Revenue_adj: multiplier on base revenue (e.g. 1.10 = +10%)
    Margin_adj:  absolute pp adjustment on margins (e.g. 0.02 = +2pp)
    """
    df = gold_annual.copy()
    df["scenario"]       = label
    df["revenue"]        = df["revenue"]    * revenue_adj
    df["gross_profit"]   = df["revenue"]    * (df["gross_margin"] + margin_adj)
    df["ebitda"]         = df["revenue"]    * (df["ebitda_margin"] + margin_adj * 0.7)
    df["net_income"]     = df["revenue"]    * (df["net_margin"]   + margin_adj * 0.5)
    df["fcf"]            = df["net_income"] * 0.85
    return df

scenarios = pd.concat([
    build_scenario(gold_annual, 1.00,  0.000, "Base"),
    build_scenario(gold_annual, 1.12,  0.020, "Bull"),
    build_scenario(gold_annual, 0.88, -0.025, "Bear"),
], ignore_index=True)

print(f"✅ Scenario table: {len(scenarios)} rows (Base/Bull/Bear × {len(gold_annual)} years)")
display(scenarios[["year","scenario","revenue","ebitda_margin","net_income","fcf"]])

# COMMAND ----------
# MAGIC %md ## 9. Write Gold Delta tables

# COMMAND ----------
def write_gold(df: pd.DataFrame, table_name: str, path: str):
    sdf = spark.createDataFrame(df)
    (sdf.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"{path}/{table_name}"))
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS gold_{table_name}
        USING DELTA LOCATION '{path}/{table_name}'
    """)
    print(f"✅ gold_{table_name}: {len(df)} rows → {path}/{table_name}")

write_gold(gold_monthly, "monthly_forecast",  GOLD_PATH)
write_gold(gold_annual,  "annual_forecast",   GOLD_PATH)
write_gold(scenarios,    "scenario_forecasts", GOLD_PATH)

# ── Also write CSVs for Power BI / Streamlit ──────────────────────────────
csv_path = "/FileStore/financial_model/exports"
dbutils.fs.mkdirs(csv_path)

for name, df in [("monthly_forecast", gold_monthly),
                  ("annual_forecast",  gold_annual),
                  ("scenarios",        scenarios),
                  ("historical_annual", df_annual)]:
    df.to_csv(f"/dbfs{csv_path}/{name}.csv", index=False)
    print(f"✅ CSV export: {csv_path}/{name}.csv")

# COMMAND ----------
print("=" * 55)
print("GOLD LAYER — FORECAST COMPLETE")
print("=" * 55)
print(f"\n  Forecast horizon: {gold_monthly['date'].min().date()} → {gold_monthly['date'].max().date()}")
print(f"  Monthly records : {len(gold_monthly)}")
print(f"  Annual records  : {len(gold_annual)}")
print(f"  Scenarios       : Base / Bull / Bear")
print("\n✅ Gold layer complete — proceed to Notebook 04")
