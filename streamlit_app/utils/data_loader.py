"""
utils/data_loader.py
====================
Loads data for the Streamlit app.
Priority: CSV exports from Databricks → synthetic fallback
"""

import os
import pandas as pd
import numpy as np
import streamlit as st


EXPORT_DIR = os.getenv("EXPORT_DIR", "./exports")


def _try_csv(name: str) -> pd.DataFrame | None:
    path = os.path.join(EXPORT_DIR, f"{name}.csv")
    if os.path.exists(path):
        try:
            df = pd.read_csv(path)
            for col in df.columns:
                try: df[col] = pd.to_numeric(df[col])
                except: pass
            return df
        except Exception as e:
            st.warning(f"Could not read {name}.csv: {e}")
    return None


def _synthetic_hist() -> pd.DataFrame:
    np.random.seed(42)
    years = list(range(2018, 2024))
    n = len(years)
    rev = np.linspace(6_500_000, 13_800_000, n)
    gm  = np.linspace(0.57, 0.61, n)
    em  = np.linspace(0.17, 0.23, n)
    nm  = np.linspace(0.09, 0.14, n)
    return pd.DataFrame({
        "year": years, "company": "AcmeCorp",
        "revenue": rev, "cogs": rev*(1-gm),
        "gross_profit": rev*gm,
        "opex": rev*0.20, "rd_expense": rev*0.09, "sga_expense": rev*0.11,
        "ebitda": rev*em, "depreciation": rev*0.04,
        "ebit": rev*(em-0.04), "interest_expense": np.full(n, 150_000),
        "ebt": rev*(em-0.04)-150_000,
        "tax_expense": (rev*(em-0.04)-150_000)*0.28,
        "net_income": rev*nm, "capex": rev*0.055,
        "accounts_receivable": rev*0.12, "inventory": rev*0.08,
        "accounts_payable": rev*0.06, "working_capital": rev*0.14,
        "customers_eoy": np.linspace(1200, 4200, n).astype(int),
        "arpu": rev/np.linspace(1200, 4200, n),
        "churn_rate_avg": np.linspace(0.055, 0.032, n),
        "gross_margin_avg": gm, "ebitda_margin_avg": em, "net_margin_avg": nm,
        "inflation_rate_avg": np.full(n, 5.5),
        "interest_rate_avg": np.full(n, 6.8),
        "gdp_growth_avg": np.full(n, 1.5),
    })


def _synthetic_forecast() -> pd.DataFrame:
    years = [2024, 2025, 2026]
    rev   = np.array([15_200_000, 17_100_000, 19_400_000])
    gm    = np.array([0.63, 0.64, 0.65])
    em    = np.array([0.25, 0.27, 0.29])
    nm    = np.array([0.15, 0.17, 0.19])
    return pd.DataFrame({
        "year": years, "company": "AcmeCorp", "scenario": "Base",
        "revenue": rev, "cogs": rev*(1-gm), "gross_profit": rev*gm,
        "opex": rev*0.18, "rd_expense": rev*0.09, "sga_expense": rev*0.10,
        "ebitda": rev*em, "depreciation": rev*0.04,
        "ebit": rev*(em-0.04), "interest_expense": np.full(3, 130_000),
        "ebt": rev*(em-0.04)-130_000,
        "tax_expense": (rev*(em-0.04)-130_000)*0.28,
        "net_income": rev*nm, "capex": rev*0.045,
        "accounts_receivable": rev*0.11, "inventory": rev*0.07,
        "accounts_payable": rev*0.055,
        "gross_margin": gm, "ebitda_margin": em, "net_margin": nm,
        "fcf": rev*nm*0.85, "cfo": rev*nm*1.1, "cfi": -rev*0.045,
        "cff": -rev*nm*0.3, "is_forecast": True,
    })


def _synthetic_scenarios(fcast_df: pd.DataFrame) -> pd.DataFrame:
    parts = []
    for sc, radj, madj in [("Base",1.00,0.000),("Bull",1.12,0.020),("Bear",0.88,-0.025)]:
        d = fcast_df.copy()
        d["scenario"] = sc
        d["revenue"]      *= radj
        d["net_income"]    = d["revenue"] * (d["net_margin"] + madj*0.5)
        d["ebitda"]        = d["revenue"] * (d["ebitda_margin"] + madj*0.7)
        d["ebitda_margin"] = d["ebitda_margin"] + madj*0.7
        d["net_margin"]    = d["net_margin"] + madj*0.5
        d["fcf"]           = d["net_income"] * 0.85
        parts.append(d)
    return pd.concat(parts, ignore_index=True)


def _synthetic_monthly() -> pd.DataFrame:
    np.random.seed(42)
    dates = pd.date_range("2024-01-01", periods=36, freq="MS")
    n = len(dates)
    trend = np.linspace(1_100_000, 1_750_000, n)
    seas  = 90_000 * np.sin(2*np.pi*np.arange(n)/12 - np.pi/2)
    rev   = trend + seas + np.random.normal(0, 20_000, n)
    return pd.DataFrame({
        "date": dates, "revenue": rev,
        "revenue_lower": rev*0.88, "revenue_upper": rev*1.12,
        "net_income": rev*0.14, "ebitda": rev*0.26,
        "fcf": rev*0.12, "cfo": rev*0.18,
    })


@st.cache_data(ttl=300)
def load_all():
    """
    Load all datasets. Returns (hist, fcast, scenarios, monthly).
    Uses CSV exports if present, otherwise synthetic data.
    """
    hist      = _try_csv("historical_annual")
    fcast     = _try_csv("annual_forecast")
    scenarios = _try_csv("scenarios")
    monthly   = _try_csv("monthly_forecast")

    using_synthetic = []

    if hist is None:
        hist = _synthetic_hist()
        using_synthetic.append("historical")

    if fcast is None:
        fcast = _synthetic_forecast()
        using_synthetic.append("forecast")

    if scenarios is None:
        scenarios = _synthetic_scenarios(fcast)
        using_synthetic.append("scenarios")

    if monthly is None:
        monthly = _synthetic_monthly()
        using_synthetic.append("monthly")

    if using_synthetic:
        st.info(
            f"📂 Using synthetic data for: {', '.join(using_synthetic)}. "
            "Run the Databricks notebooks first, then copy CSVs to ./exports/ "
            "for real pipeline data.",
            icon="ℹ️"
        )

    # Ensure date column is datetime in monthly
    if "date" in monthly.columns:
        monthly["date"] = pd.to_datetime(monthly["date"])

    return hist, fcast, scenarios, monthly
