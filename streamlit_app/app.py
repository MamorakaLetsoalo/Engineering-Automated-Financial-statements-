"""
Financial Modelling & Forecasting Platform
Streamlit App — Phase 2
Run: streamlit run app.py
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import os
import warnings
warnings.filterwarnings("ignore")

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="ML Corp Finance",
    page_icon="💰",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

  html, body, [class*="css"] {
    font-family: 'Inter', sans-serif;
    background: #F5F0E8 !important;
    color: #000000 !important;
  }
  .main, .mainContent, .block-container, [data-testid="stAppViewContainer"] {
    background: #F5F0E8 !important;
  }
  .css-1v3fvcr, .css-18e3th9, .css-1d391kg, .css-1lcbmhc {
    background: #F5F0E8 !important;
  }

  /* Sidebar */
  [data-testid="stSidebar"] {
    background: #ffffff !important;
    border-right: 1px solid #d8cdb9 !important;
  }
  button[title="Main menu"], button[title="Open main menu"],
  button[title="Settings"] {
    display: none !important;
  }
  .sidebar-logo {
    font-size: 24px;
    font-weight: 700;
    color: #1f2937;
    padding: 16px 0 8px 0;
  }
  .sidebar-logo .logo-icon {
    display: inline-block;
    margin-right: 8px;
    font-size: 24px;
    vertical-align: middle;
  }
  .sidebar-subtitle {
    color: #6b6b6b;
    font-size: 13px;
    line-height: 1.6;
    margin-bottom: 18px;
  }
  .stRadio > div > label {
    border-radius: 12px;
    margin-bottom: 6px;
    padding: 10px 12px;
  }
  .stRadio > div > label:hover {
    background: rgba(107,143,113,0.12);
  }

  /* KPI cards */
  .kpi-card {
    background: #ffffff;
    border-radius: 16px;
    padding: 22px 24px;
    margin-bottom: 18px;
    box-shadow: 0 12px 30px rgba(0,0,0,0.05);
  }
  .kpi-card--sage { border-top: 4px solid #6B8F71; }
  .kpi-card--terracotta { border-top: 4px solid #C4785A; }
  .kpi-card--stone { border-top: 4px solid #B5A48A; }
  .kpi-card--slate { border-top: 4px solid #5C7A9E; }
  .kpi-label {
    font-size: 11px;
    font-weight: 600;
    letter-spacing: 0.16em;
    color: #6b6b6b;
    text-transform: uppercase;
    margin-bottom: 10px;
  }
  .kpi-value {
    font-size: 28px;
    font-weight: 600;
    color: #000000;
    line-height: 1.1;
  }
  .kpi-delta-pos { color: #6B8F71; font-size: 12px; margin-top: 6px; }
  .kpi-delta-neg { color: #C4785A; font-size: 12px; margin-top: 6px; }

  /* Section headers */
  .section-header {
    font-size: 12px;
    font-weight: 700;
    letter-spacing: 0.18em;
    color: #20232a;
    text-transform: uppercase;
    border-bottom: 1px solid #e1d8cd;
    padding-bottom: 10px;
    margin: 26px 0 18px 0;
  }

  .fin-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 13px;
    color: #20232a;
  }
  .fin-table th, .fin-table td {
    padding: 12px 10px;
    text-align: left;
    border-bottom: 1px solid #e5dfd5;
    white-space: nowrap;
  }
  .fin-table th {
    color: #475569;
    font-weight: 700;
  }
  .fin-table tr.total-row td {
    font-weight: 700;
    background: #f9f5ef;
  }
  .fin-table tr.section-row td {
    background: #f2ebe1;
    color: #475569;
    font-size: 11px;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    padding: 10px;
  }
  .positive { color: #1f6d3b; }
  .negative { color: #8b1f2b; }

  /* Plotly containers */
  [data-testid="stPlotlyChart"] {
    border-radius: 18px !important;
    overflow: hidden !important;
  }
  .plotly-graph-div {
    border-radius: 18px !important;
  }

  /* Tables */
  .stDataFrame table {
    border: none !important;
    font-size: 13px;
  }
  .stDataFrame td, .stDataFrame th {
    border: none !important;
    color: #000000 !important;
  }

  /* Text */
  h1, h2, h3, h4, h5, h6, p, span, label {
    color: #000000 !important;
  }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# DATA LAYER
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_data
def load_data():
    """
    Load from CSVs exported by Databricks.
    Falls back to synthetic generation if CSVs not found.
    """
    export_dir = os.getenv("EXPORT_DIR", "./exports")

    def try_csv(name):
        path = os.path.join(export_dir, f"{name}.csv")
        if os.path.exists(path):
            return pd.read_csv(path)
        return None

    hist = try_csv("historical_annual")
    fcast = try_csv("annual_forecast")
    scenarios = try_csv("scenarios")
    monthly = try_csv("monthly_forecast")

    if hist is None or fcast is None or scenarios is None or monthly is None:
        # ── Synthetic fallback (runs without Databricks) ──────────────────
        st.info("📂 No exported CSVs found — running with synthetic demo data. "
                "Run the Databricks notebooks first, then copy CSVs to ./exports/")
        hist, fcast, scenarios, monthly = _generate_synthetic()

    return hist, fcast, scenarios, monthly


def _generate_synthetic():
    """Reproduce data matching Notebook 01–04 logic for standalone demo."""
    np.random.seed(42)
    hist_years = list(range(2018, 2024))
    n_hist = len(hist_years)

    rev = np.linspace(6_500_000, 13_800_000, n_hist)
    gm  = np.linspace(0.57, 0.61, n_hist)
    em  = np.linspace(0.17, 0.23, n_hist)
    nm  = np.linspace(0.09, 0.14, n_hist)

    hist = pd.DataFrame({
        "year": hist_years,
        "company": "ML Corp",
        "revenue": rev,
        "cogs": rev * (1 - gm),
        "gross_profit": rev * gm,
        "opex": rev * 0.20,
        "rd_expense": rev * 0.09,
        "sga_expense": rev * 0.11,
        "ebitda": rev * em,
        "depreciation": rev * 0.04,
        "ebit": rev * (em - 0.04),
        "interest_expense": np.full(n_hist, 150_000),
        "ebt": rev * (em - 0.04) - 150_000,
        "tax_expense": (rev * (em - 0.04) - 150_000) * 0.28,
        "net_income": rev * nm,
        "capex": rev * 0.055,
        "accounts_receivable": rev * 0.12,
        "inventory": rev * 0.08,
        "accounts_payable": rev * 0.06,
        "working_capital": rev * 0.14,
        "customers_eoy": np.linspace(1200, 4200, n_hist).astype(int),
        "arpu": rev / np.linspace(1200, 4200, n_hist),
        "churn_rate_avg": np.linspace(0.055, 0.032, n_hist),
        "gross_margin_avg": gm,
        "ebitda_margin_avg": em,
        "net_margin_avg": nm,
        "inflation_rate_avg": np.full(n_hist, 5.5),
        "interest_rate_avg": np.full(n_hist, 7.0),
        "gdp_growth_avg": np.full(n_hist, 1.8),
    })

    fcast_years = [2024, 2025, 2026]
    rev_f = np.array([15_200_000, 17_100_000, 19_400_000])
    gm_f  = np.array([0.63, 0.64, 0.65])
    em_f  = np.array([0.25, 0.27, 0.29])
    nm_f  = np.array([0.15, 0.17, 0.19])

    fcast = pd.DataFrame({
        "year": fcast_years,
        "company": "ML Corp",
        "revenue": rev_f,
        "cogs": rev_f * (1 - gm_f),
        "gross_profit": rev_f * gm_f,
        "opex": rev_f * 0.18,
        "rd_expense": rev_f * 0.09,
        "sga_expense": rev_f * 0.10,
        "ebitda": rev_f * em_f,
        "depreciation": rev_f * 0.04,
        "ebit": rev_f * (em_f - 0.04),
        "interest_expense": np.full(3, 130_000),
        "ebt": rev_f * (em_f - 0.04) - 130_000,
        "tax_expense": (rev_f * (em_f - 0.04) - 130_000) * 0.28,
        "net_income": rev_f * nm_f,
        "capex": rev_f * 0.045,
        "accounts_receivable": rev_f * 0.11,
        "inventory": rev_f * 0.07,
        "accounts_payable": rev_f * 0.055,
        "gross_margin": gm_f,
        "ebitda_margin": em_f,
        "net_margin": nm_f,
        "fcf": rev_f * nm_f * 0.85,
        "cfo": rev_f * nm_f * 1.1,
        "is_forecast": True,
    })

    def make_scenario(fcast, radj, madj, label):
        d = fcast.copy()
        d["scenario"] = label
        d["revenue"]     *= radj
        d["net_income"]   = d["revenue"] * (nm_f + madj * 0.5)
        d["ebitda"]       = d["revenue"] * (em_f + madj * 0.7)
        d["ebitda_margin"]= em_f + madj * 0.7
        d["net_margin"]   = nm_f + madj * 0.5
        d["fcf"]          = d["net_income"] * 0.85
        return d

    base = make_scenario(fcast, 1.00,  0.000, "Base")
    bull = make_scenario(fcast, 1.12,  0.020, "Bull")
    bear = make_scenario(fcast, 0.88, -0.025, "Bear")
    scenarios = pd.concat([base, bull, bear])

    # Monthly forecast
    dates = pd.date_range("2024-01-01", periods=36, freq="MS")
    n = len(dates)
    trend = np.linspace(1_100_000, 1_750_000, n)
    seas  = 90_000 * np.sin(2 * np.pi * np.arange(n) / 12 - np.pi / 2)
    rev_m = trend + seas + np.random.normal(0, 20_000, n)
    monthly = pd.DataFrame({
        "date": dates,
        "revenue": rev_m,
        "revenue_lower": rev_m * 0.88,
        "revenue_upper": rev_m * 1.12,
        "net_income": rev_m * 0.14,
        "ebitda": rev_m * 0.26,
        "fcf": rev_m * 0.12,
        "cfo": rev_m * 0.18,
    })

    return hist, fcast, scenarios, monthly


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def fmt_r(v, decimals=0):
    if pd.isna(v): return "—"
    if abs(v) >= 1_000_000:
        return f"R {v/1_000_000:.{decimals}f}M"
    return f"R {v:>,.0f}"

def fmt_pct(v):
    if pd.isna(v): return "—"
    return f"{v*100:.1f}%"

def color_val(v, fmt_fn=fmt_r):
    txt = fmt_fn(v)
    cls = "positive" if v >= 0 else "negative"
    return f'<span class="{cls}">{txt}</span>'

COLORS = {
    "blue"   : "#1f77b4",
    "green"  : "#6b8f71",
    "red"    : "#a5362a",
    "yellow" : "#b78a44",
    "purple" : "#7c5a8a",
    "teal"   : "#4e7d74",
    "bg"     : "#f6f1e6",
    "bg2"    : "#ffffff",
    "border" : "#d5c8b6",
    "text"   : "#1f2937",
    "muted"  : "#4b5563",
}

PLOT_LAYOUT = dict(
    paper_bgcolor=COLORS["bg"],
    plot_bgcolor=COLORS["bg2"],
    font=dict(family="IBM Plex Sans", color=COLORS["text"], size=12),
    xaxis=dict(gridcolor=COLORS["border"], linecolor=COLORS["border"],
               tickcolor=COLORS["green"], tickfont=dict(color=COLORS["green"]), titlefont=dict(color=COLORS["green"]), showgrid=True),
    yaxis=dict(gridcolor=COLORS["border"], linecolor=COLORS["border"],
               tickcolor=COLORS["green"], tickfont=dict(color=COLORS["green"]), titlefont=dict(color=COLORS["green"]), showgrid=True),
    legend=dict(bgcolor=COLORS["bg2"], bordercolor=COLORS["border"],
                borderwidth=1, font=dict(color=COLORS["green"]), orientation="h", yanchor="top", y=-0.15, xanchor="center", x=0.5),
    margin=dict(l=0, r=0, t=50, b=70),
    hovermode="x unified",
    title_x=0.0,
    title_xanchor="left",
    title_font=dict(size=14, color=COLORS["green"], family="IBM Plex Sans"),
)

def apply_layout(fig, **kwargs):
    fig.update_layout(**PLOT_LAYOUT, **kwargs)
    # Add rounded corners styling with shapes
    fig.update_xaxes(showline=True, linewidth=1, linecolor=COLORS["border"], mirror=False)
    fig.update_yaxes(showline=True, linewidth=1, linecolor=COLORS["border"], mirror=False)
    return fig


# ─────────────────────────────────────────────────────────────────────────────
# DCF ENGINE
# ─────────────────────────────────────────────────────────────────────────────

def run_dcf(fcf_series, wacc, terminal_growth, net_debt=0):
    """
    Standard DCF:
    - Discount explicit FCF forecasts at WACC
    - Terminal value = Gordon Growth Model on final FCF
    - Enterprise value = PV of FCFs + PV of terminal value
    - Equity value = EV - net debt
    """
    n = len(fcf_series)
    discount_factors = [(1 / (1 + wacc) ** t) for t in range(1, n + 1)]

    pv_fcf = sum(f * d for f, d in zip(fcf_series, discount_factors))
    terminal_fcf = fcf_series.iloc[-1] * (1 + terminal_growth)
    terminal_value = terminal_fcf / (wacc - terminal_growth)
    pv_terminal = terminal_value * discount_factors[-1]

    enterprise_value = pv_fcf + pv_terminal
    equity_value = enterprise_value - net_debt

    return {
        "pv_fcf"           : pv_fcf,
        "terminal_value"   : terminal_value,
        "pv_terminal"      : pv_terminal,
        "enterprise_value" : enterprise_value,
        "equity_value"     : equity_value,
        "pv_fcf_pct"       : pv_fcf / enterprise_value,
        "pv_tv_pct"        : pv_terminal / enterprise_value,
    }


# ─────────────────────────────────────────────────────────────────────────────
# MAIN APP
# ─────────────────────────────────────────────────────────────────────────────

hist, fcast, scenarios, monthly = load_data()

# Ensure numeric
for df in [hist, fcast, scenarios]:
    for col in df.select_dtypes(include="object").columns:
        try: df[col] = pd.to_numeric(df[col])
        except: pass

combined = pd.concat([
    hist.assign(period_type="Historical"),
    fcast.assign(period_type="Forecast"),
], ignore_index=True).sort_values("year")

company_name = "ML Corp"

# ── Sidebar ───────────────────────────────────────────────────────────────────
nav_options = [
    "📈 Overview",
    "📋 Income Statement",
    "🏦 Balance Sheet",
    "💵 Cash Flow",
    "🎯 DCF Valuation",
    "⚡ Scenario Engine",
]

with st.sidebar:
    st.markdown(
        '<div class="sidebar-logo"><span class="logo-icon">💰</span>ML Corp<span class="logo-icon">💵</span></div>',
        unsafe_allow_html=True,
    )
    st.markdown('<div class="sidebar-subtitle">Natural financial insight for advisory and investor strategy.</div>', unsafe_allow_html=True)
    st.divider()
    selected_tab = st.radio("Navigation", nav_options, index=0, label_visibility="collapsed")
    st.divider()

    st.markdown("#### Scenario selection")
    st.markdown("Choose the forecast view that highlights strategic upside and downside outcomes.")
    scenario = st.selectbox("Active scenario", ["Base", "Bull", "Bear"], index=0)
    sc_data = scenarios[scenarios["scenario"] == scenario].sort_values("year")

    st.divider()
    st.markdown("#### DCF assumptions")
    wacc           = st.slider("Discount rate (WACC %)",           5.0, 25.0, 12.0, 0.5) / 100
    terminal_g     = st.slider("Terminal growth (%)", 0.5,  6.0,  2.5, 0.5) / 100
    net_debt_m     = st.number_input("Net debt (R million)", value=5.0, step=0.5)
    net_debt       = net_debt_m * 1_000_000
    shares_m       = st.number_input("Shares outstanding (million)", value=10.0, step=1.0)

    st.divider()
    st.markdown("#### Macro sensitivity")
    st.markdown("Stress test revenue and margin assumptions for a sharper view of downside risk.")
    rev_shock      = st.slider("Revenue impact (%)", -30, +30, 0, 5) / 100
    margin_shock   = st.slider("Margin shift (pp)", -10, +10, 0, 1) / 100

    st.divider()
    st.caption("Built for consulting-grade scenario review and capital planning.")

# ── Apply shocks to scenario ──────────────────────────────────────────────────
sc_adj = sc_data.copy()
if rev_shock != 0 or margin_shock != 0:
    sc_adj["revenue"]   = sc_adj["revenue"]    * (1 + rev_shock)
    sc_adj["ebitda"]    = sc_adj["revenue"]     * (sc_adj.get("ebitda_margin", sc_adj["ebitda"] / sc_data["revenue"]) + margin_shock)
    sc_adj["net_income"]= sc_adj["revenue"]     * (sc_adj.get("net_margin",    sc_adj["net_income"] / sc_data["revenue"]) + margin_shock * 0.6)
    sc_adj["fcf"]       = sc_adj["net_income"]  * 0.85

# ── Header ────────────────────────────────────────────────────────────────────
st.markdown(f"""
<div style="display:flex; flex-direction:column; gap:10px; margin-bottom:24px;">
  <div style="display:flex; align-items:center; gap:16px; flex-wrap:wrap;">
    <div style="font-size:32px; font-weight:700; color:#000000;">{company_name}</div>
    <div style="background:#e8e8e8; border:1px solid #c0c0c0; border-radius:20px;
         padding:6px 16px; font-size:13px; font-weight:600; color:#000000;">
      {scenario} Scenario
    </div>
    {"<div style='background:#e8e8e8;border-radius:20px;padding:6px 16px;font-size:13px;font-weight:600;color:#000000;border:1px solid #c0c0c0;'>⚡ Shocked</div>" if (rev_shock != 0 or margin_shock != 0) else ""}
  </div>
  <div style="color:#000000; font-size:16px; max-width:820px; line-height:1.5;">
    Business finance simplified: compare historical performance, scenario-driven forecasts, and valuation sensitivity in one clean, neutral dashboard.
  </div>
</div>
""", unsafe_allow_html=True)

# ── KPI Row ───────────────────────────────────────────────────────────────────
last_hist  = hist.iloc[-1]
last_fcast = sc_adj.iloc[-1] if len(sc_adj) > 0 else fcast.iloc[-1]

rev_growth = (last_fcast["revenue"] - last_hist["revenue"]) / last_hist["revenue"]
ni_growth  = (last_fcast["net_income"] - last_hist["net_income"]) / last_hist["net_income"]

k1, k2, k3, k4, k5 = st.columns(5)
with k1:
    st.metric("Last Hist. Revenue",  fmt_r(last_hist["revenue"]),
              f"{last_hist.get('revenue_yoy', last_hist.get('gross_margin_avg',0))*0:.0f}")
with k2:
    st.metric("Forecast Revenue (Y3)", fmt_r(last_fcast["revenue"]),
              f"+{rev_growth*100:.0f}% vs last year")
with k3:
    gm_col = "gross_margin" if "gross_margin" in last_fcast else "gross_margin_avg"
    gm_val = last_fcast.get(gm_col, last_fcast.get("gross_margin", 0))
    st.metric("Forecast Gross Margin", fmt_pct(gm_val))
with k4:
    em_col = "ebitda_margin" if "ebitda_margin" in last_fcast else "ebitda_margin_avg"
    em_val = last_fcast.get(em_col, 0)
    st.metric("Forecast EBITDA Margin", fmt_pct(em_val))
with k5:
    fcf_val = last_fcast.get("fcf", last_fcast["net_income"] * 0.85)
    st.metric("Forecast FCF (Y3)", fmt_r(fcf_val))

st.markdown("---")

# ── Tabs ──────────────────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════════════
# NAVIGATION
# The sidebar controls which section is shown.

# ══════════════════════════════════════════════════════════════════════════════
# TAB 1: OVERVIEW
# ══════════════════════════════════════════════════════════════════════════════
if selected_tab == "📈 Overview":
    col_l, col_r = st.columns([3, 2])

    with col_l:
        # Revenue waterfall — historical bars + forecast line + CI band
        fig = go.Figure()

        # CI band (forecast only)
        if "revenue_lower" in monthly.columns:
            fig.add_trace(go.Scatter(
                x=pd.concat([monthly["date"], monthly["date"][::-1]]),
                y=pd.concat([monthly["revenue_upper"], monthly["revenue_lower"][::-1]]),
                fill="toself", fillcolor="rgba(88,166,255,0.08)",
                line=dict(color="rgba(0,0,0,0)"),
                name="80% CI", hoverinfo="skip",
            ))

        fig.add_trace(go.Scatter(
            x=monthly["date"], y=monthly["revenue"],
            mode="lines", name="Forecast Revenue",
            fill="tozeroy", fillcolor="rgba(47,79,111,0.12)",
            line=dict(color=COLORS["blue"], width=2, dash="dash", shape="spline"),
        ))

        # Historical bars
        hist_monthly_approx = None
        fig.add_trace(go.Bar(
            x=[str(y) for y in hist["year"]],
            y=hist["revenue"],
            name="Historical Revenue",
            marker_color=COLORS["green"],
            opacity=0.7,
            width=0.5,
        ))

        apply_layout(fig, title="Revenue — Historical & Forecast",
                     yaxis_tickprefix="R ", yaxis_tickformat=",.0f",
                     height=340)
        st.plotly_chart(fig, width='stretch')

    with col_r:
        # Margin progression
        fig2 = go.Figure()
        all_years = list(hist["year"]) + list(sc_adj["year"])
        gm_vals   = (list(hist["gross_margin_avg"]) +
                     list(sc_adj.get("gross_margin", sc_adj.get("gross_margin_avg",
                          sc_adj["gross_profit"] / sc_adj["revenue"]))))
        em_vals   = (list(hist["ebitda_margin_avg"]) +
                     list(sc_adj.get("ebitda_margin", sc_adj["ebitda"] / sc_adj["revenue"])))
        nm_vals   = (list(hist["net_margin_avg"]) +
                     list(sc_adj.get("net_margin", sc_adj["net_income"] / sc_adj["revenue"])))

        hist_n = len(hist)
        for name, vals, color in [
            ("Gross Margin",  gm_vals, COLORS["green"]),
            ("EBITDA Margin", em_vals, COLORS["blue"]),
            ("Net Margin",    nm_vals, COLORS["purple"]),
        ]:
            fig2.add_trace(go.Scatter(
                x=all_years[:hist_n], y=[v*100 for v in vals[:hist_n]],
                mode="lines+markers", name=name,
                line=dict(color=color, width=2, shape="spline"),
                marker=dict(size=6),
            ))
            if len(all_years) > hist_n:
                fig2.add_trace(go.Scatter(
                    x=all_years[hist_n-1:], y=[v*100 for v in vals[hist_n-1:]],
                    mode="lines+markers", name=f"{name} (F)",
                    line=dict(color=color, width=2, dash="dot", shape="spline"),
                    marker=dict(size=6, symbol="diamond"),
                    showlegend=False,
                ))

        apply_layout(fig2, title="Margin Progression (%)",
                     yaxis_ticksuffix="%", height=340)
        st.plotly_chart(fig2, width='stretch')

    # Bottom row
    col_a, col_b, col_c = st.columns(3)
    with col_a:
        fig3 = go.Figure(go.Bar(
            x=[str(y) for y in hist["year"]],
            y=hist["net_income"],
            marker_color=[COLORS["green"] if v >= 0 else COLORS["red"]
                          for v in hist["net_income"]],
            name="Net Income",
        ))
        apply_layout(fig3, title="Historical Net Income",
                     yaxis_tickprefix="R ", yaxis_tickformat=",.0f", height=240)
        st.plotly_chart(fig3, width='stretch')

    with col_b:
        # Customers & ARPU
        if "customers_eoy" in hist.columns:
            fig4 = make_subplots(specs=[[{"secondary_y": True}]])
            fig4.add_trace(go.Bar(
                x=[str(y) for y in hist["year"]], y=hist["customers_eoy"],
                name="Customers", marker_color=COLORS["teal"], opacity=0.7,
            ), secondary_y=False)
            fig4.add_trace(go.Scatter(
                x=[str(y) for y in hist["year"]], y=hist["arpu"],
                name="ARPU", mode="lines+markers",
                line=dict(color=COLORS["yellow"], width=2),
            ), secondary_y=True)
            fig4.update_layout(**PLOT_LAYOUT, title="Customers & ARPU", height=240)
            st.plotly_chart(fig4, width='stretch')

    with col_c:
        # FCF
        fcf_vals = sc_adj.get("fcf", sc_adj["net_income"] * 0.85)
        fig5 = go.Figure(go.Bar(
            x=[str(y) for y in sc_adj["year"]],
            y=fcf_vals,
            marker_color=[COLORS["blue"] if v >= 0 else COLORS["red"] for v in fcf_vals],
            name="FCF",
        ))
        apply_layout(fig5, title=f"Free Cash Flow ({scenario})",
                     yaxis_tickprefix="R ", yaxis_tickformat=",.0f", height=240)
        st.plotly_chart(fig5, width='stretch')

# ══════════════════════════════════════════════════════════════════════════════
# TAB 2: INCOME STATEMENT
# ══════════════════════════════════════════════════════════════════════════════
elif selected_tab == "📋 Income Statement":
    st.markdown('<div class="section-header">Income Statement — Historical & Forecast</div>',
                unsafe_allow_html=True)

    # Build display table
    hist_years  = list(hist["year"].astype(int))
    fcast_years = list(sc_adj["year"].astype(int))
    all_years   = hist_years + fcast_years

    def is_get(df, col):
        if col in df.columns:
            return list(df[col])
        return [np.nan] * len(df)

    def build_is_table():
        h = hist
        f = sc_adj

        rows = []
        def row(label, h_col, f_col=None, is_total=False, fmt=fmt_r, section=False):
            if section:
                return {"label": label, "vals": [""] * len(all_years),
                        "is_total": False, "section": True}
            f_col = f_col or h_col
            h_vals = is_get(h, h_col)
            f_vals = is_get(f, f_col)
            return {"label": label, "vals": h_vals + f_vals,
                    "is_total": is_total, "section": False,
                    "fmt": fmt, "n_hist": len(h_vals)}

        return [
            row("Revenue",               "revenue",          is_total=False),
            row("Cost of Goods Sold",    "cogs"),
            row("Gross Profit",          "gross_profit",     is_total=True),
            row("Gross Margin",          "gross_margin_avg", "gross_margin",  fmt=fmt_pct),
            {"label": "", "vals": [""] * len(all_years), "is_total": False, "section": False, "fmt": fmt_r, "n_hist": len(hist)},
            row("Operating Expenses",    "opex"),
            row("R&D Expense",           "rd_expense"),
            row("SG&A Expense",          "sga_expense"),
            row("EBITDA",                "ebitda",           is_total=True),
            row("EBITDA Margin",         "ebitda_margin_avg","ebitda_margin", fmt=fmt_pct),
            {"label": "", "vals": [""] * len(all_years), "is_total": False, "section": False, "fmt": fmt_r, "n_hist": len(hist)},
            row("Depreciation & Amort.", "depreciation"),
            row("EBIT",                  "ebit",             is_total=True),
            row("Interest Expense",      "interest_expense"),
            row("EBT",                   "ebt"),
            row("Tax Expense",           "tax_expense"),
            row("Net Income",            "net_income",       is_total=True),
            row("Net Margin",            "net_margin_avg",   "net_margin",    fmt=fmt_pct),
        ]

    table_rows = build_is_table()

    # Render HTML table
    year_headers = "".join(
        f'<th style="color:{"#d29922" if y in fcast_years else "#8b949e"}">'
        f'{"📌 " if y in fcast_years else ""}{y}</th>'
        for y in all_years
    )
    thead = f"<tr><th>Line Item</th>{year_headers}</tr>"

    tbody = ""
    for r in table_rows:
        if r.get("section"):
            tbody += f'<tr class="section-row"><td colspan="{len(all_years)+1}">{r["label"]}</td></tr>'
            continue
        fmt_fn = r.get("fmt", fmt_r)
        cells  = "".join(f"<td>{fmt_fn(v) if not isinstance(v, str) else v}</td>"
                         for v in r["vals"])
        cls = "total-row" if r.get("is_total") else ""
        tbody += f'<tr class="{cls}"><td>{r["label"]}</td>{cells}</tr>'

    st.markdown(
        f'<div style="overflow-x:auto"><table class="fin-table">'
        f'<thead>{thead}</thead><tbody>{tbody}</tbody></table></div>',
        unsafe_allow_html=True
    )

# ══════════════════════════════════════════════════════════════════════════════
# TAB 3: BALANCE SHEET
# ══════════════════════════════════════════════════════════════════════════════
elif selected_tab == "🏦 Balance Sheet":
    st.markdown('<div class="section-header">Balance Sheet</div>', unsafe_allow_html=True)

    # Build simplified balance sheet from available data
    bs_data = combined.copy()
    bs_years_all = list(bs_data["year"].astype(int))

    # Reconstruct BS items
    rev_all = bs_data["revenue"].values
    ar_col  = "accounts_receivable" if "accounts_receivable" in bs_data.columns else None
    inv_col = "inventory"           if "inventory"           in bs_data.columns else None
    ap_col  = "accounts_payable"    if "accounts_payable"    in bs_data.columns else None

    ar  = bs_data[ar_col].values  if ar_col  else rev_all * 0.12
    inv = bs_data[inv_col].values if inv_col else rev_all * 0.08
    ap  = bs_data[ap_col].values  if ap_col  else rev_all * 0.06

    # Net PPE (cumulative capex - depreciation)
    capex_arr = bs_data["capex"].values if "capex" in bs_data.columns else rev_all * 0.05
    depr_arr  = bs_data["depreciation"].values if "depreciation" in bs_data.columns else rev_all * 0.04
    ppe = [rev_all[0] * 0.4]
    for i in range(1, len(bs_data)):
        ppe.append(max(ppe[-1] + abs(capex_arr[i]) - abs(depr_arr[i]), 0))
    ppe = np.array(ppe)

    # Equity
    ni_all = bs_data["net_income"].values
    retained = np.cumsum(ni_all * 0.7)
    equity   = retained + rev_all[0] * 0.25

    # Debt
    init_debt = bs_data["interest_expense"].mean() / 0.07 * 12
    debt = np.array([max(init_debt * (0.95 ** i), 0) for i in range(len(bs_data))])

    total_liab = ap + debt
    total_assets_excl_cash = ar + inv + ppe
    cash = np.maximum((equity + total_liab) - total_assets_excl_cash, 0)
    current_assets = cash + ar + inv
    total_assets = current_assets + ppe

    def bs_html_row(label, vals, is_total=False, section=False):
        if section:
            return (f'<tr class="section-row">'
                    f'<td colspan="{len(bs_years_all)+1}">{label}</td></tr>')
        cells = "".join(f"<td>{fmt_r(v)}</td>" for v in vals)
        cls   = "total-row" if is_total else ""
        return f'<tr class="{cls}"><td>{label}</td>{cells}</tr>'

    headers = "".join(f'<th>{y}</th>' for y in bs_years_all)
    bs_html = (
        f'<table class="fin-table"><thead>'
        f'<tr><th>Line Item</th>{headers}</tr></thead><tbody>'
    )
    bs_html += bs_html_row("ASSETS", [], section=True)
    bs_html += bs_html_row("Cash & Equivalents",   cash)
    bs_html += bs_html_row("Accounts Receivable",  ar)
    bs_html += bs_html_row("Inventory",             inv)
    bs_html += bs_html_row("Total Current Assets", current_assets, is_total=True)
    bs_html += bs_html_row("Net PP&E",              ppe)
    bs_html += bs_html_row("Total Assets",          total_assets,  is_total=True)
    bs_html += bs_html_row("LIABILITIES & EQUITY", [], section=True)
    bs_html += bs_html_row("Accounts Payable",      ap)
    bs_html += bs_html_row("Long-term Debt",        debt)
    bs_html += bs_html_row("Total Liabilities",     total_liab,    is_total=True)
    bs_html += bs_html_row("Retained Earnings",     retained)
    bs_html += bs_html_row("Total Equity",          equity,        is_total=True)
    bs_html += bs_html_row("Total L + E",           total_liab + equity, is_total=True)
    bs_html += "</tbody></table>"

    st.markdown(f'<div style="overflow-x:auto">{bs_html}</div>', unsafe_allow_html=True)

    # BS composition chart
    st.markdown("")
    fig_bs = go.Figure(data=[
        go.Bar(name="Cash",       x=[str(y) for y in bs_years_all], y=cash,     marker_color=COLORS["green"]),
        go.Bar(name="AR",         x=[str(y) for y in bs_years_all], y=ar,       marker_color=COLORS["blue"]),
        go.Bar(name="Inventory",  x=[str(y) for y in bs_years_all], y=inv,      marker_color=COLORS["purple"]),
        go.Bar(name="Net PP&E",   x=[str(y) for y in bs_years_all], y=ppe,      marker_color=COLORS["yellow"]),
    ])
    fig_bs.update_layout(**PLOT_LAYOUT, barmode="stack", title="Asset Composition",
                          yaxis_tickprefix="R ", yaxis_tickformat=",.0f", height=300)
    st.plotly_chart(fig_bs, width='stretch')

# ══════════════════════════════════════════════════════════════════════════════
# TAB 4: CASH FLOW
# ══════════════════════════════════════════════════════════════════════════════
elif selected_tab == "💵 Cash Flow":
    st.markdown('<div class="section-header">Cash Flow Statement (Indirect Method)</div>',
                unsafe_allow_html=True)

    bs_data = combined.copy()
    rev_all = bs_data["revenue"].values
    ar_col  = "accounts_receivable" if "accounts_receivable" in bs_data.columns else None
    inv_col = "inventory"           if "inventory" in bs_data.columns else None
    ap_col  = "accounts_payable"    if "accounts_payable" in bs_data.columns else None

    ar  = bs_data[ar_col].values  if ar_col  else rev_all * 0.12
    inv = bs_data[inv_col].values if inv_col else rev_all * 0.08
    ap  = bs_data[ap_col].values  if ap_col else rev_all * 0.06
    capex_arr = bs_data["capex"].values if "capex" in bs_data.columns else rev_all * 0.05
    depr_arr  = bs_data["depreciation"].values if "depreciation" in bs_data.columns else rev_all * 0.04
    init_debt = bs_data["interest_expense"].mean() / 0.07 * 12
    debt      = np.array([max(init_debt * (0.95 ** i), 0) for i in range(len(bs_data))])

    ni    = combined["net_income"].values
    depr  = combined["depreciation"].values if "depreciation" in combined.columns else ni * 0.25
    d_ar  = -np.diff(ar, prepend=ar[0])
    d_inv = -np.diff(inv, prepend=inv[0])
    d_ap  =  np.diff(ap,  prepend=ap[0])
    cfo   = ni + depr + d_ar + d_inv + d_ap

    capex_out = -np.abs(capex_arr)
    cfi       = capex_out

    dividends = -ni * 0.30
    debt_chg  = np.diff(debt, prepend=debt[0])
    cff       = dividends + debt_chg

    net_chg   = cfo + cfi + cff
    cum_cash  = np.cumsum(net_chg)

    cf_years = list(combined["year"].astype(int))
    headers  = "".join(f'<th>{y}</th>' for y in cf_years)

    def cf_row(label, vals, is_total=False, section=False):
        if section:
            return (f'<tr class="section-row">'
                    f'<td colspan="{len(cf_years)+1}">{label}</td></tr>')
        cells = "".join(
            f'<td><span class="{"positive" if v >= 0 else "negative"}">{fmt_r(v)}</span></td>'
            for v in vals
        )
        cls = "total-row" if is_total else ""
        return f'<tr class="{cls}"><td>{label}</td>{cells}</tr>'

    cf_html = (f'<table class="fin-table"><thead>'
               f'<tr><th>Line Item</th>{headers}</tr></thead><tbody>')
    cf_html += cf_row("OPERATING ACTIVITIES", [], section=True)
    cf_html += cf_row("Net Income",                      ni)
    cf_html += cf_row("Add: Depreciation & Amort.",      depr)
    cf_html += cf_row("Change in Accounts Receivable",   d_ar)
    cf_html += cf_row("Change in Inventory",             d_inv)
    cf_html += cf_row("Change in Accounts Payable",      d_ap)
    cf_html += cf_row("Cash from Operations (CFO)",      cfo,  is_total=True)
    cf_html += cf_row("INVESTING ACTIVITIES", [], section=True)
    cf_html += cf_row("Capital Expenditures",            capex_out)
    cf_html += cf_row("Cash from Investing (CFI)",       cfi,  is_total=True)
    cf_html += cf_row("FINANCING ACTIVITIES", [], section=True)
    cf_html += cf_row("Dividends Paid",                  dividends)
    cf_html += cf_row("Net Debt Change",                 debt_chg)
    cf_html += cf_row("Cash from Financing (CFF)",       cff,  is_total=True)
    cf_html += cf_row("Net Change in Cash",              net_chg, is_total=True)
    cf_html += cf_row("Cumulative Cash Position",        cum_cash, is_total=True)
    cf_html += "</tbody></table>"

    st.markdown(f'<div style="overflow-x:auto">{cf_html}</div>', unsafe_allow_html=True)

    # Waterfall chart for latest forecast year
    latest_cf = {
        "CFO": float(cfo[-1]),
        "CFI": float(cfi[-1]),
        "CFF": float(cff[-1]),
    }
    fig_cf = go.Figure(go.Waterfall(
        name="Cash Flow", orientation="v",
        measure=["relative", "relative", "relative", "total"],
        x=["CFO", "CFI", "CFF", "Net Change"],
        y=[latest_cf["CFO"], latest_cf["CFI"], latest_cf["CFF"],
           sum(latest_cf.values())],
        connector=dict(line=dict(color=COLORS["border"])),
        decreasing=dict(marker_color=COLORS["red"]),
        increasing=dict(marker_color=COLORS["green"]),
        totals=dict(marker_color=COLORS["blue"]),
    ))
    apply_layout(fig_cf,
                 title=f"Cash Flow Waterfall — {cf_years[-1]}",
                 yaxis_tickprefix="R ", yaxis_tickformat=",.0f", height=320)
    st.plotly_chart(fig_cf, width='stretch')

# ══════════════════════════════════════════════════════════════════════════════
# TAB 5: DCF VALUATION
# ══════════════════════════════════════════════════════════════════════════════
elif selected_tab == "🎯 DCF Valuation":
    st.markdown('<div class="section-header">DCF Valuation Model</div>',
                unsafe_allow_html=True)

    fcf_series = sc_adj.get("fcf", sc_adj["net_income"] * 0.85).reset_index(drop=True)
    dcf = run_dcf(fcf_series, wacc, terminal_g, net_debt)

    per_share = dcf["equity_value"] / (shares_m * 1_000_000)

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Enterprise Value",  fmt_r(dcf["enterprise_value"]))
    col2.metric("Equity Value",      fmt_r(dcf["equity_value"]))
    col3.metric("Value Per Share",   f"R {per_share:,.2f}")
    col4.metric("TV as % of EV",     fmt_pct(dcf["pv_tv_pct"]))

    st.markdown("---")

    c_left, c_right = st.columns([2, 3])

    with c_left:
        st.markdown("**Valuation Bridge**")
        fig_bridge = go.Figure(go.Waterfall(
            orientation="v",
            measure=["relative", "relative", "total"],
            x=["PV of FCFs", "PV of Terminal Value", "Enterprise Value"],
            y=[dcf["pv_fcf"], dcf["pv_terminal"], 0],
            connector=dict(line=dict(color=COLORS["border"])),
            increasing=dict(marker_color=COLORS["blue"]),
            totals=dict(marker_color=COLORS["green"]),
        ))
        apply_layout(fig_bridge, yaxis_tickprefix="R ", yaxis_tickformat=",.0f", height=300)
        st.plotly_chart(fig_bridge, width='stretch')

    with c_right:
        st.markdown("**WACC × Terminal Growth Sensitivity (Equity Value)**")
        wacc_range = np.arange(0.08, 0.22, 0.02)
        tg_range   = np.arange(0.01, 0.06, 0.01)

        heat_data = []
        for w in wacc_range:
            row_vals = []
            for g in tg_range:
                if w <= g:
                    row_vals.append(np.nan)
                else:
                    d = run_dcf(fcf_series, w, g, net_debt)
                    row_vals.append(d["equity_value"] / 1_000_000)
            heat_data.append(row_vals)

        fig_heat = go.Figure(go.Heatmap(
            z=heat_data,
            x=[f"{g*100:.0f}%" for g in tg_range],
            y=[f"{w*100:.0f}%" for w in wacc_range],
            colorscale=[[0, COLORS["red"]], [0.5, COLORS["yellow"]], [1, COLORS["green"]]],
            text=[[f"R{v:.0f}M" if not np.isnan(v) else "N/A" for v in row] for row in heat_data],
            texttemplate="%{text}",
            textfont=dict(size=11),
            colorbar=dict(title="EV (R M)"),
        ))
        apply_layout(fig_heat, height=300,
                     xaxis_title="Terminal Growth Rate",
                     yaxis_title="WACC",
                     title="Equity Value (R millions)")
        st.plotly_chart(fig_heat, width='stretch')

    # FCF discount schedule
    st.markdown("**Discounted FCF Schedule**")
    schedule = pd.DataFrame({
        "Year"            : list(sc_adj["year"].astype(int)),
        "FCF (R)"         : [fmt_r(v) for v in fcf_series],
        "Discount Factor" : [f"{1/(1+wacc)**t:.4f}" for t in range(1, len(fcf_series)+1)],
        "PV of FCF (R)"   : [fmt_r(f / (1+wacc)**t)
                              for t, f in enumerate(fcf_series, 1)],
    })
    st.dataframe(schedule, width='stretch', hide_index=True)

# ══════════════════════════════════════════════════════════════════════════════
# TAB 6: SCENARIO ENGINE
# ══════════════════════════════════════════════════════════════════════════════
elif selected_tab == "⚡ Scenario Engine":
    st.markdown('<div class="section-header">Scenario Comparison Engine</div>',
                unsafe_allow_html=True)

    sc_years = sorted(scenarios["year"].unique())

    # Side-by-side metric comparison
    cols = st.columns(3)
    for i, sc_label in enumerate(["Base", "Bull", "Bear"]):
        sc_sub = scenarios[scenarios["scenario"] == sc_label].sort_values("year")
        rev_total = sc_sub["revenue"].sum()
        ni_total  = sc_sub["net_income"].sum()
        fcf_total = sc_sub.get("fcf", sc_sub["net_income"] * 0.85).sum()
        em_avg    = (sc_sub.get("ebitda_margin",
                    sc_sub["ebitda"] / sc_sub["revenue"]).mean())

        color_map = {"Base": "#58a6ff", "Bull": "#3fb950", "Bear": "#f85149"}
        with cols[i]:
            st.markdown(f"""
            <div style="border:1px solid {color_map[sc_label]};
                 border-radius:8px; padding:16px; margin-bottom:8px;">
              <div style="font-size:14px; font-weight:700;
                   color:{color_map[sc_label]}; margin-bottom:12px;">
                {sc_label} Case
              </div>
              <div class="kpi-label">3Y Revenue</div>
              <div class="kpi-value">{fmt_r(rev_total)}</div>
              <br>
              <div class="kpi-label">3Y Net Income</div>
              <div class="kpi-value">{fmt_r(ni_total)}</div>
              <br>
              <div class="kpi-label">Avg EBITDA Margin</div>
              <div class="kpi-value">{fmt_pct(em_avg)}</div>
              <br>
              <div class="kpi-label">3Y FCF</div>
              <div class="kpi-value">{fmt_r(fcf_total)}</div>
            </div>""", unsafe_allow_html=True)

    st.markdown("---")

    # Scenario revenue comparison
    fig_sc = go.Figure()
    sc_colors = {"Base": COLORS["blue"], "Bull": COLORS["green"], "Bear": COLORS["red"]}
    for sc_label in ["Base", "Bull", "Bear"]:
        sc_sub = scenarios[scenarios["scenario"] == sc_label].sort_values("year")
        fig_sc.add_trace(go.Scatter(
            x=sc_sub["year"], y=sc_sub["revenue"],
            name=sc_label,
            mode="lines+markers",
            line=dict(color=sc_colors[sc_label], width=3),
            marker=dict(size=8),
            fill="tonexty" if sc_label == "Bear" else None,
            fillcolor="rgba(248,81,73,0.08)" if sc_label == "Bear" else None,
        ))

    # Historical reference
    fig_sc.add_trace(go.Scatter(
        x=hist["year"], y=hist["revenue"],
        name="Historical",
        mode="lines+markers",
        line=dict(color=COLORS["muted"], width=2, dash="dot"),
        marker=dict(size=6),
    ))

    apply_layout(fig_sc, title="Revenue — Scenario Comparison",
                 yaxis_tickprefix="R ", yaxis_tickformat=",.0f", height=380)
    st.plotly_chart(fig_sc, width='stretch')

    # Scenario metrics table
    metric_cols = ["year", "revenue", "ebitda", "net_income", "fcf"]
    available   = [c for c in metric_cols if c in scenarios.columns]
    sc_pivot    = scenarios[["scenario"] + available].copy()

    for c in available[1:]:
        sc_pivot[c] = sc_pivot[c].apply(fmt_r)

    st.dataframe(
        sc_pivot.rename(columns={
            "scenario": "Scenario", "year": "Year",
            "revenue": "Revenue", "ebitda": "EBITDA",
            "net_income": "Net Income", "fcf": "FCF",
        }),
        width='stretch',
        hide_index=True,
    )

# ── Footer ─────────────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown("""
<div style="text-align:center; color:#30363d; font-size:11px; padding: 8px 0;">
  FinModel Pro · Automated Financial Modelling Platform ·
  Data pipeline: Databricks (Bronze→Silver→Gold) · UI: Streamlit
</div>
""", unsafe_allow_html=True)
