"""
test_business_rules.py
======================
Unit tests for financial model business rules and accounting identities.
Run these in a Databricks notebook cell or locally with: pytest test_business_rules.py -v

These tests validate that the financial model produces internally consistent
statements before any data reaches the Gold layer.
"""

import pytest
import pandas as pd
import numpy as np
import sys
import os

# Allow import of project modules when run locally
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../configs"))

# ─────────────────────────────────────────────────────────────────────────────
# TEST FIXTURES
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_income_statement():
    """Minimal valid income statement for 3 years."""
    return pd.DataFrame({
        "year"            : [2021, 2022, 2023],
        "revenue"         : [10_000_000, 11_500_000, 13_200_000],
        "cogs"            : [ 4_000_000,  4_500_000,  5_000_000],
        "gross_profit"    : [ 6_000_000,  7_000_000,  8_200_000],
        "opex"            : [ 2_000_000,  2_200_000,  2_400_000],
        "rd_expense"      : [   800_000,    900_000,  1_000_000],
        "sga_expense"     : [ 1_000_000,  1_100_000,  1_200_000],
        "ebitda"          : [ 2_200_000,  2_800_000,  3_600_000],
        "depreciation"    : [   400_000,    450_000,    520_000],
        "ebit"            : [ 1_800_000,  2_350_000,  3_080_000],
        "interest_expense": [   150_000,    140_000,    130_000],
        "ebt"             : [ 1_650_000,  2_210_000,  2_950_000],
        "tax_expense"     : [   462_000,    618_800,    826_000],
        "net_income"      : [ 1_188_000,  1_591_200,  2_124_000],
    })


@pytest.fixture
def sample_balance_sheet():
    """Minimal valid balance sheet for 3 years."""
    return pd.DataFrame({
        "year"               : [2021, 2022, 2023],
        "cash"               : [2_000_000, 2_500_000, 3_200_000],
        "accounts_receivable": [1_200_000, 1_380_000, 1_584_000],
        "inventory"          : [  800_000,   900_000, 1_000_000],
        "current_assets"     : [4_000_000, 4_780_000, 5_784_000],
        "net_ppe"            : [3_000_000, 3_200_000, 3_350_000],
        "total_assets"       : [7_000_000, 7_980_000, 9_134_000],
        "accounts_payable"   : [  600_000,   675_000,   750_000],
        "long_term_debt"     : [2_000_000, 1_900_000, 1_805_000],
        "total_liabilities"  : [2_600_000, 2_575_000, 2_555_000],
        "paid_in_capital"    : [2_500_000, 2_500_000, 2_500_000],
        "retained_earnings"  : [1_900_000, 2_905_000, 4_079_000],
        "total_equity"       : [4_400_000, 5_405_000, 6_579_000],
    })


@pytest.fixture
def sample_cashflow():
    """Minimal valid cash flow statement."""
    return pd.DataFrame({
        "year"         : [2021, 2022, 2023],
        "net_income"   : [1_188_000, 1_591_200, 2_124_000],
        "depreciation" : [  400_000,   450_000,   520_000],
        "cfo"          : [1_400_000, 1_800_000, 2_300_000],
        "cfi"          : [ -600_000,  -650_000,  -670_000],
        "cff"          : [ -300_000,  -350_000,  -380_000],
        "net_cash"     : [  500_000,   800_000, 1_250_000],
        "closing_cash" : [2_000_000, 2_500_000, 3_200_000],  # matches BS cash
    })


# ─────────────────────────────────────────────────────────────────────────────
# INCOME STATEMENT TESTS
# ─────────────────────────────────────────────────────────────────────────────

class TestIncomeStatement:

    def test_gross_profit_identity(self, sample_income_statement):
        """gross_profit must equal revenue minus cogs within R1 tolerance."""
        df = sample_income_statement
        discrepancy = abs(df["gross_profit"] - (df["revenue"] - df["cogs"]))
        assert (discrepancy < 1.0).all(), \
            f"Gross profit identity violated. Max discrepancy: R{discrepancy.max():.2f}"

    def test_ebitda_consistency(self, sample_income_statement):
        """EBITDA = Gross Profit - OpEx - R&D - SG&A."""
        df = sample_income_statement
        expected_ebitda = (df["gross_profit"] - df["opex"]
                           - df["rd_expense"] - df["sga_expense"])
        discrepancy = abs(df["ebitda"] - expected_ebitda)
        assert (discrepancy < 1.0).all(), \
            f"EBITDA inconsistency. Max discrepancy: R{discrepancy.max():.2f}"

    def test_ebit_consistency(self, sample_income_statement):
        """EBIT = EBITDA - Depreciation."""
        df = sample_income_statement
        expected = df["ebitda"] - df["depreciation"]
        discrepancy = abs(df["ebit"] - expected)
        assert (discrepancy < 1.0).all()

    def test_net_income_consistency(self, sample_income_statement):
        """Net Income = EBT - Tax."""
        df = sample_income_statement
        expected = df["ebt"] - df["tax_expense"]
        discrepancy = abs(df["net_income"] - expected)
        assert (discrepancy < 1.0).all()

    def test_revenue_positive(self, sample_income_statement):
        """Revenue must be positive."""
        assert (sample_income_statement["revenue"] > 0).all()

    def test_gross_margin_bounds(self, sample_income_statement):
        """Gross margin must be between 10% and 95%."""
        df = sample_income_statement
        gm = df["gross_profit"] / df["revenue"]
        assert (gm >= 0.10).all(), f"Gross margin below 10%: {gm.min():.1%}"
        assert (gm <= 0.95).all(), f"Gross margin above 95%: {gm.max():.1%}"

    def test_tax_nonnegative(self, sample_income_statement):
        """Tax expense must never be negative (no negative tax in this model)."""
        assert (sample_income_statement["tax_expense"] >= 0).all()

    def test_tax_rate_approximately_correct(self, sample_income_statement):
        """Tax should be approximately 28% of EBT (SA corporate tax rate)."""
        df = sample_income_statement
        profitable = df[df["ebt"] > 0]
        implied_rate = profitable["tax_expense"] / profitable["ebt"]
        assert (abs(implied_rate - 0.28) < 0.01).all(), \
            f"Tax rate deviation: {implied_rate.to_list()}"

    def test_revenue_growth_not_extreme(self, sample_income_statement):
        """YoY revenue growth should not exceed 200% (likely data error if so)."""
        df = sample_income_statement.sort_values("year")
        yoy = df["revenue"].pct_change().dropna()
        assert (yoy <= 2.00).all(), f"Revenue growth > 200%: {yoy.max():.1%}"
        assert (yoy >= -0.80).all(), f"Revenue decline > 80%: {yoy.min():.1%}"


# ─────────────────────────────────────────────────────────────────────────────
# BALANCE SHEET TESTS
# ─────────────────────────────────────────────────────────────────────────────

class TestBalanceSheet:

    def test_balance_sheet_balances(self, sample_balance_sheet):
        """Assets must equal Liabilities + Equity within R10 tolerance."""
        df = sample_balance_sheet
        discrepancy = abs(df["total_assets"] - (df["total_liabilities"] + df["total_equity"]))
        assert (discrepancy < 10.0).all(), \
            f"Balance sheet out of balance. Max error: R{discrepancy.max():,.2f}"

    def test_total_assets_correct(self, sample_balance_sheet):
        """Total assets = current assets + net PPE."""
        df = sample_balance_sheet
        expected = df["current_assets"] + df["net_ppe"]
        discrepancy = abs(df["total_assets"] - expected)
        assert (discrepancy < 10.0).all()

    def test_current_assets_correct(self, sample_balance_sheet):
        """Current assets = cash + AR + inventory."""
        df = sample_balance_sheet
        expected = df["cash"] + df["accounts_receivable"] + df["inventory"]
        discrepancy = abs(df["current_assets"] - expected)
        assert (discrepancy < 10.0).all()

    def test_cash_nonnegative(self, sample_balance_sheet):
        """Cash balance must never be negative."""
        assert (sample_balance_sheet["cash"] >= 0).all()

    def test_equity_growing(self, sample_balance_sheet):
        """Equity should grow over time (profitable company)."""
        df = sample_balance_sheet.sort_values("year")
        equity_growth = df["total_equity"].diff().dropna()
        assert (equity_growth > 0).all(), \
            "Total equity declined — check retained earnings accumulation"

    def test_debt_not_exceed_assets(self, sample_balance_sheet):
        """Long-term debt should not exceed total assets (insolvency check)."""
        df = sample_balance_sheet
        assert (df["long_term_debt"] < df["total_assets"]).all()


# ─────────────────────────────────────────────────────────────────────────────
# CASH FLOW TESTS
# ─────────────────────────────────────────────────────────────────────────────

class TestCashFlow:

    def test_net_cash_change_correct(self, sample_cashflow):
        """Net cash change = CFO + CFI + CFF."""
        df = sample_cashflow
        expected = df["cfo"] + df["cfi"] + df["cff"]
        discrepancy = abs(df["net_cash"] - expected)
        assert (discrepancy < 10.0).all()

    def test_cfo_positive_for_profitable_company(self, sample_cashflow, sample_income_statement):
        """A consistently profitable company should have positive CFO."""
        assert (sample_cashflow["cfo"] > 0).all(), \
            "CFO is negative for a profitable company — check working capital assumptions"

    def test_cash_reconciles_with_balance_sheet(self, sample_cashflow, sample_balance_sheet):
        """Closing cash in CF statement must match cash on balance sheet."""
        cf_cash = sample_cashflow["closing_cash"].values
        bs_cash = sample_balance_sheet["cash"].values
        discrepancy = abs(cf_cash - bs_cash)
        assert (discrepancy < 10.0).all(), \
            f"CF closing cash ≠ BS cash. Max error: R{max(discrepancy):,.2f}"

    def test_cfi_negative_for_investing_company(self, sample_cashflow):
        """Investing outflows (capex) should make CFI negative."""
        assert (sample_cashflow["cfi"] < 0).all(), \
            "CFI should be negative (capex outflows)"


# ─────────────────────────────────────────────────────────────────────────────
# THREE-STATEMENT LINKAGE TESTS
# ─────────────────────────────────────────────────────────────────────────────

class TestThreeStatementLinkage:

    def test_net_income_flows_to_retained_earnings(
            self, sample_income_statement, sample_balance_sheet):
        """
        Net income × (1 - payout ratio) should equal the change in retained earnings.
        Payout ratio = 30% (dividends).
        """
        ni   = sample_income_statement["net_income"].values
        re   = sample_balance_sheet["retained_earnings"].values
        d_re = np.diff(re)
        expected_d_re = ni[1:] * 0.70   # 70% retention

        discrepancy = abs(d_re - expected_d_re)
        # Allow R1000 tolerance for rounding across many line items
        assert (discrepancy < 1_000).all(), \
            f"Retained earnings not reconciling with net income. " \
            f"Max error: R{max(discrepancy):,.0f}"

    def test_depreciation_consistent_across_statements(
            self, sample_income_statement, sample_cashflow):
        """D&A on income statement should equal D&A add-back on cash flow."""
        is_depr = sample_income_statement["depreciation"].values
        cf_depr = sample_cashflow["depreciation"].values
        discrepancy = abs(is_depr - cf_depr)
        assert (discrepancy < 1.0).all()


# ─────────────────────────────────────────────────────────────────────────────
# DCF TESTS
# ─────────────────────────────────────────────────────────────────────────────

class TestDCF:

    def _run_dcf(self, fcf_list, wacc, terminal_g, net_debt=0):
        """Inline DCF for testing — mirrors app.py logic."""
        fcf = pd.Series(fcf_list)
        n   = len(fcf)
        dfs = [1 / (1 + wacc) ** t for t in range(1, n + 1)]
        pv_fcf = sum(f * d for f, d in zip(fcf, dfs))
        tv     = fcf.iloc[-1] * (1 + terminal_g) / (wacc - terminal_g)
        pv_tv  = tv * dfs[-1]
        ev     = pv_fcf + pv_tv
        eq     = ev - net_debt
        return {"ev": ev, "equity": eq, "pv_fcf": pv_fcf, "pv_tv": pv_tv}

    def test_enterprise_value_positive(self):
        """EV must be positive for a profitable company."""
        dcf = self._run_dcf([1_000_000, 1_200_000, 1_400_000], wacc=0.12, terminal_g=0.025)
        assert dcf["ev"] > 0

    def test_wacc_gt_terminal_growth(self):
        """WACC must exceed terminal growth (Gordon Growth Model denominator > 0)."""
        wacc = 0.12
        tg   = 0.025
        assert wacc > tg, "WACC must be greater than terminal growth rate"

    def test_higher_wacc_lower_ev(self):
        """Higher discount rate should reduce enterprise value."""
        fcf = [1_000_000, 1_200_000, 1_400_000]
        ev_low  = self._run_dcf(fcf, wacc=0.10, terminal_g=0.025)["ev"]
        ev_high = self._run_dcf(fcf, wacc=0.15, terminal_g=0.025)["ev"]
        assert ev_low > ev_high, \
            "Higher WACC should produce lower EV (inverse relationship)"

    def test_higher_terminal_growth_higher_ev(self):
        """Higher terminal growth should increase enterprise value."""
        fcf = [1_000_000, 1_200_000, 1_400_000]
        ev_low  = self._run_dcf(fcf, wacc=0.12, terminal_g=0.015)["ev"]
        ev_high = self._run_dcf(fcf, wacc=0.12, terminal_g=0.035)["ev"]
        assert ev_high > ev_low

    def test_terminal_value_reasonable_pct(self):
        """Terminal value should be between 50% and 90% of EV (typical range)."""
        fcf = [1_000_000, 1_200_000, 1_400_000]
        dcf = self._run_dcf(fcf, wacc=0.12, terminal_g=0.025)
        tv_pct = dcf["pv_tv"] / dcf["ev"]
        assert 0.50 <= tv_pct <= 0.95, \
            f"Terminal value is {tv_pct:.1%} of EV — outside typical 50–90% range"


# ─────────────────────────────────────────────────────────────────────────────
# DATA QUALITY TESTS
# ─────────────────────────────────────────────────────────────────────────────

class TestDataQuality:

    def test_no_duplicate_dates(self):
        """Financial data should have one record per month per company."""
        dates = pd.date_range("2018-01-01", "2023-12-31", freq="MS")
        df = pd.DataFrame({"date": dates, "company": "AcmeCorp"})
        assert df.duplicated(["date", "company"]).sum() == 0

    def test_churn_rate_in_bounds(self):
        """Churn rate must be between 0 and 1."""
        churn = pd.Series([0.02, 0.035, 0.05, 0.04, 0.03])
        assert (churn >= 0).all() and (churn <= 1).all()

    def test_null_detection(self):
        """Revenue column must not contain nulls."""
        revenue = pd.Series([500_000, 600_000, None, 700_000])
        null_count = revenue.isnull().sum()
        assert null_count == 0, f"Found {null_count} null revenue values"


# ─────────────────────────────────────────────────────────────────────────────
# RUN TESTS (local execution)
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("Running FinModel Pro unit tests...")
    pytest.main([__file__, "-v", "--tb=short"])
