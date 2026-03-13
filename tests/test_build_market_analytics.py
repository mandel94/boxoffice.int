"""Tests for build_market_analytics — KPI calculations, guard division by zero, contract compliance."""
import pandas as pd
import pytest
from pathlib import Path

import boxoffice_int.common as common_mod
import boxoffice_int.domain.market_analytics.build_product as build_mod
from boxoffice_int.contracts import load_contract, validate


# ---------------------------------------------------------------------------
# Fixture data
# ---------------------------------------------------------------------------

_COLUMNS = ["date", "rank", "title", "gross_eur", "admissions", "cinemas",
            "avg_per_cinema_eur", "total_gross_eur"]

_ROWS = [
    ("2026-01-01", 1, "Film Alpha", 500_000, 20_000, 400, 1_250, 500_000),
    ("2026-01-01", 2, "Film Beta",  300_000, 12_000, 300, 1_000, 300_000),
    ("2026-01-01", 3, "Film Gamma", 200_000,  8_000, 200, 1_000, 200_000),
    ("2026-01-02", 1, "Film Alpha", 450_000, 18_000, 390, 1_153, 950_000),
    ("2026-01-02", 2, "Film Delta", 280_000, 11_000, 270, 1_037, 280_000),
]


@pytest.fixture()
def raw_csv(tmp_path: Path) -> Path:
    df = pd.DataFrame(_ROWS, columns=_COLUMNS)
    path = tmp_path / "raw.csv"
    df.to_csv(path, index=False)
    return path


@pytest.fixture()
def run(tmp_path: Path, monkeypatch):
    """Run build_market_analytics with DATA_PRODUCTS redirected to tmp_path."""
    monkeypatch.setattr(common_mod, "DATA_PRODUCTS", tmp_path)
    monkeypatch.setattr(build_mod, "DATA_PRODUCTS", tmp_path)

    def _run(csv_path: Path, metadata_path: Path | None = None):
        fact_path, kpi_path = build_mod.build_market_analytics(csv_path, metadata_path)
        return pd.read_parquet(fact_path), pd.read_parquet(kpi_path)

    return _run


# ---------------------------------------------------------------------------
# KPI correctness
# ---------------------------------------------------------------------------

class TestKPICalculations:
    def test_one_kpi_row_per_date(self, raw_csv, run):
        _, kpis = run(raw_csv)
        assert len(kpis) == 2

    def test_gross_total_day1(self, raw_csv, run):
        _, kpis = run(raw_csv)
        day1 = kpis[kpis["date"] == pd.Timestamp("2026-01-01")].iloc[0]
        assert int(day1["gross_total_eur"]) == 1_000_000

    def test_admissions_total_day1(self, raw_csv, run):
        _, kpis = run(raw_csv)
        day1 = kpis[kpis["date"] == pd.Timestamp("2026-01-01")].iloc[0]
        assert int(day1["admissions_total"]) == 40_000

    def test_unique_titles_day1(self, raw_csv, run):
        _, kpis = run(raw_csv)
        day1 = kpis[kpis["date"] == pd.Timestamp("2026-01-01")].iloc[0]
        assert int(day1["unique_titles"]) == 3

    def test_avg_ticket_price_day1(self, raw_csv, run):
        _, kpis = run(raw_csv)
        day1 = kpis[kpis["date"] == pd.Timestamp("2026-01-01")].iloc[0]
        assert float(day1["avg_ticket_price_eur"]) == pytest.approx(round(1_000_000 / 40_000, 2), abs=0.01)

    def test_avg_gross_per_cinema_day1(self, raw_csv, run):
        _, kpis = run(raw_csv)
        day1 = kpis[kpis["date"] == pd.Timestamp("2026-01-01")].iloc[0]
        assert float(day1["avg_gross_per_cinema_eur"]) == pytest.approx(round(1_000_000 / 900, 2), abs=0.01)


# ---------------------------------------------------------------------------
# Guard: division by zero
# ---------------------------------------------------------------------------

class TestDivisionByZeroGuard:
    def test_zero_admissions_yields_na(self, tmp_path, run):
        rows = [("2026-01-01", 1, "Film X", 500_000, 0, 200, 2_500, 500_000)]
        path = tmp_path / "zero_adm.csv"
        pd.DataFrame(rows, columns=_COLUMNS).to_csv(path, index=False)
        _, kpis = run(path)
        assert pd.isna(kpis.iloc[0]["avg_ticket_price_eur"])

    def test_zero_cinemas_yields_na(self, tmp_path, run):
        rows = [("2026-01-01", 1, "Film X", 500_000, 10_000, 0, 0, 500_000)]
        path = tmp_path / "zero_cin.csv"
        pd.DataFrame(rows, columns=_COLUMNS).to_csv(path, index=False)
        _, kpis = run(path)
        assert pd.isna(kpis.iloc[0]["avg_gross_per_cinema_eur"])


# ---------------------------------------------------------------------------
# Fact table
# ---------------------------------------------------------------------------

class TestFactTable:
    def test_title_norm_column_present(self, raw_csv, run):
        fact, _ = run(raw_csv)
        assert "title_norm" in fact.columns

    def test_sorted_by_date_then_rank(self, raw_csv, run):
        fact, _ = run(raw_csv)
        dates = fact["date"].tolist()
        assert dates == sorted(dates)
        day1 = fact[fact["date"] == fact["date"].iloc[0]]
        assert day1["rank"].tolist() == sorted(day1["rank"].tolist())


# ---------------------------------------------------------------------------
# Contract compliance
# ---------------------------------------------------------------------------

class TestContractCompliance:
    def test_kpi_passes_market_analytics_contract(self, raw_csv, run):
        _, kpis = run(raw_csv)
        contract = load_contract("market-analytics-kpi-daily")
        validate(kpis, contract)  # must not raise
