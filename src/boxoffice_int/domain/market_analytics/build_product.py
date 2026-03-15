import logging
from pathlib import Path

import pandas as pd

from ...common import DATA_CURATED, DATA_PRODUCTS, normalize_title
from ...contracts import load_contract, validate, cast_to_contract

LOG = logging.getLogger(__name__)


def build_market_analytics(input_path: Path, metadata_path: Path | None = None) -> tuple[Path, Path]:
    """
    Build the market analytics data product from raw box-office data.

    Joins raw box-office with optional film metadata, computes daily KPIs
    (total gross, admissions, avg ticket price, avg gross per cinema), and
    validates both outputs against their contracts before writing.

    Args:
        input_path: Path to the raw box-office CSV (from ``ingest``).
        metadata_path: Optional path to the film metadata CSV. Defaults to
            ``DATA_CURATED/film_metadata/film_metadata.csv`` if it exists.

    Returns:
        Tuple of (fact_path, kpi_path) for the written Parquet files.
    """
    box_office = pd.read_csv(input_path)
    box_office["title_norm"] = box_office["title"].astype(str).map(normalize_title)
    box_office["date"] = pd.to_datetime(box_office["date"], errors="coerce")

    raw_contract = load_contract("box-office-raw-daily")
    box_office = cast_to_contract(box_office, raw_contract)
    validate(box_office, raw_contract)

    if metadata_path is None:
        metadata_path = DATA_CURATED / "film_metadata" / "film_metadata.csv"

    if metadata_path.exists():
        metadata = pd.read_csv(metadata_path)
        dataset = box_office.merge(metadata, on="title_norm", how="left")
    else:
        dataset = box_office.copy()

    dataset = dataset.sort_values(["date", "rank"]).reset_index(drop=True)

    kpis = (
        dataset.groupby("date", as_index=False)
        .agg(
            gross_total_eur=("gross_eur", "sum"),
            admissions_total=("admissions", "sum"),
            cinemas_total=("cinemas", "sum"),
            unique_titles=("title_norm", "nunique"),
        )
        .sort_values("date")
    )

    kpis["avg_ticket_price_eur"] = (
        kpis["gross_total_eur"] / kpis["admissions_total"].replace(0, pd.NA)
    ).round(2)
    kpis["avg_gross_per_cinema_eur"] = (
        kpis["gross_total_eur"] / kpis["cinemas_total"].replace(0, pd.NA)
    ).round(2)

    output_dir = DATA_PRODUCTS / "market_analytics"
    output_dir.mkdir(parents=True, exist_ok=True)

    fact_path = output_dir / "fact_daily_boxoffice.parquet"
    kpi_path = output_dir / "kpi_daily_market.parquet"

    dataset.to_parquet(fact_path, index=False)

    kpi_contract = load_contract("market-analytics-kpi-daily")
    kpis = cast_to_contract(kpis, kpi_contract)
    validate(kpis, kpi_contract)

    kpis.to_parquet(kpi_path, index=False)

    return fact_path, kpi_path
