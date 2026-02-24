import argparse
from datetime import date
from pathlib import Path

from .domain.box_office_raw.cineguru_scraper import scrape_cineguru
from .domain.film_metadata.tmdb_client import enrich_titles_with_tmdb
from .domain.market_analytics.build_product import build_market_analytics


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="boxoffice.int data product pipeline")
    sub = parser.add_subparsers(dest="command", required=True)

    ingest = sub.add_parser("ingest", help="Ingestione raw da Cineguru")
    ingest.add_argument("--start", type=date.fromisoformat, required=True, help="Data inizio YYYY-MM-DD")
    ingest.add_argument("--end", type=date.fromisoformat, required=True, help="Data fine YYYY-MM-DD")
    ingest.add_argument("--delay", type=float, default=2.0, help="Delay richieste")

    enrich = sub.add_parser("enrich", help="Arricchimento metadati TMDB")
    enrich.add_argument("--input", type=Path, required=True, help="CSV raw box office")

    build = sub.add_parser("build", help="Build data product analytics")
    build.add_argument("--input", type=Path, required=True, help="CSV raw box office")
    build.add_argument("--metadata", type=Path, default=None, help="CSV metadata opzionale")

    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    if args.command == "ingest":
        path = scrape_cineguru(start=args.start, end=args.end, delay=args.delay)
        print(f"Raw dataset creato: {path}")
        return

    if args.command == "enrich":
        path = enrich_titles_with_tmdb(input_path=args.input)
        print(f"Metadata dataset creato: {path}")
        return

    if args.command == "build":
        fact_path, kpi_path = build_market_analytics(input_path=args.input, metadata_path=args.metadata)
        print(f"Fact dataset creato: {fact_path}")
        print(f"KPI dataset creato:  {kpi_path}")
        return

    parser.print_help()


if __name__ == "__main__":
    main()
