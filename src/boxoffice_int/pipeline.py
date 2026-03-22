import argparse
import logging
from datetime import date, timedelta
from pathlib import Path

from .domain.box_office_raw.cineguru_scraper import scrape_cineguru
from .domain.film_metadata.tmdb_client import enrich_titles_with_tmdb
from .domain.market_analytics.build_product import build_market_analytics

# ---------------------------------------------------------------------------
# Date-range presets
# ---------------------------------------------------------------------------


def _presets(today: date) -> dict[str, tuple[date, date]]:
    """Return named date-range presets relative to *today*."""
    yesterday = today - timedelta(days=1)
    # ISO week: Monday=0 … Sunday=6
    week_start = today - timedelta(days=today.weekday())
    last_week_start = week_start - timedelta(weeks=1)
    last_week_end = week_start - timedelta(days=1)
    month_start = today.replace(day=1)
    last_month_end = month_start - timedelta(days=1)
    last_month_start = last_month_end.replace(day=1)
    return {
        "yesterday":    (yesterday, yesterday),
        "this-week":    (week_start, today),
        "last-week":    (last_week_start, last_week_end),
        "this-month":   (month_start, today),
        "last-month":   (last_month_start, last_month_end),
    }


def _resolve_range(args: argparse.Namespace, today: date) -> tuple[date, date]:
    """Return (start, end) from either a preset flag or explicit --start/--end."""
    presets = _presets(today)
    for name, (s, e) in presets.items():
        if getattr(args, name.replace("-", "_"), False):
            return s, e
    return args.start, args.end


def _build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser with subcommands: ingest, enrich, build."""
    parser = argparse.ArgumentParser(description="boxoffice.int data product pipeline")
    sub = parser.add_subparsers(dest="command", required=True)

    ingest = sub.add_parser("ingest", help="Ingestione raw da Cineguru")

    # Explicit range (both optional when a preset is used)
    ingest.add_argument("--start", type=date.fromisoformat, default=None, help="Data inizio YYYY-MM-DD")
    ingest.add_argument("--end",   type=date.fromisoformat, default=None, help="Data fine YYYY-MM-DD")

    # Preset shortcuts (mutually exclusive)
    presets_group = ingest.add_mutually_exclusive_group()
    presets_group.add_argument("--yesterday",   action="store_true", help="Ieri")
    presets_group.add_argument("--this-week",   action="store_true", help="Da lunedì a oggi")
    presets_group.add_argument("--last-week",   action="store_true", help="Settimana scorsa (lun–dom)")
    presets_group.add_argument("--this-month",  action="store_true", help="Dal 1° del mese a oggi")
    presets_group.add_argument("--last-month",  action="store_true", help="Mese scorso completo")

    ingest.add_argument("--delay", type=float, default=2.0, help="Delay richieste (secondi)")

    enrich = sub.add_parser("enrich", help="Arricchimento metadati TMDB")
    enrich.add_argument("--input", type=Path, required=True, help="CSV raw box office")

    build = sub.add_parser("build", help="Build data product analytics")
    build.add_argument("--input", type=Path, required=True, help="CSV raw box office")
    build.add_argument("--metadata", type=Path, default=None, help="CSV metadata opzionale")

    # --- seed: populate dim_date in Neon ---
    seed = sub.add_parser("seed", help="Popola dim_date su Neon (idempotente)")
    seed.add_argument("--start", type=date.fromisoformat, default=date(2015, 1, 1),
                      help="Data inizio (default: 2015-01-01)")
    seed.add_argument("--end",   type=date.fromisoformat, default=date(2035, 12, 31),
                      help="Data fine (default: 2035-12-31)")

    # --- load: ingest CSV into Neon star schema ---
    load = sub.add_parser("load", help="Carica CSV raw nel DB Neon (fact_box_office_daily)")
    load.add_argument("--input", type=Path, required=True, help="CSV raw box office da caricare")
    load.add_argument("--source-key", type=int, default=1, help="FK dim_source (default: 1=Cineguru)")

    # --- enrich-db: TMDB enrichment of dim_film rows in Neon ---
    enrich_db = sub.add_parser("enrich-db", help="Arricchisce dim_film con dati TMDB (solo righe senza tmdb_id)")
    enrich_db.add_argument("--delay", type=float, default=1.0,
                           help="Pausa tra chiamate TMDB (secondi, default: 1.0)")

    return parser


def main() -> None:
    """Entry point for the ``boxoffice-int`` CLI command."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    parser = _build_parser()
    args = parser.parse_args()

    if args.command == "ingest":
        preset_flags = ("yesterday", "this_week", "last_week", "this_month", "last_month")
        using_preset = any(getattr(args, f, False) for f in preset_flags)
        if not using_preset and (args.start is None or args.end is None):
            parser.error("ingest: specifica --start/--end oppure uno shortcut (--yesterday, --this-week, …)")
        start, end = _resolve_range(args, date.today())
        logging.getLogger(__name__).info("Range selezionato: %s → %s", start, end)
        path = scrape_cineguru(start=start, end=end, delay=args.delay)
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

    if args.command == "seed":
        from .warehouse.loader import get_connection, seed_dim_date  # optional dep
        conn = get_connection()
        n = seed_dim_date(conn, start=args.start, end=args.end)
        conn.close()
        print(f"dim_date: {n} righe inserite ({args.start} → {args.end})")
        return

    if args.command == "load":
        from .warehouse.loader import load_box_office_raw  # optional dep
        n = load_box_office_raw(csv_path=args.input, source_key=args.source_key)
        print(f"fact_box_office_daily: {n} righe inserite da {args.input.name}")
        return

    if args.command == "enrich-db":
        from .warehouse.enrich_db import enrich_dim_film  # optional dep
        n = enrich_dim_film(delay=args.delay)
        print(f"dim_film arricchiti: {n} film aggiornati con dati TMDB")
        return

    parser.print_help()


if __name__ == "__main__":
    main()
