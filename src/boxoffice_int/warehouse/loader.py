"""
warehouse/loader.py
-------------------
Populates the star schema from box-office raw CSV files.

Entry points
~~~~~~~~~~~~
- ``seed_dim_date(conn)``         — fill dim_date for 2015-2035 (idempotent)
- ``load_box_office_raw(csv_path, source_key)`` — load one raw CSV into the fact table

Connection
~~~~~~~~~~
All functions accept a live ``psycopg2`` connection.  The connection string is
read from the ``BOXOFFICE_DB_URL`` environment variable by ``get_connection()``.

Example
~~~~~~~
    conn = get_connection()
    seed_dim_date(conn)
    n = load_box_office_raw(Path("data/raw/box_office_raw/cineguru_2026-03-19_2026-03-19.csv"))
    print(f"Inserted {n} rows")
    conn.close()
"""

from __future__ import annotations

import logging
import os
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import psycopg2
import psycopg2.extras
from rapidfuzz import fuzz, process

from ..common import normalize_title
from ..contracts import load_contract, validate, cast_to_contract

LOG = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_MONTH_NAMES_IT = {
    1: "gennaio", 2: "febbraio", 3: "marzo", 4: "aprile",
    5: "maggio",  6: "giugno",   7: "luglio", 8: "agosto",
    9: "settembre", 10: "ottobre", 11: "novembre", 12: "dicembre",
}

_SEASONS = {
    12: "inverno", 1: "inverno", 2: "inverno",
    3: "primavera", 4: "primavera", 5: "primavera",
    6: "estate", 7: "estate", 8: "estate",
    9: "autunno", 10: "autunno", 11: "autunno",
}

# Italian national fixed-date holidays (month, day)
_FIXED_HOLIDAYS = {
    (1, 1):  "Capodanno",
    (1, 6):  "Epifania",
    (4, 25): "Festa della Liberazione",
    (5, 1):  "Festa dei Lavoratori",
    (6, 2):  "Festa della Repubblica",
    (8, 15): "Ferragosto",
    (11, 1): "Ognissanti",
    (12, 8): "Immacolata Concezione",
    (12, 25): "Natale",
    (12, 26): "Santo Stefano",
}


def _easter(year: int) -> date:
    """Anonymous Gregorian algorithm (Butcher's algorithm) for Easter Sunday."""
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month, day = divmod(h + l - 7 * m + 114, 31)
    return date(year, month, day + 1)


def _holiday_name(d: date) -> Optional[str]:
    """Return Italian holiday name for *d*, or None."""
    fixed = _FIXED_HOLIDAYS.get((d.month, d.day))
    if fixed:
        return fixed
    easter = _easter(d.year)
    if d == easter:
        return "Pasqua"
    if d == easter + timedelta(days=1):
        return "Pasquetta"
    return None


def _cinema_week(d: date) -> int:
    """
    Italian cinema week number: weeks run Thu-Wed.
    Returns the ISO week number of the Thursday that opened the current week.
    """
    # Roll back to the most recent Thursday (weekday 3)
    days_since_thu = (d.weekday() - 3) % 7
    thu = d - timedelta(days=days_since_thu)
    return thu.isocalendar()[1]


def _date_key(d: date) -> int:
    return d.year * 10000 + d.month * 100 + d.day


# ---------------------------------------------------------------------------
# Database connection
# ---------------------------------------------------------------------------

def get_connection() -> psycopg2.extensions.connection:
    """Return a new psycopg2 connection from ``BOXOFFICE_DB_URL``."""
    url = os.getenv("BOXOFFICE_DB_URL", "").strip()
    if not url:
        raise RuntimeError("Environment variable BOXOFFICE_DB_URL is not set.")
    return psycopg2.connect(url)


# ---------------------------------------------------------------------------
# dim_date seeder
# ---------------------------------------------------------------------------

def seed_dim_date(
    conn: psycopg2.extensions.connection,
    start: date = date(2015, 1, 1),
    end: date = date(2035, 12, 31),
) -> int:
    """
    Populate dim_date for every date in [start, end].
    Uses INSERT ... ON CONFLICT DO NOTHING — fully idempotent.
    Returns the number of rows actually inserted.
    """
    rows = []
    current = start
    while current <= end:
        iso = current.isocalendar()
        h_name = _holiday_name(current)
        rows.append((
            _date_key(current),           # date_key
            current,                      # full_date
            current.year,                 # year
            (current.month - 1) // 3 + 1, # quarter
            current.month,                # month
            _MONTH_NAMES_IT[current.month],# month_name_it
            iso[1],                       # week_number (ISO)
            iso[2],                       # day_of_week (1=Mon)
            iso[2] >= 6,                  # is_weekend
            h_name is not None,           # is_holiday
            h_name,                       # holiday_name
            _SEASONS[current.month],      # season
            _cinema_week(current),        # cinema_week
        ))
        current += timedelta(days=1)

    sql = """
        INSERT INTO dim_date (
            date_key, full_date, year, quarter, month, month_name_it,
            week_number, day_of_week, is_weekend, is_holiday, holiday_name,
            season, cinema_week
        )
        VALUES %s
        ON CONFLICT (date_key) DO NOTHING
    """
    with conn.cursor() as cur:
        # Single batch (no page_size) so cur.rowcount reflects all inserted rows.
        psycopg2.extras.execute_values(cur, sql, rows)
        inserted = cur.rowcount
    conn.commit()
    LOG.info("dim_date seeded: %d rows inserted (%s to %s)", inserted, start, end)
    return inserted


# ---------------------------------------------------------------------------
# dim_film helpers — exact + fuzzy lookup / insert
# ---------------------------------------------------------------------------

def _fetch_active_films(cur) -> dict[str, int]:
    """
    Return {title_cineguru_lower: film_key} for all is_current=TRUE rows.
    Used to build an in-process lookup cache per loader run.
    """
    cur.execute(
        "SELECT film_key, title_cineguru FROM dim_film WHERE is_current = TRUE"
    )
    return {row[1].lower(): row[0] for row in cur.fetchall()}


def _resolve_film_key(
    title: str,
    cache: dict[str, int],
    cur,
    valid_from: date,
    fuzzy_threshold: int = 85,
) -> int:
    """
    Resolve a Cineguru title to a film_key:
      1. Exact match (case-insensitive) on title_cineguru in cache.
      2. Fuzzy match using RapidFuzz token_set_ratio >= fuzzy_threshold.
      3. Insert a new dim_film row (is_current=TRUE) and return its key.

    The cache is updated in-place so subsequent titles in the same batch
    benefit from earlier inserts.
    """
    title_lower = title.lower()

    # 1. Exact match
    if title_lower in cache:
        return cache[title_lower]

    # 2. Fuzzy match
    if cache:
        match = process.extractOne(
            title_lower,
            cache.keys(),
            scorer=fuzz.token_set_ratio,
            score_cutoff=fuzzy_threshold,
        )
        if match:
            matched_title, score, _ = match
            LOG.debug("Fuzzy match '%s' -> '%s' (score=%d)", title, matched_title, score)
            return cache[matched_title]

    # 3. Not found — insert new dim_film row
    cur.execute(
        """
        INSERT INTO dim_film (title_cineguru, valid_from, valid_to, is_current)
        VALUES (%s, %s, '9999-12-31', TRUE)
        RETURNING film_key
        """,
        (title, valid_from),
    )
    film_key = cur.fetchone()[0]
    cache[title_lower] = film_key
    LOG.info("New film inserted: '%s' -> film_key=%d", title, film_key)
    return film_key


# ---------------------------------------------------------------------------
# dim_source helpers
# ---------------------------------------------------------------------------

# In-process cache: source_name_lower → source_key (populated lazily per run)
_SOURCE_KEY_CACHE: dict[str, int] = {}


def _resolve_source_key(source_name: str, cur) -> int:
    """
    Return the source_key for *source_name* by querying dim_source.

    Result is cached in-process for the duration of the Python process
    (thread-safe for single-threaded use).

    Raises
    ------
    ValueError
        If *source_name* does not exist in dim_source.
    """
    key = source_name.strip().lower()
    if key in _SOURCE_KEY_CACHE:
        return _SOURCE_KEY_CACHE[key]
    cur.execute("SELECT source_key FROM dim_source WHERE LOWER(name) = %s", (key,))
    row = cur.fetchone()
    if row is None:
        raise ValueError(
            f"source_name '{source_name}' non trovata in dim_source. "
            "Verificare lo schema e il seed."
        )
    _SOURCE_KEY_CACHE[key] = row[0]
    return row[0]


# ---------------------------------------------------------------------------
# Main loader
# ---------------------------------------------------------------------------

def load_box_office_raw(csv_path: Path, source_key: int = 1) -> int:
    """
    Load a box-office raw CSV into ``fact_box_office_daily``.

    Steps
    -----
    1. Read and validate the CSV against the ``box-office-raw-daily`` contract.
    2. For each title resolve (or create) a ``dim_film`` row.
    3. Insert into ``fact_box_office_daily`` with
       ``ON CONFLICT (date_key, film_key, source_key) DO NOTHING``
       so the loader is fully idempotent.

    Parameters
    ----------
    csv_path:
        Path to the raw CSV produced by the ingest step.
    source_key:
        FK into ``dim_source`` — used as fallback when the CSV does not
        contain a ``source`` column.  Defaults to 1 (Cineguru).

    Returns
    -------
    int
        Number of rows actually inserted into the fact table.
    """
    csv_path = Path(csv_path)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    # --- 1. Load and validate -------------------------------------------
    df = pd.read_csv(csv_path)
    contract = load_contract("box-office-raw-daily")
    df = cast_to_contract(df, contract)
    validate(df, contract)
    LOG.info("Loaded %d rows from %s", len(df), csv_path.name)

    # Determine whether the CSV carries a source column
    has_source_column = "source" in df.columns

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Build in-memory film cache (all active films)
            film_cache = _fetch_active_films(cur)

            # Pre-resolve source_key if CSV has no source column (backward-compat)
            resolved_source_key: int = source_key

            inserted_count = 0
            for _, row in df.iterrows():
                row_date: date = row["date"]
                if hasattr(row_date, "date"):
                    row_date = row_date.date()  # Timestamp -> date

                dk = _date_key(row_date)

                # -- 2. Resolve source key --------------------------------
                if has_source_column and not pd.isna(row.get("source")):
                    resolved_source_key = _resolve_source_key(str(row["source"]), cur)
                else:
                    resolved_source_key = source_key

                # -- 3. Resolve film key ---------------------------------
                film_key = _resolve_film_key(
                    title=str(row["title"]),
                    cache=film_cache,
                    cur=cur,
                    valid_from=row_date,
                )

                # -- 4. Insert fact row (idempotent) ---------------------
                cur.execute(
                    """
                    INSERT INTO fact_box_office_daily (
                        date_key, film_key, source_key,
                        rank, gross_eur, admissions, cinemas,
                        avg_per_cinema_eur, total_gross_eur
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (date_key, film_key, source_key) DO NOTHING
                    """,
                    (
                        dk,
                        film_key,
                        resolved_source_key,
                        int(row["rank"]),
                        int(row["gross_eur"]),
                        None if pd.isna(row.get("admissions")) else int(row["admissions"]),
                        None if pd.isna(row.get("cinemas")) else int(row["cinemas"]),
                        None if pd.isna(row.get("avg_per_cinema_eur")) else int(row["avg_per_cinema_eur"]),
                        None if pd.isna(row.get("total_gross_eur")) else int(row["total_gross_eur"]),
                    ),
                )
                if cur.rowcount:
                    inserted_count += 1

        conn.commit()
        LOG.info(
            "fact_box_office_daily: %d/%d rows inserted from %s",
            inserted_count, len(df), csv_path.name,
        )
        return inserted_count

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# SCD2 helper — update a film's distributor
# ---------------------------------------------------------------------------

def update_film_distributor(
    conn: psycopg2.extensions.connection,
    film_key: int,
    new_distributor_key: int,
    change_date: date,
) -> int:
    """
    Apply a Type-2 SCD update to dim_film for a distributor change.

    Closes the current active row (sets valid_to = change_date - 1,
    is_current = FALSE) and inserts a new row copying all other attributes
    with the new distributor_key.

    Returns the new film_key.
    """
    with conn.cursor() as cur:
        # Fetch current active row
        cur.execute(
            """
            SELECT title_cineguru, tmdb_id, title_it, title_orig,
                   director, runtime_min, release_date
            FROM dim_film
            WHERE film_key = %s AND is_current = TRUE
            """,
            (film_key,),
        )
        row = cur.fetchone()
        if row is None:
            raise ValueError(f"No active dim_film row for film_key={film_key}")

        title_cineguru, tmdb_id, title_it, title_orig, director, runtime_min, release_date = row

        # Close current row
        cur.execute(
            """
            UPDATE dim_film
            SET valid_to = %s, is_current = FALSE
            WHERE film_key = %s AND is_current = TRUE
            """,
            (change_date - timedelta(days=1), film_key),
        )

        # Insert new version
        cur.execute(
            """
            INSERT INTO dim_film (
                tmdb_id, title_cineguru, title_it, title_orig,
                distributor_key, director, runtime_min, release_date,
                valid_from, valid_to, is_current
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, '9999-12-31', TRUE)
            RETURNING film_key
            """,
            (
                tmdb_id, title_cineguru, title_it, title_orig,
                new_distributor_key, director, runtime_min, release_date,
                change_date,
            ),
        )
        new_key = cur.fetchone()[0]

    conn.commit()
    LOG.info(
        "SCD2 update: film_key %d -> %d (distributor_key=%d, from %s)",
        film_key, new_key, new_distributor_key, change_date,
    )
    return new_key
