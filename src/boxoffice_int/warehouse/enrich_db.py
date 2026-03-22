"""
warehouse/enrich_db.py
----------------------
TMDB enrichment for ``dim_film`` rows that have no ``tmdb_id``.

Entry point
~~~~~~~~~~~
``enrich_dim_film(delay)`` — reads all ``dim_film`` rows where
``tmdb_id IS NULL AND is_current = TRUE``, looks up each title on TMDB,
and writes back: ``tmdb_id``, ``title_orig``, ``director``,
``runtime_min``, ``release_date``.

It also inserts rows into ``bridge_film_genre``, setting ``is_primary``
for the first (main) genre returned by TMDB.

The function is idempotent: films already enriched (``tmdb_id IS NOT NULL``)
are never touched.

Connection
~~~~~~~~~~
Uses ``BOXOFFICE_DB_URL`` via ``get_connection()`` (same as the loader).
TMDB credentials are read from ``TMDB_API_KEY``.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import date

import requests

from .loader import get_connection

LOG = logging.getLogger(__name__)

TMDB_BASE = "https://api.themoviedb.org/3"
_RETRYABLE = {429, 500, 502, 503, 504}
_MAX_RETRIES = 3
_BACKOFF = 2.0  # seconds; doubled each retry


# ---------------------------------------------------------------------------
# TMDB helpers
# ---------------------------------------------------------------------------

def _get_api_key() -> str:
    api_key = os.getenv("TMDB_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("Variabile ambiente TMDB_API_KEY non impostata.")
    return api_key


def _tmdb_get(url: str, params: dict) -> dict | None:
    """GET a TMDB endpoint with retry logic. Returns the parsed JSON or None."""
    delay = _BACKOFF
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params, timeout=30)
            if r.status_code in _RETRYABLE:
                LOG.warning("TMDB HTTP %s (tentativo %d/%d)", r.status_code, attempt, _MAX_RETRIES)
                if attempt < _MAX_RETRIES:
                    time.sleep(delay)
                    delay *= 2
                    continue
                r.raise_for_status()
            r.raise_for_status()
            return r.json()
        except requests.RequestException as exc:
            LOG.warning("TMDB errore (tentativo %d/%d): %s", attempt, _MAX_RETRIES, exc)
            if attempt < _MAX_RETRIES:
                time.sleep(delay)
                delay *= 2
                continue
            return None
    return None


def _search_and_detail(title: str, api_key: str) -> dict | None:
    """
    Search TMDB for *title*, then fetch movie details (credits, genres).

    Returns a dict with keys:
        tmdb_id, title_orig, release_date (date | None),
        runtime_min (int | None), director (str | None),
        genre_ids (list[int])

    Returns None if no results or all retries exhausted.
    """
    # --- Step 1: search ---
    data = _tmdb_get(
        f"{TMDB_BASE}/search/movie",
        {"api_key": api_key, "query": title, "language": "it-IT", "include_adult": False},
    )
    if not data or not data.get("results"):
        return None

    top = data["results"][0]
    tmdb_id: int = top["id"]

    # --- Step 2: movie detail (runtime + credits) ---
    detail = _tmdb_get(
        f"{TMDB_BASE}/movie/{tmdb_id}",
        {"api_key": api_key, "language": "it-IT", "append_to_response": "credits"},
    )

    if detail is None:
        # Return partial result (no runtime or director, but we have tmdb_id)
        release_date = _parse_date(top.get("release_date"))
        return {
            "tmdb_id": tmdb_id,
            "title_orig": top.get("original_title"),
            "release_date": release_date,
            "runtime_min": None,
            "director": None,
            "genre_ids": top.get("genre_ids", []),
        }

    director = next(
        (p["name"] for p in detail.get("credits", {}).get("crew", [])
         if p.get("job") == "Director"),
        None,
    )

    return {
        "tmdb_id": tmdb_id,
        "title_orig": detail.get("original_title"),
        "release_date": _parse_date(detail.get("release_date")),
        "runtime_min": detail.get("runtime") or None,
        "director": director,
        "genre_ids": [g["id"] for g in detail.get("genres", [])],
    }


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def enrich_dim_film(delay: float = 1.0) -> int:
    """
    Enrich ``dim_film`` rows that have ``tmdb_id IS NULL`` with TMDB metadata.

    For each unmatched film:
    - Updates: ``tmdb_id``, ``title_orig``, ``director``, ``runtime_min``,
      ``release_date``
    - Inserts into ``bridge_film_genre`` (marks first genre as primary)

    Parameters
    ----------
    delay:
        Sleep duration (seconds) between TMDB API calls to respect rate limits.

    Returns
    -------
    int
        Number of films successfully updated.
    """
    api_key = _get_api_key()
    conn = get_connection()
    updated = 0
    films: list[tuple[int, str]] = []

    try:
        with conn.cursor() as cur:
            # Load all unenriched active films
            cur.execute(
                """
                SELECT film_key, title_cineguru
                FROM   dim_film
                WHERE  tmdb_id IS NULL AND is_current = TRUE
                ORDER  BY film_key
                """
            )
            films = cur.fetchall()
            LOG.info("Film senza tmdb_id: %d", len(films))

            if not films:
                return 0

            # Pre-load genre key mapping {tmdb_genre_id: genre_key}
            cur.execute(
                "SELECT tmdb_genre_id, genre_key FROM dim_genre WHERE tmdb_genre_id IS NOT NULL"
            )
            genre_map: dict[int, int] = {row[0]: row[1] for row in cur.fetchall()}

        # Process each film in its own mini-transaction so a single failure
        # doesn't roll back the entire batch.
        for film_key, title in films:
            time.sleep(delay)
            result = _search_and_detail(title, api_key)
            if not result:
                LOG.warning("Nessun risultato TMDB per '%s' (film_key=%d)", title, film_key)
                continue

            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE dim_film
                        SET    tmdb_id      = %s,
                               title_orig   = %s,
                               director     = %s,
                               runtime_min  = %s,
                               release_date = %s
                        WHERE  film_key = %s
                        """,
                        (
                            result["tmdb_id"],
                            result["title_orig"],
                            result["director"],
                            result["runtime_min"],
                            result["release_date"],
                            film_key,
                        ),
                    )

                    # Populate bridge_film_genre
                    for i, tmdb_genre_id in enumerate(result["genre_ids"]):
                        genre_key = genre_map.get(tmdb_genre_id)
                        if genre_key is None:
                            continue
                        cur.execute(
                            """
                            INSERT INTO bridge_film_genre (film_key, genre_key, is_primary)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (film_key, genre_key) DO NOTHING
                            """,
                            (film_key, genre_key, i == 0),
                        )

                conn.commit()
                updated += 1
                LOG.info(
                    "Arricchito '%s' → tmdb_id=%d director=%s",
                    title, result["tmdb_id"], result["director"],
                )

            except Exception as exc:
                conn.rollback()
                LOG.error("Errore aggiornando film_key=%d ('%s'): %s", film_key, title, exc)

    finally:
        conn.close()

    LOG.info("Arricchimento TMDB completato: %d/%d film aggiornati", updated, len(films))
    return updated
