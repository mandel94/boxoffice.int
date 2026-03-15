import logging
import os
import time
from pathlib import Path

import pandas as pd
import requests

from ...common import DATA_CURATED, normalize_title

LOG = logging.getLogger(__name__)
from ...contracts import cast_to_contract, load_contract, validate

TMDB_BASE = "https://api.themoviedb.org/3"


def _get_api_key() -> str:
    """Read TMDB_API_KEY from the environment, raising RuntimeError if absent."""
    api_key = os.getenv("TMDB_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("Variabile ambiente TMDB_API_KEY non impostata")
    return api_key


_RETRYABLE = {429, 500, 502, 503, 504}
_MAX_RETRIES = 3
_BACKOFF = 2.0  # seconds, doubled each attempt


def _search_movie(title: str, api_key: str) -> dict | None:
    """
    Search TMDB for *title* and return metadata for the top result.

    Retries up to ``_MAX_RETRIES`` times with exponential back-off on retryable
    HTTP errors (429, 5xx) and network exceptions. Returns ``None`` if no
    results are found or all retries are exhausted.
    """
    delay = _BACKOFF
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            response = requests.get(
                f"{TMDB_BASE}/search/movie",
                params={"api_key": api_key, "query": title, "language": "it-IT", "include_adult": False},
                timeout=30,
            )
            if response.status_code in _RETRYABLE:
                LOG.warning("TMDB HTTP %s per '%s' (tentativo %d/%d)", response.status_code, title, attempt, _MAX_RETRIES)
                if attempt < _MAX_RETRIES:
                    time.sleep(delay)
                    delay *= 2
                    continue
                response.raise_for_status()
            response.raise_for_status()
        except requests.RequestException as exc:
            LOG.warning("TMDB errore per '%s' (tentativo %d/%d): %s", title, attempt, _MAX_RETRIES, exc)
            if attempt < _MAX_RETRIES:
                time.sleep(delay)
                delay *= 2
                continue
            LOG.error("TMDB: '%s' saltato dopo %d tentativi", title, _MAX_RETRIES)
            return None
        results = response.json().get("results", [])
        if not results:
            return None
        top = results[0]
        return {
            "title_norm": normalize_title(title),
            "tmdb_id": top.get("id"),
            "original_title": top.get("original_title"),
            "release_date": top.get("release_date"),
            "original_language": top.get("original_language"),
            "popularity": top.get("popularity"),
            "vote_average": top.get("vote_average"),
            "vote_count": top.get("vote_count"),
        }
    return None


def enrich_titles_with_tmdb(input_path: Path) -> Path:
    """
    Look up every unique title in *input_path* on TMDB and write a metadata CSV.

    Deduplicates titles by normalised form before querying. Output is validated
    against the ``film-metadata`` contract and written to
    ``DATA_CURATED/film_metadata/film_metadata.csv``.

    Returns the path to the written CSV.
    """
    dataframe = pd.read_csv(input_path)
    titles = sorted(set(dataframe["title"].dropna().astype(str).tolist()))
    api_key = _get_api_key()

    rows: list[dict] = []
    for title in titles:
        result = _search_movie(title, api_key)
        if result:
            rows.append(result)

    output_dir = DATA_CURATED / "film_metadata"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "film_metadata.csv"

    metadata = pd.DataFrame(rows).drop_duplicates(subset=["title_norm"])

    contract = load_contract("film-metadata")
    metadata = cast_to_contract(metadata, contract)
    validate(metadata, contract)

    metadata.to_csv(output_path, index=False)
    return output_path
