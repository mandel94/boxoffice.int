import os
from pathlib import Path

import pandas as pd
import requests

from ...common import DATA_CURATED, normalize_title

TMDB_BASE = "https://api.themoviedb.org/3"


def _get_api_key() -> str:
    api_key = os.getenv("TMDB_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("Variabile ambiente TMDB_API_KEY non impostata")
    return api_key


def _search_movie(title: str, api_key: str) -> dict | None:
    response = requests.get(
        f"{TMDB_BASE}/search/movie",
        params={"api_key": api_key, "query": title, "language": "it-IT", "include_adult": False},
        timeout=30,
    )
    response.raise_for_status()
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


def enrich_titles_with_tmdb(input_path: Path) -> Path:
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
    pd.DataFrame(rows).drop_duplicates(subset=["title_norm"]).to_csv(output_path, index=False)
    return output_path
