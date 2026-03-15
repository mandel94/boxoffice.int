"""Shared constants and utilities used across all domain modules."""
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_RAW = ROOT_DIR / "data" / "raw"
DATA_CURATED = ROOT_DIR / "data" / "curated"
DATA_PRODUCTS = ROOT_DIR / "data" / "products"


def normalize_title(value: str) -> str:
    """Lowercase, strip, and collapse internal whitespace for fuzzy title matching."""
    return " ".join(value.strip().lower().split())
