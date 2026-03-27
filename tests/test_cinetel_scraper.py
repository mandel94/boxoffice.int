"""Tests per le funzioni pure di cinetel_scraper.py."""

import pytest

from boxoffice_int.domain.box_office_raw.cinetel_scraper import (
    COLUMN_MAP,
    _validate_headers,
    parse_currency,
    parse_date_it,
    parse_number,
)


class _MockHeaderCell:
    def __init__(self, text: str):
        self._text = text

    def inner_text(self) -> str:
        return self._text


class _MockPage:
    def __init__(self, header_texts: list[str]):
        self._headers = [_MockHeaderCell(t) for t in header_texts]

    def query_selector_all(self, selector: str) -> list:
        return self._headers


# ---------------------------------------------------------------------------
# parse_currency
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("value,expected", [
    ("€ 1.582.721", 1582721),
    ("€1.582.721",  1582721),
    ("1.582.721",   1582721),
    ("€ 127.955",   127955),
    ("56670",       56670),
])
def test_parse_currency_valid(value, expected):
    assert parse_currency(value) == expected


@pytest.mark.parametrize("value", [None, "", "   ", "n.d."])
def test_parse_currency_invalid(value):
    assert parse_currency(value) is None


# ---------------------------------------------------------------------------
# parse_number
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("value,expected", [
    ("195.734", 195734),
    ("9.999",   9999),
    ("7.234",   7234),
    ("0",       0),
])
def test_parse_number_valid(value, expected):
    assert parse_number(value) == expected


@pytest.mark.parametrize("value", [None, "", "n.d."])
def test_parse_number_invalid(value):
    assert parse_number(value) is None


# ---------------------------------------------------------------------------
# parse_date_it
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("value,expected", [
    ("27/03/2026", "2026-03-27"),
    ("7/3/2026",   "2026-03-07"),
    ("27-03-2026", "2026-03-27"),
    ("2026-03-27", "2026-03-27"),  # ISO passthrough
])
def test_parse_date_it_valid(value, expected):
    assert parse_date_it(value) == expected


@pytest.mark.parametrize("value", [None, "", "   "])
def test_parse_date_it_invalid(value):
    assert parse_date_it(value) is None


# ---------------------------------------------------------------------------
# Test _validate_headers
# ---------------------------------------------------------------------------

class TestValidateHeaders:
    def test_known_italian_headers_map_correctly(self):
        headers = [
            "Posizione", "Titolo", "Prima Programmazione",
            "Nazione", "Distribuzione",
            "Incasso", "Presenze",
            "Incasso aggiornato", "Presenze aggiornate",
        ]
        page = _MockPage(headers)
        result = _validate_headers(page)
        assert result["rank"] == 0
        assert result["title"] == 1
        assert result["release_date"] == 2
        assert result["country"] == 3
        assert result["distribution"] == 4
        assert result["gross"] == 5
        assert result["attendance"] == 6
        assert result["gross_total"] == 7
        assert result["attendance_total"] == 8

    def test_empty_page_falls_back_to_column_map(self):
        page = _MockPage([])
        result = _validate_headers(page)
        assert result == COLUMN_MAP

    def test_partial_headers_fill_missing_from_column_map(self):
        """Colonne mancanti devono essere completate dagli indici predefiniti."""
        headers = ["Posizione", "Titolo"]  # solo 2 colonne su 9
        page = _MockPage(headers)
        result = _validate_headers(page)
        # I campi non trovati nell'header usano il fallback COLUMN_MAP
        for field in COLUMN_MAP:
            assert field in result

    def test_result_covers_all_column_map_fields(self):
        headers = [
            "Posizione", "Titolo", "Prima Programmazione",
            "Nazione", "Distribuzione",
            "Incasso", "Presenze",
            "Incasso aggiornato", "Presenze aggiornate",
        ]
        page = _MockPage(headers)
        result = _validate_headers(page)
        assert set(result.keys()) >= set(COLUMN_MAP.keys())


# ---------------------------------------------------------------------------
# Integrazione offline — HTML reale caricato via Playwright set_content()
# ---------------------------------------------------------------------------

from pathlib import Path

from playwright.sync_api import sync_playwright

from boxoffice_int.domain.box_office_raw.cinetel_scraper import get_rows, parse_row

FIXTURE_HTML = Path(__file__).parent / "fixtures" / "cinetel_html_for_scrape.html"

ROW1_EXPECTED = {
    "rank": 1,
    "title": "L'ULTIMA MISSIONE: PROJECT HAIL MARY",
    "release_date": "2026-03-19",
    "country": "USA",
    "distribution": "EAGLE PICTURES",
    "gross": 83584,
    "attendance": 11003,
    "gross_total": 1816104,
    "attendance_total": 227702,
}


@pytest.fixture(scope="module")
def parsed_rows():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(FIXTURE_HTML.as_uri(), wait_until="domcontentloaded")
        # Scoped alla prima ngx-datatable (giornaliera), come in _fetch_table_data
        table_el = page.query_selector("ngx-datatable")
        col_map = _validate_headers(table_el)
        rows = get_rows(table_el)
        records = [parse_row(r, col_map) for r in rows]
        records = [r for r in records if r is not None]
        browser.close()
    return records


def test_offline_row_count(parsed_rows):
    assert len(parsed_rows) == 10


def test_offline_row1_rank(parsed_rows):
    assert parsed_rows[0]["rank"] == ROW1_EXPECTED["rank"]


def test_offline_row1_title(parsed_rows):
    assert parsed_rows[0]["title"] == ROW1_EXPECTED["title"]


def test_offline_row1_release_date(parsed_rows):
    assert parsed_rows[0]["release_date"] == ROW1_EXPECTED["release_date"]


def test_offline_row1_gross(parsed_rows):
    assert parsed_rows[0]["gross"] == ROW1_EXPECTED["gross"]


def test_offline_row1_attendance(parsed_rows):
    assert parsed_rows[0]["attendance"] == ROW1_EXPECTED["attendance"]


def test_offline_row1_gross_total(parsed_rows):
    assert parsed_rows[0]["gross_total"] == ROW1_EXPECTED["gross_total"]


def test_offline_ranks_are_sequential(parsed_rows):
    ranks = [r["rank"] for r in parsed_rows]
    assert ranks == list(range(1, 11))
