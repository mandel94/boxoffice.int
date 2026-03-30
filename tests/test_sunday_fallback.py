"""
Tests per il modulo sunday_fallback.

Copre le funzioni pure (nessuna rete, nessun DB):
  - is_weekend_article()
  - _extract_sunday_date()
  - _find_weekend_url_in_links()
  - compute_sunday_records()
  - parse_article() con HTML fine-settimana reale
"""
from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from boxoffice_int.domain.box_office_raw.cineguru_scraper import (
    is_weekend_article,
    _extract_sunday_date,
    parse_article,
)
from boxoffice_int.domain.box_office_raw.sunday_fallback import (
    _find_weekend_url_in_links,
    _prev_thursday,
    compute_sunday_records,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

FIXTURES_DIR = Path(__file__).parent / "fixtures"
WEEKEND_HTML = (FIXTURES_DIR / "fine_settimana_article.html").read_text(encoding="utf-8")

# URL di esempio per articoli fine-settimana (due slug diversi usati da Cineguru)
_WEEKEND_URLS = [
    # formato storico
    "https://cineguru.screenweek.it/2026/03/15/box-office-del-fine-settimana-12-15-marzo/",
    "https://cineguru.screenweek.it/2026/03/22/classifica-fine-settimana-19-22-marzo/",
    "https://cineguru.screenweek.it/2026/01/18/incassi-fine-settimana-15-18-gennaio/",
    # formato "week-end" (osservato dal 2026-03-29)
    "https://cineguru.screenweek.it/2026/03/lultima-missione-si-ripete-e-guida-il-box-office-del-week-end-26-29-marzo-49417/",
]

_DAILY_URLS = [
    "https://cineguru.screenweek.it/2026/03/14/box-office-venerdi-14-marzo/",
    "https://cineguru.screenweek.it/2026/03/13/box-office-giovedi-13-marzo/",
    "https://cineguru.screenweek.it/2026/03/12/box-office-mercoledi-12-marzo/",
]


# ---------------------------------------------------------------------------
# is_weekend_article
# ---------------------------------------------------------------------------

class TestIsWeekendArticle:
    @pytest.mark.parametrize("url", _WEEKEND_URLS)
    def test_detects_weekend_urls(self, url: str):
        assert is_weekend_article(url) is True

    @pytest.mark.parametrize("url", _DAILY_URLS)
    def test_ignores_daily_urls(self, url: str):
        assert is_weekend_article(url) is False

    def test_case_insensitive_fine_settimana(self):
        assert is_weekend_article(
            "https://cineguru.screenweek.it/2026/03/15/BOX-OFFICE-FINE-SETTIMANA-12-15-MARZO/"
        ) is True

    def test_case_insensitive_week_end(self):
        assert is_weekend_article(
            "https://cineguru.screenweek.it/2026/03/BOX-OFFICE-WEEK-END-26-29-MARZO-49417/"
        ) is True


# ---------------------------------------------------------------------------
# _extract_sunday_date
# ---------------------------------------------------------------------------

class TestExtractSundayDate:
    def test_extracts_sunday_march_15(self):
        url = "https://cineguru.screenweek.it/2026/03/15/box-office-del-fine-settimana-12-15-marzo/"
        assert _extract_sunday_date(url) == date(2026, 3, 15)

    def test_extracts_sunday_january_18(self):
        url = "https://cineguru.screenweek.it/2026/01/18/incassi-fine-settimana-15-18-gennaio/"
        assert _extract_sunday_date(url) == date(2026, 1, 18)

    def test_returns_none_for_invalid_url(self):
        assert _extract_sunday_date("https://cineguru.screenweek.it/") is None

    def test_named_month_overrides_url_month(self):
        # URL path month = 03, named month = marzo → same, but test logic
        url = "https://cineguru.screenweek.it/2026/03/15/fine-settimana-12-15-marzo/"
        result = _extract_sunday_date(url)
        assert result == date(2026, 3, 15)

    def test_extracts_sunday_from_week_end_slug(self):
        # Formato reale osservato dal 2026-03-29: slug "week-end" con ID numerico finale
        url = "https://cineguru.screenweek.it/2026/03/lultima-missione-si-ripete-e-guida-il-box-office-del-week-end-26-29-marzo-49417/"
        assert _extract_sunday_date(url) == date(2026, 3, 29)


# ---------------------------------------------------------------------------
# _find_weekend_url_in_links
# ---------------------------------------------------------------------------

class TestFindWeekendUrlInLinks:
    def test_finds_matching_url(self):
        links = [
            ("https://cineguru.screenweek.it/2026/03/14/box-office-venerdi-14-marzo/", date(2026, 3, 14)),
            ("https://cineguru.screenweek.it/2026/03/15/box-office-fine-settimana-12-15-marzo/", date(2026, 3, 15)),
            ("https://cineguru.screenweek.it/2026/03/13/box-office-giovedi-13-marzo/", date(2026, 3, 13)),
        ]
        result = _find_weekend_url_in_links(links, date(2026, 3, 15))
        assert result == "https://cineguru.screenweek.it/2026/03/15/box-office-fine-settimana-12-15-marzo/"

    def test_returns_none_when_not_found(self):
        links = [
            ("https://cineguru.screenweek.it/2026/03/14/box-office-venerdi-14-marzo/", date(2026, 3, 14)),
        ]
        assert _find_weekend_url_in_links(links, date(2026, 3, 15)) is None

    def test_returns_none_for_empty_links(self):
        assert _find_weekend_url_in_links([], date(2026, 3, 15)) is None


# ---------------------------------------------------------------------------
# _prev_thursday
# ---------------------------------------------------------------------------

class TestPrevThursday:
    def test_sunday_gives_thursday_same_week(self):
        sunday = date(2026, 3, 15)  # domenica
        assert _prev_thursday(sunday) == date(2026, 3, 12)

    def test_thursday_gives_itself(self):
        thursday = date(2026, 3, 12)
        assert _prev_thursday(thursday) == thursday


# ---------------------------------------------------------------------------
# compute_sunday_records — funzione pura
# ---------------------------------------------------------------------------

class TestComputeSundayRecords:
    def _make_weekend(self) -> list[dict]:
        return [
            {"rank": 1, "title": "Jumpers – Un salto", "gross_eur": 1_518_306,
             "admissions": 201_587, "cinemas": 562, "avg_per_cinema_eur": None,
             "total_gross_eur": 3_430_860, "source": "CINEGURU"},
            {"rank": 2, "title": "Un bel giorno", "gross_eur": 1_266_412,
             "admissions": 170_458, "cinemas": 589, "avg_per_cinema_eur": None,
             "total_gross_eur": 3_260_046, "source": "CINEGURU"},
        ]

    def _make_thu_fri_sat(self, sunday: date) -> list[dict]:
        sat = sunday - __import__("datetime").timedelta(days=1)
        return [
            # Jumpers: Thu grosses
            {"title": "Jumpers – Un salto", "gross": 300_000, "cinemas": None, "row_date": sunday - __import__("datetime").timedelta(days=3)},
            # Jumpers: Fri grosses
            {"title": "Jumpers – Un salto", "gross": 350_000, "cinemas": None, "row_date": sunday - __import__("datetime").timedelta(days=2)},
            # Jumpers: Sat grosses + cinemas (sabato)
            {"title": "Jumpers – Un salto", "gross": 500_000, "cinemas": 562, "row_date": sat},
            # Un bel giorno: Thu+Fri+Sat
            {"title": "Un bel giorno", "gross": 250_000, "cinemas": None, "row_date": sunday - __import__("datetime").timedelta(days=3)},
            {"title": "Un bel giorno", "gross": 300_000, "cinemas": None, "row_date": sunday - __import__("datetime").timedelta(days=2)},
            {"title": "Un bel giorno", "gross": 400_000, "cinemas": 589, "row_date": sat},
        ]

    def test_sunday_gross_is_difference(self):
        sunday = date(2026, 3, 15)
        weekend = self._make_weekend()
        thu_fri_sat = self._make_thu_fri_sat(sunday)
        result = compute_sunday_records(weekend, thu_fri_sat, sunday)

        assert len(result) == 2
        # Jumpers: 1_518_306 - (300_000 + 350_000 + 500_000) = 368_306
        jumpers = next(r for r in result if "jumpers" in r["title"].lower())
        assert jumpers["gross_eur"] == 368_306

        # Un bel giorno: 1_266_412 - (250_000 + 300_000 + 400_000) = 316_412
        bel_giorno = next(r for r in result if "bel giorno" in r["title"].lower())
        assert bel_giorno["gross_eur"] == 316_412

    def test_cinemas_cloned_from_saturday(self):
        sunday = date(2026, 3, 15)
        result = compute_sunday_records(self._make_weekend(), self._make_thu_fri_sat(sunday), sunday)
        jumpers = next(r for r in result if "jumpers" in r["title"].lower())
        assert jumpers["cinemas"] == 562

    def test_sunday_date_set_correctly(self):
        sunday = date(2026, 3, 15)
        result = compute_sunday_records(self._make_weekend(), self._make_thu_fri_sat(sunday), sunday)
        assert all(r["date"] == "2026-03-15" for r in result)

    def test_source_is_cineguru(self):
        sunday = date(2026, 3, 15)
        result = compute_sunday_records(self._make_weekend(), self._make_thu_fri_sat(sunday), sunday)
        assert all(r["source"] == "CINEGURU" for r in result)

    def test_negative_gross_clamped_to_zero(self):
        sunday = date(2026, 3, 15)
        # weekend gross < feriale → deve clamparsi a 0
        weekend = [{"rank": 1, "title": "Film Test", "gross_eur": 100,
                    "admissions": None, "cinemas": None,
                    "avg_per_cinema_eur": None, "total_gross_eur": None, "source": "CINEGURU"}]
        thu_fri_sat = [{"title": "Film Test", "gross": 5000, "cinemas": 100,
                        "row_date": sunday - __import__("datetime").timedelta(days=1)}]
        result = compute_sunday_records(weekend, thu_fri_sat, sunday)
        assert result[0]["gross_eur"] == 0

    def test_missing_weekend_gross_skipped(self):
        sunday = date(2026, 3, 15)
        weekend = [{"rank": 1, "title": "Film Senza Gross", "gross_eur": None,
                    "admissions": None, "cinemas": None,
                    "avg_per_cinema_eur": None, "total_gross_eur": None, "source": "CINEGURU"}]
        result = compute_sunday_records(weekend, [], sunday)
        assert result == []


# ---------------------------------------------------------------------------
# parse_article con HTML reale fine-settimana
# ---------------------------------------------------------------------------

class TestParseWeekendArticle:
    """Verifica che parse_article gestisca correttamente il formato fine-settimana."""

    def test_returns_at_least_ten_records(self):
        """parse_article non filtra per rank; il filtro top-10 avviene nel chiamante."""
        records = parse_article(WEEKEND_HTML, date(2026, 3, 15))
        assert len(records) >= 10

    def test_first_record_is_jumpers(self):
        records = parse_article(WEEKEND_HTML, date(2026, 3, 15))
        assert records[0]["rank"] == 1
        assert "jumpers" in records[0]["title"].lower()

    def test_gross_eur_is_positive(self):
        records = parse_article(WEEKEND_HTML, date(2026, 3, 15))
        assert all(r["gross_eur"] > 0 for r in records if r.get("gross_eur"))

    def test_source_is_cineguru(self):
        records = parse_article(WEEKEND_HTML, date(2026, 3, 15))
        assert all(r["source"] == "CINEGURU" for r in records)

    def test_date_set_to_provided_date(self):
        records = parse_article(WEEKEND_HTML, date(2026, 3, 15))
        assert all(r["date"] == "2026-03-15" for r in records)
