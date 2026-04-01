"""
Tests per il nuovo autore 693ae5de70c3ea16 (Andrea Francesco Berni) in cineguru_scraper.

Verifica:
- Regex primaria + fallback sulle righe campione fornite nella strategia
- _detect_author_hash() con HTML mock di screenweek.it
- _get_parsing_patterns() restituisce la strategia corretta per l'hash
- parse_article() completo con HTML mock realistico
"""
import hashlib

import pytest
from bs4 import BeautifulSoup

from boxoffice_int.domain.box_office_raw.cineguru_scraper import (
    AUTHOR_STRATEGIES,
    RE_ENTRY_693AE5DE70C3EA16,
    RE_ENTRY_LOOSE_693AE5DE70C3EA16,
    _clean_number,
    _detect_author_hash,
    _get_parsing_patterns,
    _normalize_title_for_matching,
    _parse_entry_line,
    parse_article,
)
from datetime import date


# ---------------------------------------------------------------------------
# Righe campione (dalla strategia)
# ---------------------------------------------------------------------------

SAMPLE_LINES = [
    "1. L'ultima missione: Project Hail Mary – €127.955 – 288 cinema/€444 – Tot. €1.440.287",
    "2. Jumpers – Un salto tra gli animali – €56.670 – 264 cinema/€215 – Tot. €4.427.195",
    "6. Reminders of Him – La parte migliore di te – €23.870 – 202 cinema/€118 – Tot. €893.199",
    "8. Una battaglia dopo l'altra (One Battle After Another) – €21.115 – 189 cinema/€112 – Tot. €5.495.757",
]

EXPECTED = [
    {"rank": 1, "title": "L'ultima missione: Project Hail Mary", "gross": 127955, "cinemas": 288, "total": 1440287},
    {"rank": 2, "title": "Jumpers – Un salto tra gli animali",    "gross": 56670,  "cinemas": 264, "total": 4427195},
    {"rank": 6, "title": "Reminders of Him – La parte migliore di te", "gross": 23870, "cinemas": 202, "total": 893199},
    {"rank": 8, "title": "Una battaglia dopo l'altra (One Battle After Another)", "gross": 21115, "cinemas": 189, "total": 5495757},
]

AUTHOR_HASH = "693ae5de70c3ea16"


# ---------------------------------------------------------------------------
# Test regex primaria
# ---------------------------------------------------------------------------

class TestRegexPrimaria:
    @pytest.mark.parametrize("line,expected", list(zip(SAMPLE_LINES, EXPECTED)))
    def test_rank(self, line, expected):
        m = RE_ENTRY_693AE5DE70C3EA16.match(line)
        assert m is not None, f"Regex non ha fatto match su: {line!r}"
        assert int(m.group("rank")) == expected["rank"]

    @pytest.mark.parametrize("line,expected", list(zip(SAMPLE_LINES, EXPECTED)))
    def test_gross(self, line, expected):
        m = RE_ENTRY_693AE5DE70C3EA16.match(line)
        assert m is not None
        assert _clean_number(m.group("gross")) == expected["gross"]

    @pytest.mark.parametrize("line,expected", list(zip(SAMPLE_LINES, EXPECTED)))
    def test_cinemas(self, line, expected):
        m = RE_ENTRY_693AE5DE70C3EA16.match(line)
        assert m is not None
        assert _clean_number(m.group("cinemas")) == expected["cinemas"]

    @pytest.mark.parametrize("line,expected", list(zip(SAMPLE_LINES, EXPECTED)))
    def test_total(self, line, expected):
        m = RE_ENTRY_693AE5DE70C3EA16.match(line)
        assert m is not None
        assert _clean_number(m.group("total")) == expected["total"]

    @pytest.mark.parametrize("line,expected", list(zip(SAMPLE_LINES, EXPECTED)))
    def test_title_with_internal_dashes(self, line, expected):
        """Il titolo greedy (.+) deve catturare correttamente titoli con trattini interni."""
        m = RE_ENTRY_693AE5DE70C3EA16.match(line)
        assert m is not None
        assert m.group("title").strip() == expected["title"]


# ---------------------------------------------------------------------------
# Test regex loose (fallback)
# ---------------------------------------------------------------------------

class TestRegexLoose:
    @pytest.mark.parametrize("line,expected", list(zip(SAMPLE_LINES, EXPECTED)))
    def test_fallback_parses_rank_and_gross(self, line, expected):
        m = RE_ENTRY_LOOSE_693AE5DE70C3EA16.match(line)
        assert m is not None, f"Regex loose non ha fatto match su: {line!r}"
        assert int(m.group("rank")) == expected["rank"]
        assert _clean_number(m.group("gross")) == expected["gross"]


# ---------------------------------------------------------------------------
# Test _detect_author_hash
# ---------------------------------------------------------------------------

class TestDetectAuthorHash:
    def _html_with_author(self, author_name: str) -> str:
        return f"""
        <html><head>
          <meta name="author" content="{author_name}" />
        </head><body>
          <article>
            <span class="article-author">{author_name}</span>
            <div class="entry-content"><p>contenuto</p></div>
          </article>
        </body></html>
        """

    def test_detects_known_author_hash(self):
        html = self._html_with_author("Andrea Francesco Berni")
        soup = BeautifulSoup(html, "html.parser")
        result = _detect_author_hash(soup)
        assert result == AUTHOR_HASH

    def test_hash_matches_sha256_16(self):
        expected = hashlib.sha256("Andrea Francesco Berni".encode()).hexdigest()[:16]
        html = self._html_with_author("Andrea Francesco Berni")
        soup = BeautifulSoup(html, "html.parser")
        assert _detect_author_hash(soup) == expected

    def test_returns_none_when_no_author(self):
        html = "<html><body><article><p>testo</p></article></body></html>"
        soup = BeautifulSoup(html, "html.parser")
        assert _detect_author_hash(soup) is None

    def test_meta_fallback_used_when_no_span(self):
        """Cade sul meta tag WordPress se span.article-author non è presente."""
        html = """
        <html><head><meta name="author" content="Andrea Francesco Berni" /></head>
        <body><article><p>testo</p></article></body></html>
        """
        soup = BeautifulSoup(html, "html.parser")
        assert _detect_author_hash(soup) == AUTHOR_HASH

    def test_ca77d5532d884726_hash(self):
        html = self._html_with_author("Stefano Radice")
        soup = BeautifulSoup(html, "html.parser")
        expected = hashlib.sha256("Stefano Radice".encode()).hexdigest()[:16]
        assert _detect_author_hash(soup) == expected


# ---------------------------------------------------------------------------
# Test _get_parsing_patterns
# ---------------------------------------------------------------------------

class TestGetParsingPatterns:
    def test_known_hash_returns_author_strategy(self):
        strict, loose = _get_parsing_patterns(AUTHOR_HASH)
        assert strict is RE_ENTRY_693AE5DE70C3EA16
        assert loose is RE_ENTRY_LOOSE_693AE5DE70C3EA16

    def test_unknown_hash_returns_default(self):
        from boxoffice_int.domain.box_office_raw.cineguru_scraper import RE_ENTRY, RE_ENTRY_LOOSE
        strict, loose = _get_parsing_patterns("0000000000000000")
        assert strict is RE_ENTRY
        assert loose is RE_ENTRY_LOOSE

    def test_none_hash_returns_default(self):
        from boxoffice_int.domain.box_office_raw.cineguru_scraper import RE_ENTRY, RE_ENTRY_LOOSE
        strict, loose = _get_parsing_patterns(None)
        assert strict is RE_ENTRY
        assert loose is RE_ENTRY_LOOSE

    def test_all_registered_hashes_in_strategies(self):
        assert AUTHOR_HASH in AUTHOR_STRATEGIES
        assert "ca77d5532d884726" in AUTHOR_STRATEGIES


# ---------------------------------------------------------------------------
# Test parse_article con HTML mock completo
# ---------------------------------------------------------------------------

def _build_article_html(author: str, lines: list[str]) -> str:
    ranked_block = "\n".join(f"<p>{line}</p>" for line in lines)
    return f"""
    <html><head>
      <meta name="author" content="{author}" />
    </head><body>
      <article>
        <span class="article-author">{author}</span>
        <div class="entry-content">
          <p>Introduzione all'articolo.</p>
          {ranked_block}
        </div>
      </article>
    </body></html>
    """


class TestParseArticleNuovoAutore:
    def test_parse_returns_correct_number_of_records(self):
        html = _build_article_html("Andrea Francesco Berni", SAMPLE_LINES)
        records = parse_article(html, date(2026, 3, 26))
        assert len(records) == 4

    def test_parse_first_record_fields(self):
        html = _build_article_html("Andrea Francesco Berni", SAMPLE_LINES)
        records = parse_article(html, date(2026, 3, 26))
        r = records[0]
        assert r["rank"] == 1
        assert r["title"] == "L'ultima missione: Project Hail Mary"
        assert r["gross_eur"] == 127955
        assert r["cinemas"] == 288
        assert r["total_gross_eur"] == 1440287
        assert r["date"] == "2026-03-26"

    def test_parse_title_with_internal_dash(self):
        html = _build_article_html("Andrea Francesco Berni", SAMPLE_LINES)
        records = parse_article(html, date(2026, 3, 26))
        titles = {r["rank"]: r["title"] for r in records}
        assert titles[2] == "Jumpers – Un salto tra gli animali"
        assert titles[6] == "Reminders of Him – La parte migliore di te"

    def test_admissions_nullable(self):
        """Il formato di questo autore non include le presenze: devono essere None."""
        html = _build_article_html("Andrea Francesco Berni", SAMPLE_LINES)
        records = parse_article(html, date(2026, 3, 26))
        for r in records:
            assert r["admissions"] is None

    def test_avg_per_cinema_computed(self):
        html = _build_article_html("Andrea Francesco Berni", SAMPLE_LINES)
        records = parse_article(html, date(2026, 3, 26))
        r = records[0]
        if r["gross_eur"] and r["cinemas"]:
            assert r["avg_per_cinema_eur"] == r["gross_eur"] // r["cinemas"]


# ---------------------------------------------------------------------------
# _normalize_title_for_matching — double-space regression
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("a,b", [
    # En-dash in paragraph title vs colon in top-10 title
    ("L'ultima missione – Project Hail Mary", "L'ultima missione: Project Hail Mary"),
    # Em-dash vs colon
    ("Jumpers — Un salto tra gli animali", "Jumpers: Un salto tra gli animali"),
    # Hyphen-minus in paragraph vs plain in top-10
    ("Un film - Un sottotitolo", "Un film Un sottotitolo"),
])
def test_normalize_title_dash_variants_match(a, b):
    """Titles differing only in dash/colon style must normalize to the same string."""
    assert _normalize_title_for_matching(a) == _normalize_title_for_matching(b)
