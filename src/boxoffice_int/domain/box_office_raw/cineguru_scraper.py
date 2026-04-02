import hashlib
import logging
import re
import time
from difflib import SequenceMatcher
from datetime import date
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup
from playwright.sync_api import TimeoutError as PlaywrightTimeout
from playwright.sync_api import sync_playwright

from ...common import DATA_RAW
from ...contracts import ContractViolationError, cast_to_contract, load_contract, validate

BASE_URL = "https://cineguru.screenweek.it"
ARCHIVE_URL = f"{BASE_URL}/box-office-2/box-office/"

RE_ENTRY = re.compile(
    r"""
    ^\s*(?P<rank>\d{1,2})\s*[–\-]\s*
    (?P<title>.+?)\s*[–\-]\s*
    (?P<gross>[\d.,]+)\s*euro
    \s*\((?P<admissions>[\d.,]+)\s*spettatori\)
    \s*[–\-]\s*(?P<cinemas>[\d.,]+)\s*cinema
    .*?tot\.\s*(?P<total>[\d.,]+)
    """,
    re.VERBOSE | re.IGNORECASE,
)

RE_ENTRY_LOOSE = re.compile(
    r"""
    ^\s*(?P<rank>\d{1,2})\s*[–\-]\s*
    (?P<title>[^–\-0-9].*?)\s*[–\-]\s*
    (?P<gross>[\d.,]+)\s*euro
    (?:\s*\((?P<admissions>[\d.,]+)\s*spettatori\))?
    (?:.*?(?P<cinemas>[\d.,]+)\s*cinema)?
    .*?tot\.\s*(?P<total>[\d.,]+)
    """,
    re.VERBOSE | re.IGNORECASE,
)

# Author-specific regex patterns
RE_ENTRY_CA77D5532D884726 = re.compile(
    r"""
    ^\s*(?P<rank>\d{1,2})\s*[–\-]\s*
    (?P<title>.+?)\s*[–\-]\s*
    (?P<gross>[\d.,]+)\s*euro
    \s*\((?P<admissions>[\d.,]+)\s*spettatori\)
    \s*[–\-]\s*(?P<cinemas>[\d.,]+)\s*cinema
    .*?tot\.\s*(?P<total>[\d.,]+)
    """,
    re.VERBOSE | re.IGNORECASE,
)

# Author 693ae5de70c3ea16 (Andrea Francesco Berni)
# Format: "N. Titolo – €GROSS – N cinema/€AVG – Tot. €TOTAL"
# Example: "1. L'ultima missione: Project Hail Mary – €127.955 – 288 cinema/€444 – Tot. €1.440.287"
# Greedy title (.+) handles internal dashes via backtracking to last '– €'
RE_ENTRY_693AE5DE70C3EA16 = re.compile(
    r"""
    ^\s*(?P<rank>\d{1,2})\.\s+
    (?P<title>.+)\s*[–\-]\s*
    €\s*(?P<gross>[\d.,]+)\s*[–\-]\s*
    (?P<cinemas>[\d.,]+)\s*cinema/€\s*(?P<avg>[\d.,]+)
    (?:\s*[–\-]\s*Tot\.\s*€\s*(?P<total>[\d.,]+))?
    """,
    re.VERBOSE | re.IGNORECASE,
)

# Loose fallback for author 693ae5de70c3ea16 — more permissive
RE_ENTRY_LOOSE_693AE5DE70C3EA16 = re.compile(
    r"""
    ^\s*(?P<rank>\d{1,2})\.\s+
    (?P<title>.+?)\s*[–\-]\s*
    €\s*(?P<gross>[\d.,]+)
    (?:.*?(?P<cinemas>[\d.,]+)\s*cinema)?
    (?:.*?Tot\.\s*€\s*(?P<total>[\d.,]+))?
    """,
    re.VERBOSE | re.IGNORECASE,
)

# Registry: author SHA-256[:16] hash → (strict_pattern, loose_pattern)
# Hash computed as: hashlib.sha256(author_name.encode()).hexdigest()[:16]
AUTHOR_STRATEGIES: dict[str, tuple[re.Pattern, re.Pattern]] = {
    "ca77d5532d884726": (RE_ENTRY_CA77D5532D884726, RE_ENTRY_LOOSE),
    "693ae5de70c3ea16": (RE_ENTRY_693AE5DE70C3EA16, RE_ENTRY_LOOSE_693AE5DE70C3EA16),
}

LOG = logging.getLogger("box_office_raw")


def _clean_number(value: str | None) -> int | None:
    """Strip Italian thousand separators (dots/commas) and return an integer, or None."""
    if value is None:
        return None
    cleaned = re.sub(r"[.,]", "", value.strip())
    if not cleaned:
        return None
    try:
        return int(cleaned)
    except ValueError:
        return None


def _parse_avg(line: str) -> int | None:
    """Extract avg-per-cinema revenue from a pattern like '… cinema / 12.345 euro …'."""
    match = re.search(r"cinema\s*/\s*([\d.,]+)\s*euro", line, re.IGNORECASE)
    return _clean_number(match.group(1)) if match else None


def _extract_author(soup: BeautifulSoup) -> str | None:
    """Extract and normalize author name from span.article-author."""
    author_span = soup.find("span", class_="article-author")
    if not author_span:
        return None
    author = author_span.get_text(strip=True)
    return author.strip() if author else None


def _normalize_author(author: str | None) -> str | None:
    """Normalize author name for strategy matching."""
    if not author:
        return None
    return author.strip()


def _detect_author_hash(soup: BeautifulSoup) -> str | None:
    """
    Extract the author from article HTML and return SHA-256[:16] hash.

    Tries span.article-author first (Cineguru/Screenweek selector),
    then falls back to the WordPress <meta name="author"> tag.
    Returns None if no author can be identified.
    """
    author = _extract_author(soup)
    if not author:
        meta = soup.find("meta", {"name": "author"})
        if meta and meta.get("content"):
            author = meta["content"].strip()
    if not author:
        return None
    return hashlib.sha256(author.encode()).hexdigest()[:16]


def _get_parsing_patterns(author_hash: str | None) -> tuple[re.Pattern, re.Pattern]:
    """Return (strict, loose) regex patterns for the given author hash."""
    if author_hash and author_hash in AUTHOR_STRATEGIES:
        return AUTHOR_STRATEGIES[author_hash]
    # Default fallback for unknown or missing authors
    return RE_ENTRY, RE_ENTRY_LOOSE


def _parse_entry_line(line: str, patterns: tuple[re.Pattern, re.Pattern]) -> dict | None:
    """Parse a single entry line using given patterns."""
    strict_pattern, loose_pattern = patterns
    match = strict_pattern.match(line) or loose_pattern.match(line)
    if not match:
        return None
    return match.groupdict()


def _extract_title_from_paragraph(paragraph, author: str | None = None) -> tuple[str | None, bool]:
    """
    Extract movie title from paragraph using strong/em elements.

    Args:
        paragraph: BeautifulSoup paragraph element
        author: Normalized author name for author-specific extraction logic

    Returns:
        (title, has_warning): title extracted and whether multiple strong elements were found
    """
    strong_elements = paragraph.find_all("strong")

    # Default extraction logic
    if len(strong_elements) > 1:
        # Multiple strong elements - try to find one with em inside
        strong_with_em = [s for s in strong_elements if s.find("em")]
        if strong_with_em:
            title = strong_with_em[0].get_text(strip=True)
            return title, True  # Warning: multiple strong elements found
        else:
            # Take the first strong if no em found
            title = strong_elements[0].get_text(strip=True)
            return title, True  # Warning: multiple strong elements found
    elif len(strong_elements) == 1:
        title = strong_elements[0].get_text(strip=True)
        return title, False
    else:
        # Fallback to em elements
        em_elements = paragraph.find_all("em")
        if em_elements:
            title = em_elements[0].get_text(strip=True)
            return title, False
        return None, False


def _extract_fuzzy_numbers(text: str, author: str | None = None) -> dict[str, int | None]:
    """
    Extract numbers that precede 'euro' and cinema-related keywords.

    Args:
        text: Text to search in
        author: Normalized author name for author-specific extraction logic

    Returns dict with keys: 'euro', 'cinemas'
    """
    result = {"euro": None, "cinemas": None}

    # Default extraction logic
    # Extract number immediately before "euro" (case insensitive)
    euro_match = re.search(r"([\d.,]+)\s*euro", text, re.IGNORECASE)
    if euro_match:
        result["euro"] = _clean_number(euro_match.group(1))

    # Extract number immediately before cinema/sala/sale keywords (fuzzy matching)
    # Require at least one digit to avoid false matches like ", sale di un gradino".
    cinema_pattern = r"(\d[\d.,]*)\s*(?:cinema|sala|sale)\b"
    cinema_match = re.search(cinema_pattern, text, re.IGNORECASE)
    if cinema_match:
        result["cinemas"] = _clean_number(cinema_match.group(1))

    return result


def _normalize_title_for_matching(title: str) -> str:
    """Normalize title for matching between paragraphs and top 10."""
    if not title:
        return ""
    # Remove extra whitespace, convert to lowercase, remove common punctuation.
    # The second \s+ collapse is intentional: removing a dash surrounded by spaces
    # (e.g. "missione – Project") leaves a double space; collapsing again ensures
    # "missione  Project" == "missione Project" when the top-10 uses a colon instead.
    normalized = re.sub(r"\s+", " ", title.strip().lower())
    normalized = re.sub(r"[–\-—:;.,!?]", "", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def _match_paragraph_to_top10(paragraph_title: str, top10_records: list[dict]) -> dict | None:
    """Find matching record in top 10 based on title similarity."""
    if not paragraph_title:
        return None

    normalized_para_title = _normalize_title_for_matching(paragraph_title)
    best_record = None
    best_score = 0.0

    for record in top10_records:
        normalized_record_title = _normalize_title_for_matching(record["title"])

        # Exact match first
        if normalized_para_title == normalized_record_title:
            return record

        # Partial match - paragraph title contained in record title or vice versa
        if (normalized_para_title in normalized_record_title or
            normalized_record_title in normalized_para_title):
            return record

        # Fuzzy fallback for minor editorial title variations (e.g. "figlio" vs "cielo").
        similarity = SequenceMatcher(None, normalized_para_title, normalized_record_title).ratio()
        if similarity > best_score:
            best_score = similarity
            best_record = record

    if best_record and best_score >= 0.85:
        return best_record
    return None


def _parse_paragraphs(soup: BeautifulSoup, author: str | None = None) -> tuple[list[dict], list[str]]:
    """
    Parse paragraphs before top 10 to extract admissions and cinema data.

    Args:
        soup: BeautifulSoup object of the article
        author: Normalized author name for author-specific parsing logic

    Returns:
        (paragraph_data, warnings): list of paragraph records and warning messages
    """
    content = soup.find("div", class_=re.compile(r"entry-content|post-content|article-content", re.I))
    if not content:
        content = soup.find("article")
    if not content:
        return [], []

    paragraphs = content.find_all("p")
    paragraph_data = []
    warnings = []

    for i, paragraph in enumerate(paragraphs):
        # Skip paragraphs that are likely part of the top 10 (contain numbered list patterns)
        # Handles both "N – Title" and "N. Title" formats
        paragraph_text = paragraph.get_text(strip=True)
        if re.match(r"^\d{1,2}\s*[.\-–]", paragraph_text):
            break  # Stop when we reach the top 10 section

        # Extract title
        title, has_warning = _extract_title_from_paragraph(paragraph, author)
        if has_warning:
            warnings.append(f"Paragrafo {i+1}: multipli elementi strong trovati")

        if not title:
            continue  # Skip paragraphs without movie titles

        # Extract numbers
        numbers = _extract_fuzzy_numbers(paragraph_text, author)

        if numbers["euro"] or numbers["cinemas"]:
            paragraph_data.append({
                "title": title,
                "paragraph_gross_eur": numbers["euro"],
                "paragraph_cinemas": numbers["cinemas"],
                "paragraph_index": i
            })

    return paragraph_data, warnings


def _fetch_html(url: str, page) -> str:
    """Navigate *page* to *url* and return its rendered HTML. Returns '' on timeout or error."""
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        page.wait_for_selector("article", timeout=15_000)
        return page.content()
    except PlaywrightTimeout:
        LOG.warning("Timeout su %s", url)
        return ""
    except Exception as exc:
        LOG.warning("Errore su %s: %s", url, exc)
        return ""


def parse_article(html: str, article_date: date) -> list[dict]:
    """
    Parse a Cineguru box-office article and return one record per ranked entry.

    Extracts author from span.article-author, normalizes it, and uses
    author-specific parsing strategy if available. Falls back to default strategy
    for unknown authors.

    Enhanced parsing: extracts admissions and cinema data from article paragraphs
    before the top 10 list, then merges with ranking data from the numbered list.

    Tries the strict pattern first, then falls back to loose pattern.
    Lines that match neither are silently skipped.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Extract and normalize author
    author = _extract_author(soup)
    author = _normalize_author(author)
    if author:
        LOG.debug("  Articolo scritto da: %s", author)

    # Get parsing patterns based on author hash
    author_hash = _detect_author_hash(soup)
    if author_hash:
        LOG.debug("  Author hash: %s", author_hash)
    patterns = _get_parsing_patterns(author_hash)

    # Parse paragraphs for admissions and cinema data
    paragraph_data, warnings = _parse_paragraphs(soup, author)
    if warnings:
        for warning in warnings:
            LOG.warning("  %s", warning)
    if paragraph_data:
        LOG.debug("  Estratti %d paragrafi con dati numerici", len(paragraph_data))

    content = soup.find("div", class_=re.compile(r"entry-content|post-content|article-content", re.I))
    if not content:
        content = soup.find("article")
    if not content:
        return []

    # Parse top 10 ranking data
    # Supports both "N – Title" (default) and "N. Title" (author 693ae5de70c3ea16) formats
    top10_records: list[dict] = []
    for line in content.get_text(separator="\n").splitlines():
        line = line.strip()
        if not line or not re.match(r"^\d{1,2}\s*[.\-–]", line):
            continue

        group = _parse_entry_line(line, patterns)
        if not group:
            continue

        top10_records.append({
            "rank": int(group["rank"]),
            "title": group["title"].strip(),
            "gross_eur": _clean_number(group.get("gross")),
            "admissions": _clean_number(group.get("admissions")),
            "cinemas": _clean_number(group.get("cinemas")),
            "total_gross_eur": _clean_number(group.get("total")),
        })

    # Merge paragraph data with top 10 data
    records: list[dict] = []
    for record in top10_records:
        # Try to find matching paragraph data
        matched_paragraph = _match_paragraph_to_top10(record["title"], paragraph_data)

        if matched_paragraph:
            LOG.debug("  Matched '%s' con paragrafo %d", record["title"], matched_paragraph["paragraph_index"] + 1)

            # Use paragraph data for cinemas if available, keep top10 data for admissions/gross
            final_cinemas = matched_paragraph.get("paragraph_cinemas")
            final_admissions = record["admissions"]  # Keep from top 10
            final_gross = matched_paragraph.get("paragraph_gross_eur") or record["gross_eur"]

            # If paragraph cinema data is not available, fall back to top 10 data
            if final_cinemas is None:
                final_cinemas = record["cinemas"]
        else:
            # No paragraph match - use top 10 data as-is
            final_admissions = record["admissions"]
            final_cinemas = record["cinemas"]
            final_gross = record["gross_eur"]

        avg = (final_gross // final_cinemas) if (final_gross and final_cinemas) else None
        records.append({
            "date": article_date.isoformat(),
            "rank": record["rank"],
            "title": record["title"],
            "gross_eur": final_gross,
            "admissions": final_admissions,
            "cinemas": final_cinemas,
            "avg_per_cinema_eur": avg,
            "total_gross_eur": record["total_gross_eur"],
            "source": "CINEGURU",
        })

    return records


_MONTH_IT: dict[str, int] = {
    "gennaio": 1, "febbraio": 2, "marzo": 3, "aprile": 4,
    "maggio": 5, "giugno": 6, "luglio": 7, "agosto": 8,
    "settembre": 9, "ottobre": 10, "novembre": 11, "dicembre": 12,
}


def is_weekend_article(href: str) -> bool:
    """Return True if *href* refers to a Cineguru weekend summary article.

    Matches both slug variants used by Cineguru:
    - ``fine-settimana`` (historic format)
    - ``week-end``        (format observed from 2026-03-29 onward)
    """
    href_lower = href.lower()
    return "fine-settimana" in href_lower or "week-end" in href_lower


def _extract_sunday_date(href: str) -> date | None:
    """
    Extract the Sunday date from a weekend article URL.

    Weekend article slugs contain both the opening day (Thu) and closing day
    (Sun), e.g. "box-office-del-fine-settimana-12-15-marzo".  We extract the
    *last* numeric token before the named month as the Sunday date.
    """
    matched = re.search(r"/(\d{4})/(\d{2})/(.+)/", href)
    if not matched:
        return None
    year = int(matched.group(1))
    month = int(matched.group(2))
    slug = matched.group(3)

    # Override month with named Italian month in slug (e.g. "marzo")
    named_month = re.search(
        r"(gennaio|febbraio|marzo|aprile|maggio|giugno|luglio|agosto"
        r"|settembre|ottobre|novembre|dicembre)",
        slug, re.I,
    )
    if named_month:
        month = _MONTH_IT[named_month.group(1).lower()]

    # Find all 1-2 digit numbers in slug and take the last one (= Sunday)
    day_candidates = re.findall(r"(?<![\d])(\d{1,2})(?![\d])", slug)
    if not day_candidates:
        return None
    for candidate in reversed(day_candidates):
        try:
            return date(year, month, int(candidate))
        except ValueError:
            continue
    return None


def _extract_article_links(html: str) -> list[tuple[str, date]]:
    """
    Scrape all *daily* box-office article links from an archive page.

    Weekend aggregate articles (URLs containing "fine-settimana") are
    intentionally excluded: they aggregate Thu-Sun and are handled
    separately by the sunday_fallback module.

    Parses date from URL slug (numeric day + optional Italian month name).
    Duplicate URLs are deduplicated; unparseable dates are skipped.
    """
    soup = BeautifulSoup(html, "html.parser")
    items: list[tuple[str, date]] = []

    for anchor in soup.find_all("a", href=True):
        href = anchor["href"]
        # Skip weekend aggregate articles
        if is_weekend_article(href):
            continue

        matched = re.search(r"/(\d{4})/(\d{2})/(.+box.office.+)/", href, re.I)
        if not matched:
            continue

        year, month = int(matched.group(1)), int(matched.group(2))
        slug = matched.group(3)
        day_match = re.search(r"(\d{1,2})[^\d]", slug)
        if not day_match:
            continue

        named_month = re.search(
            r"(gennaio|febbraio|marzo|aprile|maggio|giugno|luglio|agosto"
            r"|settembre|ottobre|novembre|dicembre)",
            slug, re.I,
        )
        if named_month:
            month = _MONTH_IT[named_month.group(1).lower()]

        try:
            items.append((href, date(year, month, int(day_match.group(1)))))
        except ValueError:
            continue

    deduped: dict[str, tuple[str, date]] = {href: (href, d) for href, d in items}
    return list(deduped.values())


def scrape_cineguru(start: date, end: date, delay: float = 2.0, output_path: Path | None = None) -> Path:
    """
    Full ingest pipeline: scrape Cineguru, parse articles, validate, and write CSV.

    Paginates the archive until all articles in [start, end] are collected,
    then fetches and parses each article. Output is validated against the
    box-office-raw-daily contract before being written.

    Args:
        start: First date of the range (inclusive).
        end: Last date of the range (inclusive).
        delay: Seconds to wait between HTTP requests.
        output_path: Override the default output path under DATA_RAW.

    Returns:
        Path to the written CSV file.

    Raises:
        RuntimeError: If no records were extracted for the requested range.
    """
    all_records: list[dict] = []
    links: list[tuple[str, date]] = []
    seen_in_range_hrefs: set[str] = set()
    consecutive_pages_without_new_in_range = 0

    LOG.info("Avvio scraping Cineguru | range %s → %s | delay %.1fs", start, end, delay)

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context(locale="it-IT")
        page = context.new_page()
        LOG.info("Browser Chromium avviato (headless)")

        page_index = 1
        while True:
            url = ARCHIVE_URL if page_index == 1 else f"{ARCHIVE_URL}page/{page_index}/"
            LOG.info("Archivio — pagina %d: %s", page_index, url)
            archive_html = _fetch_html(url, page)
            if not archive_html:
                LOG.warning("Nessun HTML ricevuto per la pagina archivio %d — stop paginazione", page_index)
                break

            page_links = _extract_article_links(archive_html)
            if not page_links:
                LOG.info("Nessun link trovato nella pagina archivio %d — stop paginazione", page_index)
                break

            in_range = [(href, d) for href, d in page_links if start <= d <= end]
            LOG.info("  trovati %d link totali, %d nel range", len(page_links), len(in_range))
            links.extend(in_range)

            if not in_range:
                LOG.info("  nessun articolo nel range su questa pagina — stop paginazione")
                break

            new_in_range = [(href, d) for href, d in in_range if href not in seen_in_range_hrefs]
            for href, _ in new_in_range:
                seen_in_range_hrefs.add(href)

            if not new_in_range:
                consecutive_pages_without_new_in_range += 1
                LOG.info(
                    "  nessun nuovo link nel range (consecutivi=%d) — possibile duplicazione sidebar",
                    consecutive_pages_without_new_in_range,
                )
                if consecutive_pages_without_new_in_range >= 2:
                    LOG.info("  stop paginazione: solo duplicati nel range su pagine consecutive")
                    break
            else:
                consecutive_pages_without_new_in_range = 0

            max_date = max((d for _, d in page_links), default=None)
            if max_date and max_date < start:
                LOG.info("  max_date %s < start %s — stop paginazione", max_date, start)
                break

            page_index += 1
            time.sleep(delay)

        links = sorted(set(links), key=lambda x: (x[1], x[0]))
        LOG.info("Articoli da scrapare: %d", len(links))

        for idx, (href, article_date) in enumerate(links, start=1):
            LOG.info("[%d/%d] Articolo %s — %s", idx, len(links), article_date, href)
            html = _fetch_html(href, page)
            if html:
                records = parse_article(html, article_date)
                LOG.info("  → %d righe estratte", len(records))
                all_records.extend(records)
            else:
                LOG.warning("  → HTML vuoto, articolo saltato")
            time.sleep(delay)

        browser.close()
        LOG.info("Browser chiuso")

    if not all_records:
        raise RuntimeError("Nessun record estratto da Cineguru nel range richiesto")

    LOG.info("Record grezzi raccolti: %d — deduplicazione e validazione in corso", len(all_records))
    dataframe = pd.DataFrame(all_records).drop_duplicates(subset=["date", "rank", "title"])
    dataframe = dataframe.sort_values(["date", "rank"]).reset_index(drop=True)

    dataframe = dataframe[(dataframe["rank"] >= 1) & (dataframe["rank"] <= 10)]
    dataframe = dataframe[dataframe["gross_eur"].fillna(0) >= 0]

    contract = load_contract("box-office-raw-daily")
    dataframe = cast_to_contract(dataframe, contract)
    try:
        validate(dataframe, contract)
        LOG.info("Validazione contratto OK")
    except ContractViolationError:
        LOG.exception("Output contract violation — dataset written anyway for inspection")

    output_dir = DATA_RAW / "box_office_raw"
    output_dir.mkdir(parents=True, exist_ok=True)
    if output_path is None:
        output_path = output_dir / f"cineguru_{start}_{end}.csv"

    dataframe.to_csv(output_path, index=False)
    LOG.info("Dataset scritto: %s (%d righe)", output_path, len(dataframe))
    return output_path
