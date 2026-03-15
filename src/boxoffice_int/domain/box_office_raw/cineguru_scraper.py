import logging
import re
import time
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

LOG = logging.getLogger("box_office_raw")


def _clean_number(value: str | None) -> int | None:
    """Strip Italian thousand separators (dots/commas) and return an integer, or None."""
    if value is None:
        return None
    return int(re.sub(r"[.,]", "", value.strip()))


def _parse_avg(line: str) -> int | None:
    """Extract avg-per-cinema revenue from a pattern like '… cinema / 12.345 euro …'."""
    match = re.search(r"cinema\s*/\s*([\d.,]+)\s*euro", line, re.IGNORECASE)
    return _clean_number(match.group(1)) if match else None


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

    Tries the strict RE_ENTRY pattern first, then falls back to RE_ENTRY_LOOSE.
    Lines that match neither are silently skipped.
    """
    soup = BeautifulSoup(html, "html.parser")
    content = soup.find("div", class_=re.compile(r"entry-content|post-content|article-content", re.I))
    if not content:
        content = soup.find("article")
    if not content:
        return []

    records: list[dict] = []
    for line in content.get_text(separator="\n").splitlines():
        line = line.strip()
        if not line or not re.match(r"^\d{1,2}\s*[–\-]", line):
            continue

        match = RE_ENTRY.match(line) or RE_ENTRY_LOOSE.match(line)
        if not match:
            continue

        group = match.groupdict()
        records.append(
            {
                "date": article_date.isoformat(),
                "rank": int(group["rank"]),
                "title": group["title"].strip(),
                "gross_eur": _clean_number(group.get("gross")),
                "admissions": _clean_number(group.get("admissions")),
                "cinemas": _clean_number(group.get("cinemas")),
                "avg_per_cinema_eur": _parse_avg(line),
                "total_gross_eur": _clean_number(group.get("total")),
            }
        )
    return records


def _extract_article_links(html: str) -> list[tuple[str, date]]:
    """
    Scrape all box-office article links from an archive page.

    Parses date from URL slug (numeric day + optional Italian month name).
    Duplicate URLs are deduplicated; unparseable dates are skipped.
    """
    soup = BeautifulSoup(html, "html.parser")
    items: list[tuple[str, date]] = []
    month_it = {
        "gennaio": 1,
        "febbraio": 2,
        "marzo": 3,
        "aprile": 4,
        "maggio": 5,
        "giugno": 6,
        "luglio": 7,
        "agosto": 8,
        "settembre": 9,
        "ottobre": 10,
        "novembre": 11,
        "dicembre": 12,
    }

    for anchor in soup.find_all("a", href=True):
        href = anchor["href"]
        matched = re.search(r"/(\d{4})/(\d{2})/(.+box.office.+)/", href, re.I)
        if not matched:
            continue

        year, month = int(matched.group(1)), int(matched.group(2))
        slug = matched.group(3)
        day_match = re.search(r"(\d{1,2})[^\d]", slug)
        if not day_match:
            continue

        named_month = re.search(
            r"(gennaio|febbraio|marzo|aprile|maggio|giugno|luglio|agosto|settembre|ottobre|novembre|dicembre)",
            slug,
            re.I,
        )
        if named_month:
            month = month_it[named_month.group(1).lower()]

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
