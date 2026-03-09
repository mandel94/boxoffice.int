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
from ...contracts import ContractViolationError, load_contract, validate

BASE_URL = "https://cineguru.screenweek.it"
ARCHIVE_URL = f"{BASE_URL}/box-office-2/box-office/"

RE_ENTRY = re.compile(
    r"""
    ^\s*(?P<rank>\d{1,2})\s*[–\-]\s*
    (?P<title>[^–\-]+?)\s*[–\-]\s*
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
    (?P<title>[^–\-0-9][^–\-]*?)\s*[–\-]\s*
    (?P<gross>[\d.,]+)\s*euro
    .*?(?:(?P<admissions>[\d.,]+)\s*spettatori)?
    .*?(?:(?P<cinemas>[\d.,]+)\s*cinema)?
    .*?tot\.\s*(?P<total>[\d.,]+)
    """,
    re.VERBOSE | re.IGNORECASE,
)

LOG = logging.getLogger("box_office_raw")


def _clean_number(value: str | None) -> int | None:
    if value is None:
        return None
    return int(re.sub(r"[.,]", "", value.strip()))


def _parse_avg(line: str) -> int | None:
    match = re.search(r"cinema\s*/\s*([\d.,]+)\s*euro", line, re.IGNORECASE)
    return _clean_number(match.group(1)) if match else None


def _fetch_html(url: str, page) -> str:
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
    all_records: list[dict] = []
    links: list[tuple[str, date]] = []

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context(locale="it-IT")
        page = context.new_page()

        page_index = 1
        while True:
            url = ARCHIVE_URL if page_index == 1 else f"{ARCHIVE_URL}page/{page_index}/"
            archive_html = _fetch_html(url, page)
            if not archive_html:
                break

            page_links = _extract_article_links(archive_html)
            if not page_links:
                break

            in_range = [(href, d) for href, d in page_links if start <= d <= end]
            links.extend(in_range)

            max_date = max((d for _, d in page_links), default=None)
            if max_date and max_date < start:
                break

            page_index += 1
            time.sleep(delay)

        links = sorted(set(links), key=lambda x: (x[1], x[0]))
        for href, article_date in links:
            html = _fetch_html(href, page)
            if html:
                all_records.extend(parse_article(html, article_date))
            time.sleep(delay)

        browser.close()

    if not all_records:
        raise RuntimeError("Nessun record estratto da Cineguru nel range richiesto")

    dataframe = pd.DataFrame(all_records).drop_duplicates(subset=["date", "rank", "title"])
    dataframe = dataframe.sort_values(["date", "rank"]).reset_index(drop=True)

    dataframe = dataframe[(dataframe["rank"] >= 1) & (dataframe["rank"] <= 10)]
    dataframe = dataframe[dataframe["gross_eur"].fillna(0) >= 0]

    contract = load_contract("box-office-raw-daily")
    try:
        validate(dataframe, contract)
    except ContractViolationError:
        LOG.exception("Output contract violation — dataset written anyway for inspection")

    output_dir = DATA_RAW / "box_office_raw"
    output_dir.mkdir(parents=True, exist_ok=True)
    if output_path is None:
        output_path = output_dir / f"cineguru_{start}_{end}.csv"

    dataframe.to_csv(output_path, index=False)
    return output_path
