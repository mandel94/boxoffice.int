"""Script temporaneo di esplorazione della pagina Cinetel."""
import json
import logging
import time
from datetime import date

from playwright.sync_api import sync_playwright

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("explore_cinetel")

URL = "https://cinetel.it/homepage"

SELECTORS = {
    "table":       "div[role='table']",
    "header_cell": "datatable-header-cell",
    "body":        "datatable-body",
    "row":         "datatable-body-row",
    "cell":        "datatable-body-cell",
    "cell_label":  ".datatable-body-cell-label",
    "scroller":    "datatable-scroller",
}


def scroll_and_collect(page):
    prev = 0
    stable = 0
    for _ in range(80):
        rows = page.query_selector_all(f"{SELECTORS['body']} {SELECTORS['row']}")
        cur = len(rows)
        log.debug("scroll iter — righe visibili: %d", cur)
        if cur == prev:
            stable += 1
            if stable >= 2:
                break
        else:
            stable = 0
        prev = cur
        page.evaluate(
            "const s = document.querySelector('datatable-scroller'); if(s) s.scrollTop += 400;"
        )
        time.sleep(0.4)
    return page.query_selector_all(f"{SELECTORS['body']} {SELECTORS['row']}")


with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    ctx = browser.new_context(locale="it-IT")
    pg = ctx.new_page()

    log.info("Caricamento pagina: %s", URL)
    try:
        pg.goto(URL, wait_until="networkidle", timeout=60_000)
    except Exception as e:
        log.warning("Timeout/errore networkidle: %s — continuo con domcontentloaded", e)
        pg.goto(URL, wait_until="domcontentloaded", timeout=30_000)

    # Aspetta tabella o almeno il body
    try:
        pg.wait_for_selector(SELECTORS["row"], timeout=20_000)
        log.info("Datatable trovata!")
    except Exception:
        log.warning("datatable-body-row non trovata — dump HTML parziale")
        html = pg.content()
        print("=== PRIMI 3000 CHAR HTML ===")
        print(html[:3000])
        browser.close()
        exit(1)

    # Headers
    headers = pg.query_selector_all(SELECTORS["header_cell"])
    log.info("Header cells trovati: %d", len(headers))
    for i, h in enumerate(headers):
        print(f"  header[{i}] = {h.inner_text().strip()!r}")

    # Scroll completo
    rows = scroll_and_collect(pg)
    log.info("Righe totali dopo scroll: %d", len(rows))

    # Parsifica prime 15 righe
    records = []
    for i, row in enumerate(rows[:15]):
        cells = row.query_selector_all(SELECTORS["cell"])
        row_data = []
        for j, cell in enumerate(cells):
            label = cell.query_selector(SELECTORS["cell_label"])
            target = label if label else cell
            strong = target.query_selector("strong")
            text = strong.inner_text().strip() if strong else target.inner_text().strip()
            row_data.append(text)
        records.append(row_data)
        print(f"  riga[{i}] = {row_data}")

    browser.close()

print("\n=== RIEPILOGO ===")
print(f"Righe estratte: {len(records)}")
if records:
    print("Prima riga:", records[0])
