"""
cinetel_scraper.py
==================
Scraper per i dati di box office giornaliero da Cinetel (cinetel.it).

La pagina utilizza una datatable Angular virtualizzata (ngx-datatable style).
Le righe sono identificate per posizione, non per attributi data-*.
La virtual scrolling richiede scroll incrementale per caricare tutte le righe.

Struttura output (DataFrame / CSV):
    date | rank | title | release_date | country | distribution |
    gross | attendance | gross_total | attendance_total

Architettura a 4 layer:
  1. Selettori (configurazione centralizzata)
  2. Estrazione righe (get_rows)
  3. Parsing riga (parse_row — funzione pura)
  4. Scroll manager (scroll_until_all_rows_loaded)
"""

import logging
import re
import time
from datetime import date
from pathlib import Path

import pandas as pd
from playwright.sync_api import TimeoutError as PlaywrightTimeout
from playwright.sync_api import sync_playwright

from ...common import DATA_RAW

LOG = logging.getLogger("box_office_raw.cinetel")

# ---------------------------------------------------------------------------
# Layer 1 – Selettori (configurazione centralizzata)
# ---------------------------------------------------------------------------

SELECTORS: dict[str, str] = {
    "table":       "div[role='table']",
    "header_cell": "datatable-header-cell",
    "body":        "datatable-body",
    "row":         "datatable-body-row",
    "cell":        "datatable-body-cell",
    "cell_label":  ".datatable-body-cell-label",
    "scroller":    "datatable-scroller",
}

# Mappatura colonne per posizione — mai usare numeri hardcoded nel codice di parsing
COLUMN_MAP: dict[str, int] = {
    "rank":              0,
    "title":             1,
    "release_date":      2,
    "country":           3,
    "distribution":      4,
    "gross":             5,
    "attendance":        6,
    "gross_total":       7,
    "attendance_total":  8,
}


# ---------------------------------------------------------------------------
# Normalizzazione dati
# ---------------------------------------------------------------------------

def parse_currency(value: str) -> int | None:
    """Converte '€ 1.582.721' o '1.582.721' in 1582721."""
    if not value:
        return None
    cleaned = value.strip().lstrip("€").strip()
    cleaned = re.sub(r"[\s.,]", "", cleaned)
    if not cleaned:
        return None
    try:
        return int(cleaned)
    except ValueError:
        return None


def parse_number(value: str) -> int | None:
    """Converte '195.734' in 195734."""
    if not value:
        return None
    cleaned = re.sub(r"[\s.,]", "", value.strip())
    if not cleaned:
        return None
    try:
        return int(cleaned)
    except ValueError:
        return None


def parse_date_it(value: str) -> str | None:
    """Converte 'DD/MM/YYYY' o 'DD-MM-YYYY' in 'YYYY-MM-DD'. Passa attraverso date già ISO."""
    if not value:
        return None
    match = re.search(r"(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})", value.strip())
    if match:
        day, month, year = match.groups()
        return f"{year}-{int(month):02d}-{int(day):02d}"
    return value.strip() or None


# ---------------------------------------------------------------------------
# Layer 2 – Estrazione righe
# ---------------------------------------------------------------------------

def get_rows(page) -> list:
    """Restituisce tutti gli elementi riga visibili nella datatable."""
    return page.query_selector_all(
        f"{SELECTORS['body']} {SELECTORS['row']}"
    )


# ---------------------------------------------------------------------------
# Validazione header dinamica (robustezza futura)
# ---------------------------------------------------------------------------

def _validate_headers(page) -> dict[str, int]:
    """
    Legge gli header dinamicamente e costruisce la mappatura colonna → indice.

    Confronta con lo schema atteso e logga i mismatch.
    Restituisce la mappatura effettiva (con fallback a COLUMN_MAP per i campi non trovati).
    """
    header_cells = page.query_selector_all(SELECTORS["header_cell"])
    if not header_cells:
        LOG.warning("Nessun header trovato — uso mappatura colonne predefinita")
        return COLUMN_MAP.copy()

    dynamic_map: dict[str, int] = {}
    for i, cell in enumerate(header_cells):
        label = cell.inner_text().strip().lower()
        if "pos" in label:
            dynamic_map["rank"] = i
        elif "titolo" in label:
            dynamic_map["title"] = i
        elif "prima" in label:
            dynamic_map["release_date"] = i
        elif "nazione" in label or "paese" in label:
            dynamic_map["country"] = i
        elif "distribu" in label:
            dynamic_map["distribution"] = i
        elif "incasso" in label and "aggiornat" in label:
            dynamic_map["gross_total"] = i
        elif "incasso" in label:
            dynamic_map["gross"] = i
        elif "presenze" in label and "aggiornat" in label:
            dynamic_map["attendance_total"] = i
        elif "presenze" in label:
            dynamic_map["attendance"] = i

    missing = [k for k in COLUMN_MAP if k not in dynamic_map]
    if missing:
        LOG.warning(
            "Mappatura colonne incompleta: campi mancanti %s — uso indici predefiniti",
            missing,
        )
        for k in missing:
            dynamic_map[k] = COLUMN_MAP[k]

    return dynamic_map


# ---------------------------------------------------------------------------
# Layer 3 – Parsing riga (funzione pura)
# ---------------------------------------------------------------------------

def parse_row(row_element, column_map: dict[str, int]) -> dict | None:
    """
    Parsifica una singola riga della datatable in un dict strutturato.

    Funzione pura: nessuna logica di scroll o attesa.
    Restituisce None se la riga ha celle insufficienti o il rank non è numerico.
    """
    cells = row_element.query_selector_all(SELECTORS["cell"])
    if len(cells) < len(COLUMN_MAP):
        return None

    def cell_text(field: str) -> str:
        idx = column_map.get(field)
        if idx is None or idx >= len(cells):
            return ""
        label_el = cells[idx].query_selector(SELECTORS["cell_label"])
        target = label_el if label_el else cells[idx]
        # Il titolo è in un <strong> interno alla cella (§3 della strategia)
        if field == "title":
            strong = target.query_selector("strong")
            if strong:
                return strong.inner_text().strip()
        return target.inner_text().strip()

    rank = parse_number(cell_text("rank"))
    if rank is None:
        return None  # Righe senza rank numerico (header duplicati, separatori, ecc.)

    return {
        "rank":             rank,
        "title":            cell_text("title"),
        "release_date":     parse_date_it(cell_text("release_date")),
        "country":          cell_text("country"),
        "distribution":     cell_text("distribution"),
        "gross":            parse_currency(cell_text("gross")),
        "attendance":       parse_number(cell_text("attendance")),
        "gross_total":      parse_currency(cell_text("gross_total")),
        "attendance_total": parse_number(cell_text("attendance_total")),
    }


# ---------------------------------------------------------------------------
# Layer 4 – Scroll manager (virtualizzazione)
# ---------------------------------------------------------------------------

def scroll_until_all_rows_loaded(page, max_iterations: int = 100) -> None:
    """
    Scrolla la datatable virtuale fino a caricare tutte le righe.

    Strategia:
    - Scroll incrementale sul datatable-scroller
    - Confronta il numero di righe prima/dopo ogni scroll
    - Termina quando il conteggio non aumenta per 2 iterazioni consecutive

    Il parametro max_iterations previene loop infiniti su pagine anomale.
    """
    prev_count = 0
    stable_iterations = 0

    for _ in range(max_iterations):
        current_count = len(get_rows(page))

        if current_count == prev_count:
            stable_iterations += 1
            if stable_iterations >= 2:
                LOG.debug("Scroll completato: %d righe caricate", current_count)
                return
        else:
            stable_iterations = 0

        prev_count = current_count

        page.evaluate(
            """
            const scroller = document.querySelector('datatable-scroller');
            if (scroller) { scroller.scrollTop += 500; }
            """
        )
        time.sleep(0.3)

    LOG.warning("Scroll terminato dopo %d iterazioni (limite massimo raggiunto)", max_iterations)


# ---------------------------------------------------------------------------
# Navigazione pagina
# ---------------------------------------------------------------------------

def _fetch_table_data(page, url: str) -> list[dict]:
    """Naviga su una pagina Cinetel ed estrae tutte le righe della datatable."""
    try:
        page.goto(url, wait_until="networkidle", timeout=60_000)
        page.wait_for_selector(SELECTORS["row"], timeout=30_000)
    except PlaywrightTimeout:
        LOG.warning("Timeout caricamento pagina Cinetel: %s", url)
        return []
    except Exception as exc:
        LOG.warning("Errore caricamento pagina Cinetel: %s", exc)
        return []

    # Validazione header dinamica
    column_map = _validate_headers(page)

    # Scroll fino al caricamento completo (virtual table)
    scroll_until_all_rows_loaded(page)

    rows = get_rows(page)
    LOG.info("  Righe trovate dopo scroll completo: %d", len(rows))

    records: list[dict] = []
    for row in rows:
        try:
            record = parse_row(row, column_map)
            if record is not None:
                records.append(record)
        except Exception as exc:
            LOG.debug("  Riga saltata (errore parsing): %s", exc)

    return records


# ---------------------------------------------------------------------------
# Funzione principale
# ---------------------------------------------------------------------------

def scrape_cinetel(
    target_date: date,
    url: str,
    output_path: Path | None = None,
) -> Path:
    """
    Scarica i dati di box office da una pagina Cinetel con datatable Angular.

    Args:
        target_date: Data di riferimento dei dati.
        url: URL completo della pagina Cinetel con la datatable.
        output_path: Percorso output opzionale (default: data/raw/box_office_raw/).

    Returns:
        Path al file CSV scritto.

    Raises:
        RuntimeError: Se non vengono estratti record.
    """
    LOG.info("Avvio scraping Cinetel | data: %s | url: %s", target_date, url)

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context(locale="it-IT")
        page = context.new_page()
        records = _fetch_table_data(page, url)
        browser.close()

    if not records:
        raise RuntimeError(
            f"Nessun record estratto da Cinetel per la data {target_date}"
        )

    LOG.info("Record grezzi raccolti: %d", len(records))

    for record in records:
        record["date"] = target_date.isoformat()

    dataframe = pd.DataFrame(records)
    dataframe = dataframe.drop_duplicates(subset=["rank", "title"])
    dataframe = dataframe.sort_values("rank").reset_index(drop=True)
    dataframe = dataframe[dataframe["rank"].between(1, 10)]

    output_dir = DATA_RAW / "box_office_raw"
    output_dir.mkdir(parents=True, exist_ok=True)
    if output_path is None:
        output_path = output_dir / f"cinetel_{target_date}_{target_date}.csv"

    dataframe.to_csv(output_path, index=False)
    LOG.info("Dataset scritto: %s (%d righe)", output_path, len(dataframe))
    return output_path
