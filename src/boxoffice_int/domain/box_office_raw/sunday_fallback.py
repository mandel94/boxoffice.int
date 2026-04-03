"""
sunday_fallback.py
==================
Logica di fallback per la domenica quando i dati Cinetel non sono disponibili.

Flusso:
  1. Verifica se per la domenica target esistono record con source='CINETEL'
     in fact_box_office_daily.
  2. Se presenti  → nessuna azione (dati già pronti nel DB).
  3. Se assenti   → attiva il fallback Cineguru weekend:
       a. Naviga l'archivio Cineguru e trova l'articolo "fine-settimana"
          relativo alla settimana che include la domenica target.
       b. Parsa l'articolo per ottenere i totali Thu-Sun per film.
       c. Interroga il DB (fact_box_office_daily) per i valori di
          giovedì, venerdì e sabato della stessa settimana.
       d. Calcola: domenica = totale_weekend – (giovedì + venerdì + sabato).
       e. Clona il numero di cinema dalla domenica prendendo il valore
          del sabato in DB.
       f. Produce un CSV con source='CINEGURU' pronto per il loader.

Il modulo espone SOLO funzioni pure e il singolo entry-point
``scrape_sunday_fallback``, che gestisce Playwright e I/O.

Funzioni pure (testabili senza DB né rete):
  - ``is_sunday_cinetel_present(sunday_date, conn)``
  - ``compute_sunday_records(weekend_records, thu_fri_sat_rows, sunday_date)``
  - ``_find_weekend_url_in_links(links, sunday_date)``
"""

from __future__ import annotations

import logging
import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from playwright.sync_api import sync_playwright

from ...common import DATA_RAW
from ...contracts import ContractViolationError, cast_to_contract, load_contract, validate
from .cineguru_scraper import (
    ARCHIVE_URL,
    _MONTH_IT,
    _fetch_html,
    is_weekend_article,
    _extract_sunday_date,
    parse_article,
)

LOG = logging.getLogger("box_office_raw.sunday_fallback")

# Giorni della settimana cinema (indice ISO: 1=Lun … 7=Dom)
_ISO_THURSDAY = 4
_ISO_SATURDAY = 6
_ISO_SUNDAY   = 7


# ---------------------------------------------------------------------------
# Helpers puri
# ---------------------------------------------------------------------------

def _prev_thursday(sunday: date) -> date:
    """Restituisce il giovedì che apre la settimana cinema di *sunday*."""
    # domenica ha weekday() 6 (Python), giovedì 3  → distance = 3
    days_back = (sunday.weekday() - 3) % 7  # 0 se già giovedì
    return sunday - timedelta(days=days_back)


def _find_weekend_url_in_links(
    links: list[tuple[str, date]],
    sunday_date: date,
) -> str | None:
    """
    Cerca tra i *links* dell'archivio (lista di (href, date)) l'URL
    dell'articolo fine-settimana la cui domenica corrisponde a *sunday_date*.

    Restituisce l'href oppure None se non trovato.
    """
    for href, _ in links:
        if not is_weekend_article(href):
            continue
        extracted = _extract_sunday_date(href)
        if extracted == sunday_date:
            return href
    return None


def compute_sunday_records(
    weekend_records: list[dict],
    thu_fri_sat_rows: list[dict],
    sunday_date: date,
) -> list[dict]:
    """
    Calcola i record della domenica sottraendo i valori feriali dal totale weekend.

    Parameters
    ----------
    weekend_records:
        Record parsati dall'articolo fine-settimana (totale Thu-Sun per film).
        Ogni record deve avere almeno: rank, title, gross_eur.
    thu_fri_sat_rows:
        Righe da fact_box_office_daily per giovedì+venerdì+sabato.
        Ogni riga deve avere: title (title_cineguru), gross, cinemas (può essere None).
    sunday_date:
        La data della domenica da produrre.

    Returns
    -------
    list[dict]
        Record pronti per essere scritti nel CSV raw (stessa struttura dei
        record giornalieri, con source='CINEGURU').

    Note
    ----
    - Se gross_eur del weekend è None o il titolo non ha dati feriali,
      il record domenica viene saltato con un warning.
    - I cinema sono clonati dal sabato. Se il sabato non è disponibile
      per quel film, il valore è None.
    - avg_per_cinema_eur è ricalcolato.
    """
    # Indice veloce: title_lower → somma gross feriale + cinemas sabato
    thu_fri_sat_gross: dict[str, int] = {}
    sat_cinemas: dict[str, int | None] = {}
    sat_date_key = sunday_date - timedelta(days=1)  # sabato

    for row in thu_fri_sat_rows:
        title_lower = row["title"].lower()
        gross = row.get("gross") or 0
        thu_fri_sat_gross[title_lower] = thu_fri_sat_gross.get(title_lower, 0) + gross
        # Tieni i cinema del sabato (date_key del sabato)
        if row.get("row_date") == sat_date_key:
            sat_cinemas[title_lower] = row.get("cinemas")

    sunday_records: list[dict] = []
    for rec in weekend_records:
        if not isinstance(rec.get("rank"), int):
            continue
        title = rec["title"]
        title_lower = title.lower()
        weekend_gross = rec.get("gross_eur")

        if weekend_gross is None:
            LOG.warning("Record weekend senza gross_eur: '%s' — saltato", title)
            continue

        feriale_gross = thu_fri_sat_gross.get(title_lower, 0)
        sunday_gross = weekend_gross - feriale_gross
        if sunday_gross < 0:
            LOG.warning(
                "Sunday gross negativo per '%s': weekend=%d feriale=%d → forzo a 0",
                title, weekend_gross, feriale_gross,
            )
            sunday_gross = 0

        cinemas = sat_cinemas.get(title_lower)
        avg = (sunday_gross // cinemas) if (sunday_gross and cinemas) else None

        sunday_records.append({
            "date":               sunday_date.isoformat(),
            "rank":               rec["rank"],
            "title":              title,
            "gross_eur":          sunday_gross,
            "admissions":         None,   # non disponibile nel calcolo diff
            "cinemas":            cinemas,
            "avg_per_cinema_eur": avg,
            "total_gross_eur":    rec.get("total_gross_eur"),
            "source":             "CINEGURU",
        })

    return sunday_records


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def is_sunday_cinetel_present(sunday_date: date, conn) -> bool:
    """
    Restituisce True se esistono record domenica con source='CINETEL' nel DB.

    Cerca in fact_box_office_daily tramite JOIN su dim_source (name='Cinetel')
    e dim_date (full_date=*sunday_date*).
    """
    date_key = sunday_date.year * 10000 + sunday_date.month * 100 + sunday_date.day
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)
            FROM   fact_box_office_daily f
            JOIN   dim_source s ON s.source_key = f.source_key
            WHERE  f.date_key = %s
              AND  s.name     = 'Cinetel'
            """,
            (date_key,),
        )
        count: int = cur.fetchone()[0]
    return count > 0


def _fetch_thu_fri_sat_rows(sunday_date: date, conn) -> list[dict]:
    """
    Recupera da fact_box_office_daily i valori di giovedì, venerdì e sabato
    per la settimana che si chiude con *sunday_date*.

    Restituisce una lista di dict: {title, gross, cinemas, row_date}.
    Usa CINETEL come fonte preferita; se assente, usa CINEGURU.
    """
    thu = _prev_thursday(sunday_date)
    sat = sunday_date - timedelta(days=1)
    fri = thu + timedelta(days=1)

    date_keys = [
        thu.year * 10000 + thu.month * 100 + thu.day,
        fri.year * 10000 + fri.month * 100 + fri.day,
        sat.year * 10000 + sat.month * 100 + sat.day,
    ]
    dates_map = {
        thu.year * 10000 + thu.month * 100 + thu.day:           thu,
        fri.year * 10000 + fri.month * 100 + fri.day: fri,
        sat.year * 10000 + sat.month * 100 + sat.day:           sat,
    }

    LOG.debug(
        "Recupero dati feriali per settimana che chiude con %s: "
        "giovedì=%s, venerdì=%s, sabato=%s (date_keys=%s)",
        sunday_date, thu, fri, sat, date_keys,
    )

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                df.title_cineguru      AS title,
                f.gross_eur            AS gross,
                f.cinemas              AS cinemas,
                f.date_key             AS date_key,
                s.name                 AS source_name
            FROM   fact_box_office_daily f
            JOIN   dim_film   df ON df.film_key   = f.film_key
            JOIN   dim_source s  ON s.source_key  = f.source_key
            WHERE  f.date_key = ANY(%s)
            ORDER BY f.date_key, s.name DESC   -- CINETEL prima di CINEGURU (DESC)
            """,
            (date_keys,),
        )
        raw_rows = cur.fetchall()

    LOG.debug("Query recuperate %d righe totali dal DB", len(raw_rows))
    if raw_rows:
        for i, (title, gross, cinemas, dk, source_name) in enumerate(raw_rows[:5], 1):
            LOG.debug(
                "  [%d] %s | date_key=%s | source=%s | gross_eur=%s | cinemas=%s",
                i, title, dk, source_name, gross, cinemas,
            )
        if len(raw_rows) > 5:
            LOG.debug("  ... e altri %d record", len(raw_rows) - 5)

    # Deduplica: per ogni (date_key, title) preferisce CINETEL
    seen: dict[tuple[int, str], dict] = {}
    for title, gross, cinemas, dk, source_name in raw_rows:
        key = (dk, title.lower())
        if key not in seen:
            LOG.debug(
                "  NUOVO: %s (date_key=%s) da %s | gross=%s | cinemas=%s",
                title, dk, source_name, gross, cinemas,
            )
            seen[key] = {
                "title":    title,
                "gross":    gross,
                "cinemas":  cinemas,
                "row_date": dates_map.get(dk),
            }
        elif source_name == "Cinetel":
            old_source = "unknown"
            # Ricostruisci la source precedente (difficile without extra tracking, skip per ora)
            LOG.debug(
                "  AGGIORNA: %s (date_key=%s) da %s (migliore) | gross=%s | cinemas=%s",
                title, dk, source_name, gross, cinemas,
            )
            seen[key] = {
                "title":    title,
                "gross":    gross,
                "cinemas":  cinemas,
                "row_date": dates_map.get(dk),
            }
        else:
            LOG.debug(
                "  SCARTA: %s (date_key=%s) da %s (già presente Cinetel) | gross=%s",
                title, dk, source_name, gross,
            )

    result = list(seen.values())
    LOG.info(
        "Fetch thu/fri/sat completato: %d record unici dopo deduplicazione (da %d totali)",
        len(result), len(raw_rows),
    )
    if result:
        by_date: dict[date | None, list[str]] = {}
        by_date_stats: dict[date | None, dict[str, int]] = {}
        for rec in result:
            d = rec.get("row_date")
            if d not in by_date:
                by_date[d] = []
                by_date_stats[d] = {"total_gross": 0, "count": 0, "with_cinemas": 0}
            by_date[d].append(rec["title"])
            by_date_stats[d]["count"] += 1
            by_date_stats[d]["total_gross"] += rec.get("gross") or 0
            if rec.get("cinemas"):
                by_date_stats[d]["with_cinemas"] += 1
        
        for d in sorted(by_date.keys(), reverse=True):
            stats = by_date_stats[d]
            titles_sample = ", ".join(by_date[d][:3]) + ("..." if len(by_date[d]) > 3 else "")
            LOG.info(
                "  %s: %d film | gross_tot=€%d | cinema_count=%d/%d | film: %s",
                d, 
                stats["count"],
                stats["total_gross"],
                stats["with_cinemas"],
                stats["count"],
                titles_sample,
            )
            # Mostra dettagli per i primi film di cada data
            for i, rec in enumerate([r for r in result if r.get("row_date") == d][:3], 1):
                LOG.debug(
                    "    [%d] %s | gross=€%s | cinemas=%s",
                    i, rec["title"], rec.get("gross"), rec.get("cinemas"),
                )

    return result


# ---------------------------------------------------------------------------
# Archive scraping per trovare l'URL del fine-settimana
# ---------------------------------------------------------------------------

def _find_weekend_article_url(sunday_date: date, page, delay: float) -> str | None:
    """
    Naviga l'archivio Cineguru finché non trova l'articolo fine-settimana
    la cui domenica è *sunday_date*.

    Restituisce l'URL oppure None se non trovato entro 5 pagine archivio.
    """
    from bs4 import BeautifulSoup

    for page_index in range(1, 6):
        url = ARCHIVE_URL if page_index == 1 else f"{ARCHIVE_URL}page/{page_index}/"
        LOG.info("Archivio weekend — pagina %d: %s", page_index, url)
        archive_html = _fetch_html(url, page)
        if not archive_html:
            break

        soup = BeautifulSoup(archive_html, "html.parser")
        for anchor in soup.find_all("a", href=True):
            href = anchor["href"]
            if not is_weekend_article(href):
                continue
            extracted = _extract_sunday_date(href)
            if extracted == sunday_date:
                LOG.info("  Trovato articolo fine-settimana: %s", href)
                return href

        # Se la pagina archivio ha articoli più vecchi del giovedì → stop
        import re
        all_dates = []
        for a in soup.find_all("a", href=True):
            m = re.search(r"/(\d{4})/(\d{2})/", a["href"])
            if m:
                try:
                    all_dates.append(date(int(m.group(1)), int(m.group(2)), 1))
                except ValueError:
                    pass
        thu = _prev_thursday(sunday_date)
        if all_dates and max(all_dates) < thu.replace(day=1):
            break
        time.sleep(delay)

    LOG.warning("Articolo fine-settimana non trovato per domenica %s", sunday_date)
    return None


# ---------------------------------------------------------------------------
# Entry point pubblico
# ---------------------------------------------------------------------------

def scrape_sunday_fallback(
    sunday_date: date,
    conn,
    delay: float = 2.0,
    output_path: Path | None = None,
) -> Path | None:
    """
    Calcola e salva il record domenica usando il fallback Cineguru weekend.

    Verifica prima la presenza di dati Cinetel per la domenica nel DB.
    Se presenti, non fa nulla e restituisce None.
    Se assenti, scarica l'articolo fine-settimana, calcola la domenica per
    differenza, e scrive il CSV (source='CINEGURU').

    Parameters
    ----------
    sunday_date:
        La domenica da calcolare (weekday deve essere domenica, altrimenti
        viene sollevato ValueError).
    conn:
        Connessione psycopg2 al DB (già aperta, NON chiusa qui).
    delay:
        Secondi di attesa tra richieste HTTP al sito Cineguru.
    output_path:
        Percorso di output opzionale; di default viene creato sotto DATA_RAW.

    Returns
    -------
    Path | None
        Path al CSV scritto oppure None se i dati Cinetel erano già presenti.

    Raises
    ------
    ValueError
        Se *sunday_date* non è una domenica.
    RuntimeError
        Se l'articolo fine-settimana non viene trovato o il parsing fallisce.
    """
    if sunday_date.isoweekday() != 7:
        raise ValueError(
            f"sunday_date deve essere una domenica, ricevuto {sunday_date} "
            f"({sunday_date.strftime('%A')})"
        )

    LOG.info("Sunday fallback — verifica Cinetel per %s", sunday_date)

    if is_sunday_cinetel_present(sunday_date, conn):
        LOG.info("  Dati Cinetel già presenti per domenica %s — operazione saltata", sunday_date)
        return None

    LOG.info("  Dati Cinetel assenti — attivazione fallback Cineguru weekend")

    # Recupera Thu+Fri+Sat dal DB
    thu_fri_sat_rows = _fetch_thu_fri_sat_rows(sunday_date, conn)
    LOG.info("  Righe feriali recuperate dal DB: %d", len(thu_fri_sat_rows))

    # Preflight: verifica che i 3 giorni feriali siano tutti presenti nel DB.
    # Senza di essi il calcolo per differenza produrrebbe valori errati
    # (il gross domenica risulterebbe uguale al totale weekend).
    thu = _prev_thursday(sunday_date)
    fri = thu + timedelta(days=1)
    sat = sunday_date - timedelta(days=1)
    dates_with_data = {rec["row_date"] for rec in thu_fri_sat_rows}
    missing = [d for d in (thu, fri, sat) if d not in dates_with_data]
    if missing:
        missing_str = ", ".join(str(d) for d in missing)
        raise RuntimeError(
            f"Impossibile calcolare la domenica {sunday_date}: dati mancanti nel DB per "
            f"{missing_str}. Esegui prima:\n"
            + "\n".join(f"  boxoffice-int ingest-date --date {d}" for d in missing)
            + "\n  boxoffice-int load --input <csv_path>"
        )

    # Scarica e parsa l'articolo fine-settimana
    weekend_records: list[dict] = []
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context(locale="it-IT")
        page = context.new_page()

        weekend_url = _find_weekend_article_url(sunday_date, page, delay)
        if not weekend_url:
            browser.close()
            raise RuntimeError(
                f"Articolo fine-settimana non trovato per domenica {sunday_date}"
            )

        LOG.info("  Parsing articolo fine-settimana: %s", weekend_url)
        html = _fetch_html(weekend_url, page)
        browser.close()

    if not html:
        raise RuntimeError(
            f"HTML vuoto per l'articolo fine-settimana {weekend_url}"
        )

    # parse_article usa sunday_date come date: per il fine-settimana
    # il dato rappresenta il totale Thu-Sun, non una data specifica.
    # Usiamo sunday_date come convenzione (= ultima data del range).
    weekend_records = parse_article(html, sunday_date)
    LOG.info("  Record weekend parsati: %d", len(weekend_records))

    if not weekend_records:
        raise RuntimeError(
            f"Nessun record estratto dall'articolo fine-settimana {weekend_url}"
        )

    # Calcola domenica per differenza
    sunday_records = compute_sunday_records(weekend_records, thu_fri_sat_rows, sunday_date)
    LOG.info("  Record domenica calcolati: %d", len(sunday_records))

    if not sunday_records:
        raise RuntimeError(
            f"Nessun record domenica calcolato per {sunday_date}: "
            "verificare che Thu/Fri/Sat siano presenti nel DB"
        )

    # Valida e scrivi CSV
    df = pd.DataFrame(sunday_records)
    df = df.sort_values("rank").reset_index(drop=True)

    # Gli articoli fine-settimana possono includere film oltre la top 10.
    # Manteniamo solo rank 1-10 per rispettare il contratto e il DB.
    out_of_top10 = df[df["rank"] > 10]
    if not out_of_top10.empty:
        LOG.info(
            "  Rimossi %d record con rank > 10: %s",
            len(out_of_top10),
            out_of_top10["title"].tolist(),
        )
    df = df[df["rank"] <= 10].reset_index(drop=True)

    contract = load_contract("box-office-raw-daily")
    df = cast_to_contract(df, contract)
    try:
        validate(df, contract)
        LOG.info("Validazione contratto OK")
    except ContractViolationError:
        LOG.exception("Output contract violation — dataset scritto per ispezione")

    output_dir = DATA_RAW / "box_office_raw"
    output_dir.mkdir(parents=True, exist_ok=True)
    if output_path is None:
        output_path = output_dir / f"cineguru_sunday_{sunday_date}.csv"

    df.to_csv(output_path, index=False)
    LOG.info("Dataset domenica scritto: %s (%d righe)", output_path, len(df))
    return output_path
