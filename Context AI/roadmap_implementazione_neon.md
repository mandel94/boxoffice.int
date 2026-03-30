# Neon PostgreSQL — Implementation Roadmap
> `boxoffice.it` · data mesh · box office italiano giornaliero

---

## Fase 1 — Setup Neon (Giorno 1)

### 1.1 Creazione account e progetto
- Registrarsi su [neon.tech](https://neon.tech) con account GitHub
- Creare un nuovo progetto: `boxoffice-it`
- Selezionare region: `eu-central-1` (Frankfurt) — latenza minima dall'Italia
- Annotare la **connection string** nel formato:
  ```
  postgresql://user:password@ep-xxxx.eu-central-1.aws.neon.tech/neondb?sslmode=require
  ```

### 1.2 Branching strategy
Neon supporta il branching del database (come Git per i dati). Struttura consigliata:

| Branch | Scopo |
|--------|-------|
| `main` | Produzione — dati reali, pipeline giornaliera |
| `dev` | Sviluppo — test di modifiche allo schema |
| `staging` | Validazione prima del merge su main |

---

## Fase 2 — Schema deployment (Giorno 1-2)

### 2.1 Esecuzione DDL
Applicare lo star schema sul branch `dev` prima di promuoverlo su `main`:

```bash
# Connessione al branch dev
psql $NEON_DEV_URL -f schema/star_schema.ddl

# Verifica tabelle create
psql $NEON_DEV_URL -c "\dt"
```

### 2.2 Popolamento dim_date
La tabella `dim_date` va pre-popolata per il range 2015–2035:

```bash
psql $NEON_DEV_URL -f schema/seed_dim_date.sql
```

### 2.3 Seed dati di riferimento
```bash
# Distributori noti (Warner, Universal, 01 Distribution, ecc.)
psql $NEON_DEV_URL -f schema/seed_dim_distributor.sql

# Generi (da TMDB)
psql $NEON_DEV_URL -f schema/seed_dim_genre.sql
```

### 2.4 Promozione su main
Verificato lo schema su `dev`, promuovere su `main`:
```bash
# Via Neon console o CLI
neon branches merge dev --into main
```

---

## Fase 3 — GitHub Secrets (Giorno 2)

Salvare le credenziali Neon come secrets nel repository GitHub.
Navigare su `Settings → Secrets and variables → Actions` e aggiungere:

| Secret | Valore |
|--------|--------|
| `NEON_DATABASE_URL` | Connection string completa di produzione |
| `NEON_DEV_DATABASE_URL` | Connection string branch dev |

> **Non committare mai la connection string nel codice.** Usare sempre i secrets.

---

## Fase 4 — Pipeline di ingestione (Giorno 3-5)

### 4.1 Loader script
Creare `pipeline/loader.py` che:
1. Legge il CSV prodotto dallo scraper Cineguru
2. Risolve le chiavi delle dimensioni (`film_key`, `date_key`, `distributor_key`)
3. Esegue l'upsert sulla fact table con `ON CONFLICT DO NOTHING`

Struttura minima:
```python
import psycopg2
import psycopg2.extras
import os

conn = psycopg2.connect(os.environ["NEON_DATABASE_URL"])

sql = """
    INSERT INTO fact_box_office_daily
        (date_key, film_key, source_key, rank, gross_eur,
         admissions, cinemas, avg_per_cinema_eur, total_gross_eur)
    VALUES %s
    ON CONFLICT (date_key, film_key, source_key) DO NOTHING
"""

with conn.cursor() as cur:
    psycopg2.extras.execute_values(cur, sql, rows, page_size=500)
    inserted = cur.rowcount

conn.commit()
```

### 4.2 Dimensione film — matching logic
Il titolo scraped da Cineguru non sempre coincide esattamente con TMDB.
Implementare una lookup a tre livelli:

1. **Match esatto** su `title_cineguru`
2. **Match fuzzy** (distanza di Levenshtein) con soglia 0.85
3. **Inserimento nuovo film** se nessun match trovato (da arricchire con TMDB in seguito)

---

## Fase 5 — GitHub Actions (Giorno 5-7)

### 5.1 Workflow giornaliero
Creare `.github/workflows/daily_pipeline.yml`:

```yaml
name: Daily Box Office Pipeline

on:
  schedule:
    - cron: '0 9 * * 2'  # ogni martedì alle 09:00 UTC
                          # Cineguru pubblica il lunedì sera
  workflow_dispatch:       # trigger manuale per backfill

jobs:
  scrape-and-load:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.12'

      - name: Install dependencies
        run: |
          pip install playwright beautifulsoup4 pandas psycopg2-binary
          playwright install chromium

      - name: Run scraper
        run: python pipeline/scraper.py --last-days 7

      - name: Load to Neon
        env:
          NEON_DATABASE_URL: ${{ secrets.NEON_DATABASE_URL }}
        run: python pipeline/loader.py

      - name: Upload artifact (CSV backup)
        uses: actions/upload-artifact@v4
        with:
          name: box-office-${{ github.run_id }}
          path: data/raw/*.csv
          retention-days: 90
```

### 5.2 Workflow backfill (one-shot)
Creare `.github/workflows/backfill.yml` con input manuale per range di date:

```yaml
on:
  workflow_dispatch:
    inputs:
      start_date:
        description: 'Data inizio (YYYY-MM-DD)'
        required: true
      end_date:
        description: 'Data fine (YYYY-MM-DD)'
        required: true
```

---

## Fase 6 — Enrichment TMDB (Settimana 2)

### 6.1 API key
Registrarsi su [themoviedb.org](https://www.themoviedb.org) e ottenere una API key gratuita.
Aggiungerla come secret GitHub: `TMDB_API_KEY`.

### 6.2 Enrichment script
Creare `pipeline/enrich_tmdb.py` che:
- Legge i film in `dim_film` con `tmdb_id IS NULL`
- Cerca il titolo su TMDB API (`/search/movie`)
- Aggiorna `dim_film` con: `tmdb_id`, `title_orig`, `director`, `runtime_min`, `release_date`
- Popola `bridge_film_genre` con i generi TMDB

### 6.3 Scheduling
Aggiungere un secondo step al workflow giornaliero, eseguito dopo il loader.

---

## Fase 7 — Monitoraggio (Settimana 2-3)

### 7.1 Alert su pipeline failure
GitHub Actions invia già notifiche email in caso di workflow fallito.
Aggiungere uno step di notifica esplicita su Slack o email con il report di ingestione:
```
✅ Pipeline 2026-03-21: 10 film inseriti, 0 errori
```

### 7.2 Query di controllo qualità
Creare `sql/qa_checks.sql` con query periodiche per rilevare anomalie:

```sql
-- Giorni mancanti negli ultimi 30 giorni
SELECT full_date
FROM dim_date
WHERE full_date BETWEEN CURRENT_DATE - 30 AND CURRENT_DATE
  AND day_of_week NOT IN (1,2,3,4,5,6,7) -- placeholder
  AND date_key NOT IN (SELECT DISTINCT date_key FROM fact_box_office_daily);

-- Film con incasso totale anomalo (decrescente)
SELECT date_key, film_key, total_gross_eur
FROM fact_box_office_daily
ORDER BY film_key, date_key;
```

---

## Struttura finale del repository

```
boxoffice.it/
├── .github/
│   └── workflows/
│       ├── daily_pipeline.yml
│       └── backfill.yml
├── pipeline/
│   ├── scraper.py          # Cineguru scraper (Playwright)
│   ├── loader.py           # CSV → Neon PostgreSQL
│   └── enrich_tmdb.py      # Enrichment TMDB API
├── schema/
│   ├── star_schema.ddl     # DDL completo
│   ├── seed_dim_date.sql
│   ├── seed_dim_distributor.sql
│   └── seed_dim_genre.sql
├── sql/
│   └── qa_checks.sql
├── data/
│   └── raw/                # CSV giornalieri (gitignored)
├── ROADMAP_NEON.md
└── README.md
```

---

## Timeline riepilogativa

| Fase | Attività | Durata stimata |
|------|----------|----------------|
| 1 | Setup Neon + branching | 2 ore |
| 2 | Schema deployment + seed | 4 ore |
| 3 | GitHub Secrets | 30 min |
| 4 | Loader script | 1 giorno |
| 5 | GitHub Actions workflows | 1 giorno |
| 6 | Enrichment TMDB | 2 giorni |
| 7 | Monitoraggio e QA | 1 giorno |
| **Totale** | | **~1 settimana** |