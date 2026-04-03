# boxoffice.int — Data Product (Italian Box Office)

Data product built with a **Data Mesh** approach to analyze the Italian box office on a daily basis.

## Goal

Produce a BI/analytics-consumable dataset with:

- daily per-film rankings
- daily market KPIs

---

## Prerequisites

| Requirement | Minimum version |
|-------------|----------------|
| Python | 3.11 |
| Playwright (Chromium) | installed via `playwright install chromium` |
| `TMDB_API_KEY` | required environment variable for `enrich` |

## Installation

```bash
pip install -e ".[dev]"
playwright install chromium
```

## Environment variables

```bash
# Required for the enrich command
export TMDB_API_KEY=<your_key>
```

On Windows with PowerShell:

```powershell
$env:TMDB_API_KEY = "<your_key>"
```

---

## CLI Usage

All commands assume the virtualenv is activated (`source .venv/bin/activate` or `.venv\Scripts\Activate.ps1` on Windows).

---

### 1. `ingest` — Cineguru daily scraping

```bash
# Explicit date range
boxoffice-int ingest --start 2026-01-01 --end 2026-01-31

# Preset shortcuts
boxoffice-int ingest --yesterday
boxoffice-int ingest --this-week
boxoffice-int ingest --last-week
boxoffice-int ingest --this-month
boxoffice-int ingest --last-month
```

Output: `data/raw/box_office_raw/cineguru_<start>_<end>.csv`

**Cinetel automatic fallback** — if Cineguru has not yet published the article for a
single-day range, pass `--cinetel-url` and the pipeline falls back to Cinetel automatically.
The day is logged in `cinetel_fallback_log` for later backfill of the `cinemas` column.

```bash
boxoffice-int ingest --start 2026-03-30 --end 2026-03-30 \
  --cinetel-url "https://www.cinetel.it/homepage"
```

Output: `data/raw/box_office_raw/cinetel_2026-03-30.csv`

> The Cinetel fallback is only supported for single-day ranges.

---

### 2. `ingest-date` — Single-date ingestion (auto-routes weekends)

Simplified interface for ingesting a single date. Automatically handles weekends:

- **Weekday (Mon–Fri)**: scrapes Cineguru daily article
- **Saturday**: redirects you to scrape the Sunday (when weekend aggregate is published)
- **Sunday**: auto-routes to `sunday-fallback` to compute weekend data

```bash
# Weekday — scrapes Cineguru daily
boxoffice-int ingest-date --date 2026-04-02

# Saturday — shows helpful message directing to Sunday
boxoffice-int ingest-date --date 2026-04-04
# Output: "2026-04-04 è un sabato — i dati del fine-settimana si estraggono dalla domenica. Esegui: boxoffice-int ingest-date --date 2026-04-05"

# Sunday — auto-routes to sunday-fallback for weekend aggregate
boxoffice-int ingest-date --date 2026-03-29
```

Supports `--delay` and `--cinetel-url` (for weekday Cinetel fallback):

```bash
boxoffice-int ingest-date --date 2026-04-02 --delay 1.0 \
  --cinetel-url "https://www.cinetel.it/homepage"
```

Output: `data/raw/box_office_raw/cineguru_YYYY-MM-DD_YYYY-MM-DD.csv` (weekday) or
`data/raw/box_office_raw/cineguru_sunday_YYYY-MM-DD.csv` (Sunday)

---

### 3. `ingest-cinetel` — Direct Cinetel scraping

Use this command to scrape Cinetel directly without going through the Cineguru flow.

```bash
boxoffice-int ingest-cinetel \
  --date 2026-04-01 \
  --url "https://www.cinetel.it/homepage"
```

Output: `data/raw/box_office_raw/cinetel_2026-04-01.csv`

---

### 4. `sunday-fallback` — Reconstruct missing Sunday

Computes Sunday data by subtracting Friday+Saturday Cinetel figures from the weekend
total published in the Cineguru weekend article. Requires DB access.

```bash
boxoffice-int sunday-fallback --date 2026-03-29
```

Output: `data/raw/box_office_raw/cineguru_sunday_2026-03-29.csv`

---

### 5. `backfill-cinemas` — Backfill cinema count for Cinetel fallback days

Re-scrapes Cineguru to fill the `cinemas` column for days that were originally ingested
via Cinetel (which does not expose cinema count). Processes all pending dates from
`cinetel_fallback_log`, or a single date if `--date` is specified.

```bash
# All pending days
boxoffice-int backfill-cinemas

# Single day
boxoffice-int backfill-cinemas --date 2026-03-30
```

---

### 6. `load` — Load raw CSV into Neon (star schema)

```bash
boxoffice-int load \
  --input data/raw/box_office_raw/cineguru_2026-01-01_2026-01-31.csv

# Cinetel source (source-key 2)
boxoffice-int load \
  --input data/raw/box_office_raw/cinetel_2026-04-01.csv \
  --source-key 2
```

Inserts into `fact_box_office_daily`. Default `--source-key 1` (Cineguru).

---

### 7. `enrich` — TMDB metadata enrichment (CSV)

```bash
boxoffice-int enrich \
  --input data/raw/box_office_raw/cineguru_2026-01-01_2026-01-31.csv
```

Output: `data/curated/film_metadata/film_metadata.csv`

---

### 8. `enrich-db` — TMDB enrichment of `dim_film` rows in Neon

Enriches only rows in `dim_film` that still have no `tmdb_id`.

```bash
boxoffice-int enrich-db --delay 1.0
```

---

### 9. `build` — Build analytics data product

```bash
boxoffice-int build \
  --input data/raw/box_office_raw/cineguru_2026-01-01_2026-01-31.csv \
  --metadata data/curated/film_metadata/film_metadata.csv
```

Output:
- `data/products/market_analytics/fact_daily_boxoffice.parquet`
- `data/products/market_analytics/kpi_daily_market.parquet`

> `--metadata` is optional: if omitted the build proceeds without the join.

---

### 9. `seed` — Populate `dim_date` in Neon

```bash
boxoffice-int seed --start 2015-01-01 --end 2035-12-31
```

Idempotent. Defaults cover 2015-01-01 → 2035-12-31.

---

## Tests

```bash
pytest tests/ -v
```

---

## Data layers

```
data/
  raw/          # ingestion output (CSV)
  curated/      # enrichment output (CSV)
  products/     # build output (Parquet, BI consumption)
```

## Domain structure

```
src/boxoffice_int/
  domain/
    box_office_raw/     # Cineguru scraping
    film_metadata/      # TMDB enrichment
    market_analytics/   # aggregated KPIs
  contracts.py          # Pandera contract validation
  pipeline.py           # CLI entry-point
contracts/              # YAML data contracts
```

## Data Mesh Domains

1. `box_office_raw` (source-aligned)
   - raw and curated ingestion of Cineguru data
2. `film_metadata` (source-aligned)
   - film metadata from TMDB
3. `market_analytics` (consumer-aligned)
   - final data product with KPIs and cross-domain join

## Project structure

```text
src/boxoffice_int/
  domain/
    box_office_raw/
      cineguru_scraper.py   # Cineguru HTML scraper (primary source)
      cinetel_scraper.py    # Cinetel Angular datatable scraper (fallback / direct)
      sunday_fallback.py    # Sunday reconstruction from weekend diff
    film_metadata/
      tmdb_client.py        # TMDB API enrichment
    market_analytics/
      build_product.py      # KPI aggregation
  warehouse/
    loader.py               # Neon DB load (fact_box_office_daily, star schema)
    enrich_db.py            # dim_film TMDB enrichment
  pipeline.py               # CLI entry-point (all subcommands)
  contracts.py              # Pandera contract validation
contracts/
  box_office_raw_daily.schema.yaml
  film_metadata.schema.yaml
  market_analytics_kpis.schema.yaml
docs/
  architecture.md
  data_model.md
  neon_setup.md
```

## CI / orchestration

The `.github/workflows/daily_pipeline.yml` workflow runs **every day at 23:00 UTC** (midnight Italian time):
`test → ingest → load → enrich-db → enrich → build`.

Required secrets in the GitHub repository:

| Secret | Used by |
|---|---|
| `TMDB_API_KEY` | `enrich`, `enrich-db` steps |
| `BOXOFFICE_DB_URL` | `load`, `enrich-db` steps |
