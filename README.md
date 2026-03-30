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

### 1. Raw ingestion from Cineguru

```bash
boxoffice-int ingest --start 2026-01-01 --end 2026-01-31
```

Output: `data/raw/box_office_raw/cineguru_2026-01-01_2026-01-31.csv`

**Cinetel fallback** — if Cineguru has not yet published the article for a given day, pass `--cinetel-url` with the Cinetel homepage URL and the pipeline will automatically fall back to scraping Cinetel for that date:

```bash
boxoffice-int ingest --start 2026-03-30 --end 2026-03-30 \
  --cinetel-url "https://www.cinetel.it/homepage"
```

Output: `data/raw/box_office_raw/cinetel_2026-03-30.csv`

> The Cinetel fallback is only supported for single-day ranges.

### 2. TMDB metadata enrichment

```bash
boxoffice-int enrich --input data/raw/box_office_raw/cineguru_2026-01-01_2026-01-31.csv
```

Output: `data/curated/film_metadata/film_metadata.csv`

### 3. Build analytics data product

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
      cineguru_scraper.py
    film_metadata/
      tmdb_client.py
    market_analytics/
      build_product.py
  pipeline.py
contracts/
  box_office_raw_daily.schema.yaml
  market_analytics_kpis.schema.yaml
docs/
  architecture.md
```

## CI / orchestration

The `.github/workflows/daily_pipeline.yml` workflow runs **every day at 23:00 UTC** (midnight Italian time):
`test → ingest → load → enrich-db → enrich → build`.

Required secrets in the GitHub repository:

| Secret | Used by |
|---|---|
| `TMDB_API_KEY` | `enrich`, `enrich-db` steps |
| `BOXOFFICE_DB_URL` | `load`, `enrich-db` steps |
