# boxoffice.int — Data Product (Italia Box Office)

Data product costruito con approccio **Data Mesh** per analizzare il box office italiano su base giornaliera.

## Obiettivo

Produrre un dataset consumabile da BI/analytics con:

- classifica giornaliera per film
- KPI giornalieri di mercato
- arricchimento metadati film (TMDB)

## Domini Data Mesh

1. `box_office_raw` (source-aligned)
   - ingestione dati Cineguru in formato raw e curated
2. `film_metadata` (source-aligned)
   - metadati film da TMDB
3. `market_analytics` (consumer-aligned)
   - data product finale con KPI e join tra domini

## Struttura progetto

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

## Setup rapido

1. Crea ambiente virtuale:
   - Windows PowerShell: `python -m venv .venv`
   - Attivazione: `.\.venv\Scripts\Activate.ps1`
2. Installa dipendenze: `pip install -r requirements.txt`
3. Installa browser Playwright: `playwright install chromium`

## Esecuzione pipeline

### 1) Ingestione box office raw

```bash
python -m src.boxoffice_int.pipeline ingest --start 2025-01-01 --end 2025-01-31
```

Output:

- `data/raw/box_office_raw/cineguru_YYYY-MM-DD_YYYY-MM-DD.csv`

### 2) Enrichment metadata (opzionale, ma consigliato)

Imposta variabile ambiente:

```bash
setx TMDB_API_KEY "<YOUR_KEY>"
```

Poi esegui:

```bash
python -m src.boxoffice_int.pipeline enrich --input data/raw/box_office_raw/cineguru_2025-01-01_2025-01-31.csv
```

Output:

- `data/curated/film_metadata/film_metadata.csv`

### 3) Build data product analytics

```bash
python -m src.boxoffice_int.pipeline build --input data/raw/box_office_raw/cineguru_2025-01-01_2025-01-31.csv
```

Output:

- `data/products/market_analytics/fact_daily_boxoffice.parquet`
- `data/products/market_analytics/kpi_daily_market.parquet`

## Best practice incluse

- separazione per domini (ownership chiara)
- data contract versionabile (`contracts/`)
- idempotenza (output deterministico su stesso input)
- typing + validazioni di schema minime
- layer raw/curated/product

## Prossimi step raccomandati

- orchestrazione schedulata (Airflow/Prefect/GitHub Actions)
- test automatici qualità dati (Great Expectations)
- pubblicazione su warehouse (BigQuery/Snowflake/Postgres)
