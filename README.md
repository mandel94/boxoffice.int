# boxoffice.int — Data Product (Italia Box Office)

Data product costruito con approccio **Data Mesh** per analizzare il box office italiano su base giornaliera.

## Obiettivo

Produrre un dataset consumabile da BI/analytics con:

- classifica giornaliera per film
- KPI giornalieri di mercato

---

## Prerequisiti

| Requisito | Versione minima |
|-----------|----------------|
| Python | 3.11 |
| Playwright (Chromium) | installato via `playwright install chromium` |
| `TMDB_API_KEY` | variabile d'ambiente obbligatoria per `enrich` |

## Installazione

```bash
pip install -e ".[dev]"
playwright install chromium
```

## Variabili d'ambiente

```bash
# Obbligatoria per il comando enrich
export TMDB_API_KEY=<la_tua_chiave>
```

Su Windows con PowerShell:

```powershell
$env:TMDB_API_KEY = "<la_tua_chiave>"
```

---

## Utilizzo CLI

### 1. Ingestione raw da Cineguru

```bash
boxoffice-int ingest --start 2026-01-01 --end 2026-01-31
```

Output: `data/raw/box_office_raw/cineguru_2026-01-01_2026-01-31.csv`

### 2. Arricchimento metadati TMDB

```bash
boxoffice-int enrich --input data/raw/box_office_raw/cineguru_2026-01-01_2026-01-31.csv
```

Output: `data/curated/film_metadata/film_metadata.csv`

### 3. Build data product analytics

```bash
boxoffice-int build \
  --input data/raw/box_office_raw/cineguru_2026-01-01_2026-01-31.csv \
  --metadata data/curated/film_metadata/film_metadata.csv
```

Output:
- `data/products/market_analytics/fact_daily_boxoffice.parquet`
- `data/products/market_analytics/kpi_daily_market.parquet`

> `--metadata` è opzionale: se omesso il build procede senza join.

---

## Test

```bash
pytest tests/ -v
```

---

## Layer dati

```
data/
  raw/          # output ingestion (CSV)
  curated/      # output enrichment (CSV)
  products/     # output build (Parquet, consumo BI)
```

## Struttura domini

```
src/boxoffice_int/
  domain/
    box_office_raw/     # scraping Cineguru
    film_metadata/      # arricchimento TMDB
    market_analytics/   # KPI aggregati
  contracts.py          # validazione contratti Pandera
  pipeline.py           # CLI entry-point
contracts/              # YAML data contracts
```
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

## CI / orchestrazione

Il workflow `.github/workflows/daily_pipeline.yml` esegue ogni giorno alle 08:00 UTC:
`test → ingest → enrich → build`. Configura il secret `TMDB_API_KEY` nel repository GitHub.
