# Data Product Architecture (Data Mesh)

## 1. Product thinking

The analytics dataset is treated as a product:

- Explicit ownership per domain
- Contractualized schema
- Versionable and discoverable output

## 2. Domains and ownership

- **Domain `box_office_raw`**
  - Responsibility: ingestion, parsing, and basic quality checks
  - Output: daily per-film table
- **Domain `film_metadata`**
  - Responsibility: external metadata enrichment
  - Output: normalized film registry
- **Domain `market_analytics`**
  - Responsibility: KPIs, aggregations, BI-consumable dataset
  - Output: daily fact and daily KPIs

## 3. Data layers

- `data/raw/`: data extracted from source without complex transformations
- `data/curated/`: normalizations and enrichments
- `data/products/`: final datasets for analytics consumption

## 4. Data contracts

Contracts are in `contracts/` and define:

- Column names
- Expected types
- Minimum nullability
- Semantics of main fields

## 5. Pipeline lineage

### Ingestion paths

| Command | Source | Output file |
|---|---|---|
| `ingest --start … --end …` | Cineguru HTML | `cineguru_<start>_<end>.csv` |
| `ingest … --cinetel-url …` | Cinetel (auto-fallback) | `cinetel_<date>.csv` |
| `ingest-cinetel --date … --url …` | Cinetel (direct) | `cinetel_<date>.csv` |
| `sunday-fallback --date …` | Cineguru weekend article diff | `cineguru_sunday_<date>.csv` |

### Full pipeline (daily)

1. **ingest** (Cineguru) → `box_office_raw`
2. **ingest-cinetel** (Cinetel, fallback or direct) → `box_office_raw`
3. **sunday-fallback** → fills missing Sunday rows from weekend diff
4. **backfill-cinemas** → fills `cinemas` for Cinetel-sourced days
5. **load** → `fact_box_office_daily` in Neon (star schema)
6. **enrich** (CSV) / **enrich-db** (Neon) → `film_metadata` via TMDB
7. **build** → `market_analytics` (Parquet, BI consumption)
8. **seed** → `dim_date` population (idempotent, run once)

## 6. Minimum quality gates

- Mandatory columns present
- `rank` between 1 and 10
- `gross_eur` non-negative
- Deduplication on (`date`, `rank`, `title`)

## 7. Industrialization roadmap

- Daily scheduler ✓ (GitHub Actions, every day at 23:00 UTC)
- Retry policy and alerting
- Observability (structured logs, metrics)
- Automated data quality tests
- Publication layer on DWH
