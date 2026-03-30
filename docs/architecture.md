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

1. Cineguru Scraper → `box_office_raw` (primary source)
2. Cinetel Scraper → `box_office_raw` (fallback when Cineguru article not yet published)
3. TMDB Enrichment → `film_metadata`
4. Join + KPIs → `market_analytics`

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
