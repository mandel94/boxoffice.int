# Data Model Implementation Guide

## Overview

The **boxoffice.int** project implements a modern **Data Mesh** architecture with a three-layer data pipeline that transforms raw box office data into business-ready analytics. The data model combines:

- **Data Contracts** (YAML-based) — Define data product boundaries and quality requirements
- **Star Schema** (PostgreSQL) — Dimensionally-modeled warehouse for efficient analytics
- **Domain Layer** (Python) — Domain-driven logic for data ingestion, enrichment, and transformation

---

## 1. Architecture Principles

### Data Mesh Organization

The data platform is organized around **three autonomous domains**, each with explicit ownership and contractual outputs:

| Domain | Responsibility | Output | Layer |
|--------|-----------------|--------|-------|
| **box_office_raw** | Ingest, parse, quality check | Daily top-10 box office from Cineguru | Raw |
| **film_metadata** | Normalize titles, enrich via TMDB API | Normalized film attributes (genres, release dates, etc.) | Curated |
| **market_analytics** | Aggregate KPIs, join enriched data | Daily analytics fact and KPI aggregations | Products |

### Data Layers

```
data/raw/           ← Source data (CSV from Cineguru scraper)
    ├── box_office_raw/
    └── cineguru_*.csv

data/curated/       ← Normalized and enriched datasets
    ├── film_metadata/
    └── *.parquet

data/products/      ← Consumable analytics datasets
    ├── market_analytics_daily/
    └── *.parquet
```

---

## 2. Data Contracts

### Purpose

Contracts establish **data product boundaries** following Data Mesh principles. Each domain publishes a contract that defines:
- Column names and types
- Nullable constraints
- Semantic meaning of fields
- Quality gates (e.g., range checks)

### Contract Structure

All contracts are stored in `/contracts/` as YAML files with this schema:

```yaml
apiVersion: boxoffice.int/v1
kind: DataContract

metadata:
  id: <contract-id>              # Unique identifier (e.g., "box-office-raw-daily")
  version: "1.0.0"               # Semantic versioning
  domain: <domain-name>          # Domain ownership
  owner: team-data-engineering   # Team responsible
  status: active                 # active | deprecated | sunset
  created: "2026-01-01"
  updated: "2026-03-09"

info:
  title: <Human-readable title>
  description: >
    Multi-line description of the data product and its purpose.
  source: <URL to source system>

schema:
  dataset: <dataset-name>        # Name for storage (table, dataset, etc.)
  format: csv | parquet
  primary_key:
    - <field1>
    - <field2>

  fields:
    - name: <column-name>
      type: string | integer | date | float
      nullable: bool (default: false)
      description: >
        Field description (use for semantics, units, examples).
      constraints:
        minimum: <value>
        maximum: <value>

quality:
  completeness:
    required_fields:
      - <field1>
      - <field2>
```

### Existing Contracts

#### `box_office_raw_daily.schema.yaml`

Ingested daily top-10 from Cineguru (raw layer, no transformations):

| Field | Type | Nullable | Constraints |
|-------|------|----------|-------------|
| `date` | date | No | — |
| `rank` | integer | No | 1–10 |
| `title` | string | No | As-is from source |
| `gross_eur` | integer | Yes | ≥ 0 |
| `admissions` | integer | Yes | ≥ 0 |
| `cinemas` | integer | Yes | ≥ 0 |

**Primary Key:** `(date, rank)`  
**Quality Gates:**  
- `rank` must be between 1 and 10
- `gross_eur` cannot be negative
- Deduplicate on `(date, rank, title)`

#### `film_metadata.schema.yaml`

Normalized film information joined from TMDB API:

| Field | Type | Nullable | Purpose |
|-------|------|----------|---------|
| `title_norm` | string | No | Normalized title (lowercase, single spaces) — join key |
| `tmdb_id` | integer | Yes | Unique TMDB identifier |
| `original_title` | string | Yes | Film's original title |
| `release_date` | date | Yes | Official release date |
| `original_language` | string | Yes | ISO 639-1 code (e.g., "it", "en") |
| `popularity_score` | float | Yes | TMDB popularity index |
| `vote_average` | float | Yes | TMDB rating (0–10) |

**Primary Key:** `(title_norm)`  
**Join Strategy:** Match `box_office_raw.title` normalized → `film_metadata.title_norm` (left join)

#### `market_analytics_kpis.schema.yaml`

Derived analytics product for BI consumption:

| Field | Type | Purpose |
|-------|------|---------|
| `date` | date | KPI reference date |
| `gross_total_eur` | integer | Sum of top-10 daily gross |
| `admissions_total` | integer | Sum of top-10 admissions |
| `cinemas_total` | integer | Count of unique cinemas |
| `unique_titles` | integer | Distinct titles in top-10 |
| `avg_gross_per_cinema` | float | Daily average per cinema |

**Aggregation Level:** Daily  
**Composition:** ~1 row/day

---

## 3. Star Schema (Dimensional Model)

### Overview

The warehouse implements a **star schema** in PostgreSQL, optimized for analytics queries with dimension and fact tables.

### Schema Design

```
                    dim_date
                      ↑
                      |
    dim_distributor ← fact_box_office_daily → dim_film
                      ↑        ↓              ↑
                      |        |              |
                  dim_source   |      bridge_film_genre
                               |              ↑
                               ↓              |
                          dim_genre ←────────┘
```

### Dimension Tables

#### `dim_date`

Pre-populated time dimension (2015-01-01 to 2035-12-31).

```sql
CREATE TABLE dim_date (
    date_key          INTEGER PRIMARY KEY,        -- YYYYMMDD (compact)
    full_date         DATE NOT NULL UNIQUE,
    year              SMALLINT NOT NULL,
    quarter           SMALLINT NOT NULL,          -- 1-4
    month             SMALLINT NOT NULL,          -- 1-12
    month_name_it     VARCHAR(20) NOT NULL,       -- "January", "February", … (Italian locale)
    week_number       SMALLINT NOT NULL,          -- ISO 8601
    day_of_week       SMALLINT NOT NULL,          -- 1=Mon, 7=Sun
    is_weekend        BOOLEAN NOT NULL,           -- Mon-Fri=false, Sat-Sun=true
    is_holiday        BOOLEAN NOT NULL DEFAULT,   -- Italian holidays
    holiday_name      VARCHAR(60),                -- NULL if not a holiday
    season            VARCHAR(10) NOT NULL,       -- "spring", "summer", "autumn", "winter"
    cinema_week       SMALLINT NOT NULL,          -- Italian cinema week indicator
    created_at        TIMESTAMP NOT NULL DEFAULT NOW()
);
```

**Key Design Decisions:**

- **`date_key` as YYYYMMDD integer:** Enables efficient range queries (`WHERE date_key BETWEEN 20260301 AND 20260331`) without date casting
- **`cinema_week`:** Tracks Italian cinema industry weeks (Thu–Wed), crucial for industry reporting
- **`is_weekend`:** Pre-computed for quick filtering on cinema performance patterns
- **`season`:** Supports seasonal analysis (e.g., summer vs. winter box office trends)
- **Idempotent seed:** The `seed_dim_date()` function populates this once and can be run repeatedly without duplication

#### `dim_film`

Film master data (normalized from box_office_raw and enriched via film_metadata domain):

```sql
CREATE TABLE dim_film (
    film_key          SERIAL PRIMARY KEY,
    title_norm        VARCHAR(255) NOT NULL UNIQUE,  -- Join key from contracts
    original_title    VARCHAR(255),
    tmdb_id           INTEGER,
    release_date      DATE,
    original_language CHAR(2),                       -- ISO 639-1
    popularity_score  REAL,
    vote_average      REAL,
    is_italian        BOOLEAN,                       -- original_language = 'it'
    created_at        TIMESTAMP NOT NULL DEFAULT NOW()
);
```

#### `dim_genre`

Film genres (Italian names):

```sql
CREATE TABLE dim_genre (
    genre_key         SERIAL PRIMARY KEY,
    genre_name        VARCHAR(50) NOT NULL UNIQUE,
    created_at        TIMESTAMP NOT NULL
);
```

#### `dim_distributor`

Film distributors (extracted from source metadata):

```sql
CREATE TABLE dim_distributor (
    distributor_key   SERIAL PRIMARY KEY,
    distributor_name  VARCHAR(255) NOT NULL UNIQUE,
    country_code      CHAR(2),
    created_at        TIMESTAMP NOT NULL
);
```

#### `dim_source`

Data sources (Cineguru, other scrapers, feeds):

```sql
CREATE TABLE dim_source (
    source_key        SERIAL PRIMARY KEY,
    source_name       VARCHAR(100) NOT NULL UNIQUE,  -- "cineguru", etc.
    source_url        VARCHAR(500),
    created_at        TIMESTAMP NOT NULL
);
```

#### `bridge_film_genre`

Many-to-many relationship between films and genres:

```sql
CREATE TABLE bridge_film_genre (
    film_key          INTEGER NOT NULL REFERENCES dim_film(film_key),
    genre_key         INTEGER NOT NULL REFERENCES dim_genre(genre_key),
    PRIMARY KEY (film_key, genre_key)
);
```

### Fact Table

#### `fact_box_office_daily`

Daily box office observations aggregated at the film level (one row per film per date):

```sql
CREATE TABLE fact_box_office_daily (
    fact_key              BIGSERIAL PRIMARY KEY,
    date_key              INTEGER NOT NULL REFERENCES dim_date(date_key),
    film_key              INTEGER REFERENCES dim_film(film_key),
    distributor_key       INTEGER REFERENCES dim_distributor(distributor_key),
    source_key            INTEGER NOT NULL REFERENCES dim_source(source_key),
    
    rank                  SMALLINT NOT NULL CHECK (rank BETWEEN 1 AND 100),
    gross_eur             INTEGER NOT NULL CHECK (gross_eur >= 0),
    admissions            INTEGER CHECK (admissions >= 0),
    cinemas               INTEGER CHECK (cinemas >= 0),
    avg_gross_per_cinema  REAL,
    
    loaded_at             TIMESTAMP NOT NULL DEFAULT NOW(),
    
    UNIQUE (date_key, film_key),
    INDEX idx_date_rank (date_key, rank),
    INDEX idx_film (film_key)
);
```

---

## 4. Data Contracts Validation

Contracts are **automatically validated** at domain boundaries using [Pandera](https://pandera.readthedocs.io/).

### Validation Framework

The `boxoffice_int.contracts` module provides:

#### `load_contract(contract_id: str) → dict`

Load a contract YAML by ID or filename:

```python
from boxoffice_int.contracts import load_contract

contract = load_contract("box-office-raw-daily")
# or
contract = load_contract("box_office_raw_daily")  # filename stem
```

#### `build_pandera_schema(contract: dict) → pa.DataFrameSchema`

Compile a contract into a Pandera schema for runtime validation:

```python
from boxoffice_int.contracts import build_pandera_schema

schema = build_pandera_schema(contract)
# Use for type coercion and validation
```

#### `validate(df: pd.DataFrame, contract: dict) → None`

Validate an entire DataFrame against a contract; raises `ContractViolationError` on failure:

```python
from boxoffice_int.contracts import validate, ContractViolationError

try:
    validate(df, contract)
except ContractViolationError as e:
    print(f"Validation failed: {e}")
    # Handle error (log, alert, reject data)
```

#### `cast_to_contract(df: pd.DataFrame, contract: dict) → pd.DataFrame`

Coerce DataFrame columns to contract types:

```python
from boxoffice_int.contracts import cast_to_contract

df_coerced = cast_to_contract(df, contract)
```

### Validation Features

- **Type coercion:** Converts columns to declared types (string → object, date → datetime64, integer → Int64)
- **Nullability constraints:** Marks non-nullable fields as required
- **Min/max constraints:** Enforces numeric bounds (e.g., `rank: 1–10`)
- **Custom rules:** Pluggable Pandera checks for domain-specific rules
- **Schema evolution:** Allows extra columns not in the contract (lenient mode)

---

## 5. Domain Layer Implementation

### Domain Structure

Each domain is a Python package with:
- **Contract file** (YAML in `/contracts/`)
- **Processing logic** (Python in `/src/boxoffice_int/domain/<domain>/`)
- **Explicit boundaries** (no cross-domain imports)

### Domain: `box_office_raw`

**Path:** `src/boxoffice_int/domain/box_office_raw/`

**Responsibility:** Ingest, parse, and validate raw box office data from Cineguru.

#### Scraper: `cineguru_scraper.py`

Extracts top-10 box office rankings using Playwright (handles dynamic HTML):

```python
from boxoffice_int.domain.box_office_raw.cineguru_scraper import scrape_cineguru

# Scrape a date range
df = scrape_cineguru(
    start_date=date(2026, 3, 1),
    end_date=date(2026, 3, 31)
)

# Output: DataFrame with columns: date, rank, title, gross_eur, admissions, cinemas
```

**Parsing Strategy:**
- Uses Playwright for client-side rendering (handles JavaScript)
- Implements regex patterns for robust box office entry parsing
- Normalizes numbers (Italian format: dots/commas as thousand separators)
- Handles author-specific formatting variations (per-author regex strategies via hash registry)

**Output Contract:** `box_office_raw_daily.schema.yaml`

### Domain: `film_metadata`

**Path:** `src/boxoffice_int/domain/film_metadata/`

**Responsibility:** Enrich raw film titles with normalized data via TMDB API.

#### TMDB Client: `tmdb_client.py`

Queries TMDB API to lookup and enrich film metadata:

```python
from boxoffice_int.domain.film_metadata.tmdb_client import enrich_titles_with_tmdb

# Enriches raw film titles with TMDB data
df = enrich_titles_with_tmdb(
    box_office_df,           # DataFrame with raw titles
    batch_size=50,           # API request batch size
    fuzzy_threshold=0.8      # Title matching similarity threshold
)

# Output: DataFrame with additional columns:
# - tmdb_id, original_title, release_date, original_language, popularity_score, vote_average
```

**Functionality:**
- **Title normalization:** Converts titles to lowercase, removes extra spaces (join key)
- **Fuzzy matching:** Uses `rapidfuzz` to match normalized titles against TMDB catalog
- **Batching:** Groups API requests for efficiency
- **Caching:** (Optional) Caches TMDB responses to avoid redundant API calls
- **Fallback:** If no TMDB match found, returns NULL values

**Output Contract:** `film_metadata.schema.yaml`

### Domain: `market_analytics`

**Path:** `src/boxoffice_int/domain/market_analytics/`

**Responsibility:** Join enriched data and compute daily analytics KPIs.

#### KPI Builder: `build_product.py`

Aggregates top-10 data and computes analytics KPIs:

```python
from boxoffice_int.domain.market_analytics.build_product import build_market_analytics

df_kpis = build_market_analytics(
    box_office_df,      # Raw top-10 DataFrame
    metadata_df=None    # Optional: enriched metadata
)

# Output: DataFrame with one row per date, columns:
# - date, gross_total_eur, admissions_total, cinemas_total, unique_titles, avg_gross_per_cinema
```

**Aggregations:**
- **gross_total_eur:** Sum of gross across top-10 films
- **admissions_total:** Sum of admissions
- **cinemas_total:** Total cinema count (or distinct count if available)
- **unique_titles:** Count of distinct films in top-10
- **avg_gross_per_cinema:** Derived metric: `gross_total_eur / cinemas_total`

**Output Contract:** `market_analytics_kpis.schema.yaml`

---

## 6. Pipeline Orchestration

### Pipeline Module: `pipeline.py`

Orchestrates end-to-end data flow with CLI entry points.

```bash
# Ingest (fetch raw data from Cineguru for a date range)
python -m boxoffice_int.pipeline ingest --yesterday

# Enrich (join with TMDB metadata)
python -m boxoffice_int.pipeline enrich --this-week

# Build (compute analytics KPIs)
python -m boxoffice_int.pipeline build --last-month

# Full pipeline
python -m boxoffice_int.pipeline run --start 2026-01-01 --end 2026-03-31
```

### Date Range Presets

| Preset | Range |
|--------|-------|
| `--yesterday` | Previous day |
| `--this-week` | Monday of current week → today |
| `--last-week` | Previous Monday → previous Sunday |
| `--this-month` | 1st of current month → today |
| `--last-month` | 1st of previous month → last day of previous month |

Or use explicit range:
```bash
python -m boxoffice_int.pipeline ingest --start 2026-01-01 --end 2026-03-31
```

---

## 7. Warehouse Integration

### Loader Module: `warehouse/loader.py`

Populates the star schema from validated domain outputs.

```python
from boxoffice_int.warehouse.loader import (
    get_connection,
    seed_dim_date,
    load_box_office_raw
)

# Initialize connection
conn = get_connection()

# Seed time dimension (idempotent)
seed_dim_date(conn)

# Load raw box office CSV into fact table
n_rows = load_box_office_raw(
    csv_path=Path("data/raw/box_office_raw/cineguru_2026-03-19_2026-03-19.csv"),
    source_key=1  # Cineguru source ID
)
print(f"Loaded {n_rows} rows into fact_box_office_daily")

conn.close()
```

### Loader Workflow

1. **Read CSV** — Parse box office raw CSV
2. **Validate** — Check against `box_office_raw_daily` contract
3. **Normalize** — Apply title normalization, foreign key lookups
4. **Lookup dimensions** — Match/insert into `dim_film`, `dim_distributor`, etc.
5. **Insert fact** — Load rows into `fact_box_office_daily` with surrogate keys
6. **Commit** — Transaction boundary ensures atomicity

---

## 8. Data Quality & Validation

### Quality Gates

Each domain enforces quality checks at its boundary:

**box_office_raw:**
- `rank` ∈ [1, 10]
- `gross_eur` ≥ 0
- No null required fields
- Deduplicate on `(date, rank, title)`

**film_metadata:**
- `title_norm` non-null (join key)
- `tmdb_id` matches TMDB catalog (or NULL if not found)
- Type constraints validated via Pandera

**market_analytics:**
- `date` non-null
- `gross_total_eur`, `admissions_total` ≥ 0
- Derived metrics consistent (e.g., `avg_gross_per_cinema = gross_total_eur / cinemas_total`)

### Error Handling

```python
from boxoffice_int.contracts import ContractViolationError, validate

try:
    validate(df, contract)
except ContractViolationError as e:
    # Log error details
    logger.error(f"Contract violation: {e}")
    
    # Optional: Return to source domain for reprocessing
    # Optional: Send alert to data steward
    
    # Reject data from proceeding downstream
    raise
```

---

## 9. Common Patterns & Best Practices

### Pattern: Domain-to-Domain Communication

Domains communicate **only via published contracts**, never through shared code:

❌ **Anti-pattern (DO NOT DO):**
```python
# In market_analytics domain
from boxoffice_int.domain.box_office_raw.cineguru_scraper import scrape_cineguru
```

✅ **Pattern (DO THIS):**
```python
# In domain boundary
from boxoffice_int.contracts import load_contract
contract = load_contract("box-office-raw-daily")
# Read CSV file produced by box_office_raw domain
df = pd.read_csv("data/raw/box_office_raw/cineguru_*.csv")
validate(df, contract)
```

### Pattern: Title Normalization

The same title normalization logic is used consistently across domains:

```python
from boxoffice_int.common import normalize_title

# Apply same rules everywhere for join consistency
title_raw = "La Grande Bellezza"
title_norm = normalize_title(title_raw)  # "la grande bellezza"
```

### Pattern: Date Keys for Efficiency

Always use `YYYYMMDD` integer key for date dimensions to avoid casting overhead:

```sql
-- ✅ Efficient: no function call
SELECT * FROM fact_box_office_daily
WHERE date_key BETWEEN 20260301 AND 20260331;

-- ❌ Less efficient: requires function evaluation
SELECT * FROM fact_box_office_daily
WHERE extract(month FROM full_date) = 3;
```

---

## 10. Extending the Data Model

### Adding a New Data Product

1. **Create contract:** New file in `/contracts/new_product.schema.yaml`
2. **Create domain:** New package in `/src/boxoffice_int/domain/new_product/`
3. **Implement logic:** Transform/aggregate input data
4. **Validate output:** Apply contract validation at domain boundary
5. **Update pipeline:** Add CLI command in `pipeline.py`
6. **Integrate warehouse:** Add loader functions if persisting to star schema

### Adding a New Dimension

1. Add table to `schema/schema.sql`
2. Update `loader.py` with lookup/insert logic
3. Update fact table foreign keys
4. Implement idempotent seed function (if applicable)

---

## 11. References

- **Data Mesh:** [Zhamak Dehghani's blog](https://martinfowler.com/articles/data-mesh.html)
- **Star Schema:** [Ralph Kimball's Dimensional Modeling](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/)
- **Data Contracts:** [Atlan Data Contracts Framework](https://atlan.com/blog/data-contracts/)
- **Pandera:** [Schema Validation for Pandas](https://pandera.readthedocs.io/)
- **TMDB API:** [The Movie Database API Documentation](https://developer.themoviedb.org/)

---

## Glossary

| Term | Definition |
|------|-----------|
| **Data Contract** | YAML specification defining a data product's structure, semantics, and quality requirements |
| **Domain** | Autonomous team/system owning end-to-end responsibility for a data product |
| **Data Mesh** | Decentralized data architecture where domains own their data products |
| **Star Schema** | Dimensional model with central fact table and surrounding dimension tables optimized for analytics |
| **Fact Table** | Central table in star schema containing measurable events (box office daily observations) |
| **Dimension Table** | Reference table providing context for facts (time, film, distributor) |
| **Primary Key** | Unique identifier within a contract (e.g., `(date, rank)` for raw box office) |
| **Surrogate Key** | System-generated identifier in warehouse (e.g., `film_key` SERIAL) |
