-- ============================================================
--  boxoffice.int - Star Schema DDL
--  PostgreSQL >= 14
--
--  Drop order is the reverse of creation order to satisfy FK
--  constraints during a clean rebuild in development.
-- ============================================================

-- ---- Teardown (reverse dependency order) -------------------
DROP TABLE IF EXISTS fact_box_office_daily  CASCADE;
DROP TABLE IF EXISTS bridge_film_genre      CASCADE;
DROP TABLE IF EXISTS dim_film               CASCADE;
DROP TABLE IF EXISTS dim_genre              CASCADE;
DROP TABLE IF EXISTS dim_distributor        CASCADE;
DROP TABLE IF EXISTS dim_source             CASCADE;
DROP TABLE IF EXISTS dim_date               CASCADE;


-- ============================================================
--  1. dim_date
--  Pre-populated for the full range 2015-01-01 to 2035-12-31.
--  date_key uses the compact integer format YYYYMMDD so that
--  range filters in SQL remain human-readable and fast on a
--  btree index without needing a date cast.
--
--  cinema_week: Italian cinema industry counts weeks Thursday
--  through Wednesday (new releases debut on Thursday). We
--  compute this as the ISO week of the preceding Thursday.
-- ============================================================
CREATE TABLE dim_date (
    date_key        INTEGER      PRIMARY KEY,   -- YYYYMMDD
    full_date       DATE         NOT NULL UNIQUE,
    year            SMALLINT     NOT NULL,
    quarter         SMALLINT     NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    month           SMALLINT     NOT NULL CHECK (month   BETWEEN 1 AND 12),
    month_name_it   VARCHAR(20)  NOT NULL,
    week_number     SMALLINT     NOT NULL,      -- ISO 8601 week
    day_of_week     SMALLINT     NOT NULL CHECK (day_of_week BETWEEN 1 AND 7), -- 1=Mon
    is_weekend      BOOLEAN      NOT NULL,
    is_holiday      BOOLEAN      NOT NULL DEFAULT FALSE,
    holiday_name    VARCHAR(60),                -- NULL when not a holiday
    season          VARCHAR(10)  NOT NULL CHECK (season IN ('primavera','estate','autunno','inverno')),
    cinema_week     SMALLINT     NOT NULL,      -- ISO week of the Thu that opened this cinema week
    created_at      TIMESTAMP    NOT NULL DEFAULT NOW()
);

COMMENT ON COLUMN dim_date.cinema_week IS
    'Italian cinema industry week (Thu-Wed). Value is the ISO week number
     of the Thursday that opened the current cinema week.';


-- ============================================================
--  2. dim_source
--  The data source that produced the box office ranking.
--  Seeded with the sole current source: Cineguru/Screenweek.
-- ============================================================
CREATE TABLE dim_source (
    source_key  SERIAL       PRIMARY KEY,
    name        VARCHAR(80)  NOT NULL UNIQUE,
    url         VARCHAR(255) NOT NULL,
    country     CHAR(2)      NOT NULL,          -- ISO 3166-1 alpha-2
    is_active   BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMP    NOT NULL DEFAULT NOW()
);

-- Seed records
INSERT INTO dim_source (source_key, name, url, country, is_active)
VALUES
    (1, 'Cineguru', 'https://cineguru.screenweek.it', 'IT', TRUE),
    (2, 'Cinetel',  'https://www.cinetel.it',          'IT', TRUE)
ON CONFLICT (source_key) DO NOTHING;


-- ============================================================
--  3. dim_distributor
-- ============================================================
CREATE TABLE dim_distributor (
    distributor_key SERIAL      PRIMARY KEY,
    name            VARCHAR(120) NOT NULL UNIQUE,
    country         CHAR(2),                    -- ISO 3166-1 alpha-2, nullable
    type            VARCHAR(20)  NOT NULL DEFAULT 'unknown'
                        CHECK (type IN ('major', 'independent', 'unknown')),
    created_at      TIMESTAMP    NOT NULL DEFAULT NOW()
);


-- ============================================================
--  4. dim_genre
-- ============================================================
CREATE TABLE dim_genre (
    genre_key    SERIAL       PRIMARY KEY,
    name_it      VARCHAR(60)  NOT NULL,
    name_orig    VARCHAR(60)  NOT NULL,
    tmdb_genre_id INTEGER     UNIQUE,            -- NULL if not mapped to TMDB
    created_at   TIMESTAMP    NOT NULL DEFAULT NOW()
);


-- ============================================================
--  5. dim_film  — Type 2 Slowly Changing Dimension
--
--  A new row is inserted every time a film attribute changes
--  (e.g. distributor reassignment). The superseded row gets
--  valid_to = change_date - 1 and is_current = FALSE.
--  The active record always has is_current = TRUE and
--  valid_to = '9999-12-31'.
--
--  The partial index on (tmdb_id) WHERE is_current = TRUE lets
--  the loader quickly find the active version of a film without
--  a full table scan.
-- ============================================================
CREATE TABLE dim_film (
    film_key          SERIAL       PRIMARY KEY,
    tmdb_id           INTEGER,                  -- nullable: not all titles are on TMDB
    title_cineguru    VARCHAR(255) NOT NULL,     -- raw title exactly as scraped
    title_it          VARCHAR(255),
    title_orig        VARCHAR(255),
    distributor_key   INTEGER      REFERENCES dim_distributor (distributor_key) ON DELETE SET NULL,
    director          VARCHAR(255),
    runtime_min       INTEGER,
    release_date      DATE,
    -- SCD Type 2 validity fields
    valid_from        DATE         NOT NULL,
    valid_to          DATE         NOT NULL DEFAULT '9999-12-31',
    is_current        BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at        TIMESTAMP    NOT NULL DEFAULT NOW()
);

COMMENT ON COLUMN dim_film.valid_from IS
    'SCD2: date from which this version of the film record is active.';
COMMENT ON COLUMN dim_film.valid_to IS
    'SCD2: date on which this version was superseded (9999-12-31 = still active).';
COMMENT ON COLUMN dim_film.is_current IS
    'SCD2: TRUE for the current active version only. Used as a partial index predicate.';

-- Fast lookup for active records by TMDB id
CREATE UNIQUE INDEX dim_film_tmdb_current
    ON dim_film (tmdb_id)
    WHERE is_current = TRUE AND tmdb_id IS NOT NULL;

-- Fast lookup for active records by Cineguru title
CREATE INDEX dim_film_title_cineguru_current
    ON dim_film (title_cineguru)
    WHERE is_current = TRUE;


-- ============================================================
--  6. bridge_film_genre
--  Many-to-many between dim_film and dim_genre.
--  is_primary flags the film's main genre (at most one per film).
-- ============================================================
CREATE TABLE bridge_film_genre (
    film_key    INTEGER   NOT NULL REFERENCES dim_film  (film_key)  ON DELETE CASCADE,
    genre_key   INTEGER   NOT NULL REFERENCES dim_genre (genre_key) ON DELETE CASCADE,
    is_primary  BOOLEAN   NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (film_key, genre_key)
);


-- ============================================================
--  7. fact_box_office_daily
--
--  Grain: one row per (date, film, source) combination.
--  The composite PK enforces uniqueness and powers idempotent
--  upserts with ON CONFLICT DO NOTHING in the loader.
--
--  IMPORTANT: `rank` is a non-additive measure. Never SUM it.
--  Use MIN(rank) to find the peak chart position across a range.
-- ============================================================
CREATE TABLE fact_box_office_daily (
    -- Degenerate dimensions / surrogate keys
    date_key        INTEGER    NOT NULL REFERENCES dim_date   (date_key),
    film_key        INTEGER    NOT NULL REFERENCES dim_film   (film_key),
    source_key      INTEGER    NOT NULL REFERENCES dim_source (source_key),
    -- Non-additive measure: chart position (1-10)
    rank            SMALLINT   NOT NULL CHECK (rank BETWEEN 1 AND 10),
    -- Additive measures
    gross_eur       INTEGER    NOT NULL CHECK (gross_eur >= 0),
    admissions      INTEGER,
    cinemas         INTEGER,
    avg_per_cinema_eur INTEGER,
    total_gross_eur    INTEGER,
    created_at      TIMESTAMP  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (date_key, film_key, source_key)
);

COMMENT ON COLUMN fact_box_office_daily.rank IS
    'Chart position 1-10. Non-additive — use MIN(rank), never SUM.';

-- Supporting indexes for common BI query patterns
CREATE INDEX fact_bo_date  ON fact_box_office_daily (date_key);
CREATE INDEX fact_bo_film  ON fact_box_office_daily (film_key);
