-- ============================================================
--  seed_dim_genre.sql
--  Seed data for dim_genre — TMDB genre taxonomy (Italian labels)
--  Idempotent: INSERT ... ON CONFLICT DO NOTHING
-- ============================================================
--
--  tmdb_genre_id values are the canonical TMDB genre IDs:
--    https://developer.themoviedb.org/reference/genre-movie-list
-- ============================================================

INSERT INTO dim_genre (name_it, name_orig, tmdb_genre_id) VALUES
    ('Azione',          'Action',           28),
    ('Avventura',       'Adventure',        12),
    ('Animazione',      'Animation',        16),
    ('Commedia',        'Comedy',           35),
    ('Crimine',         'Crime',            80),
    ('Documentario',    'Documentary',      99),
    ('Dramma',          'Drama',            18),
    ('Famiglia',        'Family',           10751),
    ('Fantasy',         'Fantasy',          14),
    ('Storia',          'History',          36),
    ('Horror',          'Horror',           27),
    ('Musica',          'Music',            10402),
    ('Mistero',         'Mystery',          9648),
    ('Romance',         'Romance',          10749),
    ('Fantascienza',    'Science Fiction',  878),
    ('Film TV',         'TV Movie',         10770),
    ('Thriller',        'Thriller',         53),
    ('Guerra',          'War',              10752),
    ('Western',         'Western',          37)

ON CONFLICT (tmdb_genre_id) DO NOTHING;
