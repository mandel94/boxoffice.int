-- ============================================================
--  seed_dim_distributor.sql
--  Seed data for dim_distributor — Italian film market
--  Idempotent: INSERT ... ON CONFLICT DO NOTHING
-- ============================================================

INSERT INTO dim_distributor (name, country, type) VALUES
    -- Majors (local branches of Hollywood studios)
    ('Warner Bros. Italia',         'IT', 'major'),
    ('Universal Pictures Italia',   'IT', 'major'),
    ('Walt Disney Company Italia',  'IT', 'major'),
    ('The Walt Disney Company',     'US', 'major'),
    ('Sony Pictures Italia',        'IT', 'major'),
    ('Paramount Pictures Italia',   'IT', 'major'),

    -- Italian majors / semi-majors
    ('01 Distribution',             'IT', 'major'),        -- RAI Cinema
    ('Medusa Film',                 'IT', 'major'),        -- Fininvest / Mediaset
    ('Vision Distribution',         'IT', 'major'),        -- Sky Italia / HBO

    -- Italian independents
    ('Lucky Red',                   'IT', 'independent'),
    ('BIM Distribuzione',           'IT', 'independent'),
    ('Notorious RBW',               'IT', 'independent'),
    ('Academy Two',                 'IT', 'independent'),
    ('M2 Pictures',                 'IT', 'independent'),
    ('Koch Media',                  'IT', 'independent'),
    ('Movies Inspired',             'IT', 'independent'),
    ('Eagle Pictures',              'IT', 'independent'),
    ('Adler Entertainment',         'IT', 'independent'),
    ('Fandango',                    'IT', 'independent'),
    ('Altre Storie',                'IT', 'independent'),
    ('Officine UBU',                'IT', 'independent'),
    ('Nexo Digital',                'IT', 'independent'),
    ('I Wonder Pictures',           'IT', 'independent'),
    ('Satine Film',                 'IT', 'independent'),
    ('Tucker Film',                 'IT', 'independent'),
    ('Cinefile',                    'IT', 'independent'),
    ('Teodora Film',                'IT', 'independent'),
    ('Microcinema Distribuzione',   'IT', 'independent'),
    ('Istituto Luce Cinecittà',     'IT', 'independent'),
    ('Minerva Pictures',            'IT', 'independent'),
    ('Valmyn',                      'IT', 'independent'),
    ('True Colours',                'IT', 'independent'),
    ('Wanted Cinema',               'IT', 'independent'),

    -- Placeholder for unknown/untracked distributors
    ('N/D',                         NULL, 'unknown')

ON CONFLICT (name) DO NOTHING;
