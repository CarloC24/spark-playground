-- Runs once, on the first start of an empty data volume (see docker-compose.yml).
--
-- Two tables:
--   people           the pipeline's Postgres *source*: the same 5 records as people.json
--   people_enriched  the pipeline's Postgres *sink*: what Transform produces

-- The source. `hobbies` is jsonb, and the values are deliberately stored in the same two
-- shapes the JSON file uses: sometimes an array of objects, sometimes a bare object.
-- The key-spelling problem (hobbies vs Hobbies) cannot exist here because the column
-- name is fixed, but the shape problem can, and Extract.fromPostgres has to handle it.
CREATE TABLE people (
    id      bigint PRIMARY KEY,
    name    text    NOT NULL,
    email   text    NOT NULL,
    age     integer NOT NULL,
    city    text    NOT NULL,
    hobbies jsonb
);

INSERT INTO people (id, name, email, age, city, hobbies) VALUES
    (1, 'Amara Okonkwo',   'amara.okonkwo@example.com',   34, 'Lagos',     '[{"hobbieName": "basketball"}, {"hobbieName": "tennis"}]'),
    (2, 'Bjorn Halvorsen', 'bjorn.halvorsen@example.com', 41, 'Oslo',      '{"hobbieName": "wrestling"}'),
    (3, 'Chen Wei',        'chen.wei@example.com',        29, 'Shanghai',  '[{"hobbieName": "basketball"}, {"hobbieName": "tennis"}]'),
    (4, 'Divya Raman',     'divya.raman@example.com',     37, 'Bengaluru', '{"hobbieName": "wrestling"}'),
    (5, 'Elena Rossi',     'elena.rossi@example.com',     52, 'Milan',     '{"hobbieName": "wrestling"}');

-- The sink. Pre-created rather than left for Spark to create, for two reasons:
--   * Spark's JDBC writer would type `hobbies` as text; we want jsonb.
--   * Column names are snake_case. Spark double-quotes identifiers when it writes, so a
--     DataFrame column called ageGroup would become a case-sensitive "ageGroup" column
--     that every later query has to quote. Load.toPostgres renames before writing.
-- Load writes with truncate=true so this definition survives every rerun.
CREATE TABLE people_enriched (
    id           bigint PRIMARY KEY,
    name         text,
    email        text,
    age          integer,
    city         text,
    hobbies      jsonb,
    age_group    text,
    email_domain text,
    hobby_count  integer
);
