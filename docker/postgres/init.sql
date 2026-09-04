-- Runs once, on the first start of an empty data volume (see docker-compose.yml).
--
-- The pipeline's Postgres source: the same 5 records as people.json.
--
-- `hobbies` is jsonb, and the values are deliberately stored in the same two shapes the
-- JSON file uses: sometimes an array of objects, sometimes a bare object. The key-spelling
-- problem (hobbies vs Hobbies) cannot exist here because the column name is fixed, but the
-- shape problem can, and Extract.fromPostgres has to handle it.
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
