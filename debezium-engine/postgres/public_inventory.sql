SET search_path TO inventory;

CREATE TABLE authors (
  id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY (START WITH 1001),
  first_name VARCHAR(255) NOT NULL,
  last_name VARCHAR(255) NOT NULL,
  biography TEXT STORAGE EXTERNAL,
  dob TIMESTAMP
);

-- Taken from https://en.wikipedia.org/wiki/Tom_Wolfe
-- Text is available under the Creative Commons Attribution-ShareAlike 4.0 License (https://en.wikipedia.org/wiki/Wikipedia:Text_of_the_Creative_Commons_Attribution-ShareAlike_4.0_International_License);
INSERT INTO authors (first_name, last_name, biography, dob) VALUES (
    'Tom',
    'Wolfe',
    'Thomas Kennerly Wolfe Jr. (March 2, 1930 – May 14, 2018)[a] ...',
    '1930-03-01 00:00:00'),

  -- Taken from https://en.wikipedia.org/wiki/Agatha_Christie
  -- Text is available under the Creative Commons Attribution-ShareAlike 4.0 License (https://en.wikipedia.org/wiki/Wikipedia:Text_of_the_Creative_Commons_Attribution-ShareAlike_4.0_International_License);
  (
    'Agatha',
    'Christie',
    '1890–1907: childhood and adolescence ...',
    '1890-09-15 00:00:00'
  );

CREATE TABLE books (
  id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY (START WITH 1),
  title VARCHAR(255) NOT NULL,
  author_id INT REFERENCES authors(id),
  isbn CHAR(13),
  published_date DATE,
  price NUMERIC(10, 2),
  in_stock BOOLEAN DEFAULT TRUE,
  page_count SMALLINT,
  description TEXT,
  metadata JSONB
);

INSERT INTO books (title, author_id, isbn, published_date, price, in_stock, page_count, description, metadata) VALUES
  ('The Bonfire of the Vanities', 1001, '9780312427573', '1987-10-01', 16.99, TRUE, 690, 'A satirical novel about ambition, racism, and social class in 1980s New York City.', '{"genre": "satire", "awards": ["National Book Critics Circle Award nomination"], "language": "en"}'),
  ('The Right Stuff', 1001, '9780312427566', '1979-01-01', 15.99, TRUE, 352, 'An account of the pilots who became the first Project Mercury astronauts.', '{"genre": "non-fiction", "awards": ["National Book Award"], "language": "en", "film_adaptation": 1983}'),
  ('Murder on the Orient Express', 1002, '9780062693662', '1934-01-01', 14.99, TRUE, 256, 'Hercule Poirot investigates a murder aboard the famous train.', '{"genre": "mystery", "detective": "Hercule Poirot", "language": "en"}'),
  ('And Then There Were None', 1002, '9780062073488', '1939-11-06', 14.99, FALSE, 272, 'Ten strangers are lured to an island and accused of past crimes.', '{"genre": "mystery", "language": "en", "original_title": "Ten Little Niggers"}');

-- Create a large number of tables if needed for testing
SELECT format('
  CREATE TABLE books_%s (
    id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY (START WITH 1),
    title VARCHAR(255) NOT NULL,
    author_id INT REFERENCES authors(id),
    isbn CHAR(13),
    published_date DATE,
    price NUMERIC(10, 2),
    in_stock BOOLEAN DEFAULT TRUE,
    page_count SMALLINT,
    description TEXT,
    metadata JSONB
  );', lpad(i::text, 5, '0'))
FROM generate_series(1, 5) AS i
\gexec

INSERT INTO books_00001 (title, author_id, isbn, published_date, price, in_stock, page_count, description, metadata) VALUES
  ('The Bonfire of the Vanities', 1001, '9780312427573', '1987-10-01', 16.99, TRUE, 690, 'A satirical novel about ambition, racism, and social class in 1980s New York City.', '{"genre": "satire", "awards": ["National Book Critics Circle Award nomination"], "language": "en"}');
