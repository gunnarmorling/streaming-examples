SET search_path TO inventory;

CREATE TABLE authors (
  id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY (START WITH 1001),
  first_name VARCHAR(255) NOT NULL,
  last_name VARCHAR(255) NOT NULL,
  biography TEXT STORAGE EXTERNAL,
  registered TIMESTAMP
);
ALTER TABLE authors REPLICA IDENTITY FULL;

-- A table with default replica identity,
-- thus Debezium events for it won't have the "before" part
CREATE TABLE authors_default (
  id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY (START WITH 1001),
  first_name VARCHAR(255) NOT NULL,
  last_name VARCHAR(255) NOT NULL,
  biography TEXT STORAGE EXTERNAL,
  registered TIMESTAMP
);

INSERT INTO authors (first_name, last_name, biography, registered) VALUES ('John', 'Thomas', 'ZbJ0duDvW', '2025-03-10 21:36:40');
INSERT INTO authors_default (first_name, last_name, biography, registered) VALUES ('John', 'Thomas', 'ZbJ0duDvW', '2025-03-10 21:36:40');