SET search_path TO inventory;

CREATE TABLE authors (
  id INT PRIMARY KEY GENERATED ALWAYS AS IDENTITY (START WITH 1001),
  first_name VARCHAR(255) NOT NULL,
  last_name VARCHAR(255) NOT NULL,
  biography TEXT STORAGE EXTERNAL,
  registered TIMESTAMP
);
--ALTER TABLE inventory.authors REPLICA IDENTITY FULL;

INSERT INTO authors (first_name, last_name, biography, registered) VALUES ('John', 'Thomas', 'ZbJ0duDvW', '2025-03-10 21:36:40');
