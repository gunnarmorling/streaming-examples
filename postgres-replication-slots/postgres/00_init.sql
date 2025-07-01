BEGIN;

CREATE USER prom_user WITH login ENCRYPTED PASSWORD 'kusnyf-maczuz-7qabnA';
GRANT pg_monitor TO prom_user;

CREATE USER dbz_user WITH replication login ENCRYPTED PASSWORD 'kusnyf-maczuz-7qabnA';
GRANT USAGE ON SCHEMA public TO dbz_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO dbz_user;

CREATE SCHEMA inventory;
GRANT USAGE ON SCHEMA inventory TO dbz_user;

CREATE PUBLICATION dbz_publication FOR ALL TABLES;

-- customers
CREATE TABLE inventory.customers (
  id SERIAL NOT NULL PRIMARY KEY,
  first_name VARCHAR(255) NOT NULL,
  last_name VARCHAR(255) NOT NULL,
  email VARCHAR(255) NOT NULL UNIQUE,
  is_test_account BOOLEAN NOT NULL
);

ALTER SEQUENCE inventory.customers_id_seq RESTART WITH 1001;
ALTER TABLE inventory.customers REPLICA IDENTITY FULL;

INSERT INTO inventory.customers
VALUES (default, 'Sally', 'Thomas', 'sally.thomas@acme.com', FALSE),
       (default, 'George', 'Bailey', 'gbailey@foobar.com', FALSE),
       (default, 'Edward', 'Walker', 'ed@walker.com', FALSE),
       (default, 'Aidan', 'Barrett', 'aidan@example.com', TRUE),
       (default, 'Anne', 'Kretchmar', 'annek@noanswer.org', TRUE),
       (default, 'Melissa', 'Cole', 'melissa@example.com', FALSE),
       (default, 'Rosalie', 'Stewart', 'rosalie@example.com', FALSE);

GRANT SELECT ON ALL TABLES IN SCHEMA inventory TO dbz_user;

COMMIT;

CREATE DATABASE shipmentdb;
