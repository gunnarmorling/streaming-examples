-- connect to shipmentdb

BEGIN;

GRANT CONNECT ON DATABASE shipmentdb TO dbz_user;
CREATE SCHEMA shipment;
GRANT USAGE ON SCHEMA shipment TO dbz_user;

CREATE PUBLICATION dbz_publication FOR ALL TABLES;

CREATE TABLE shipment.shipments (
  id SERIAL NOT NULL PRIMARY KEY,
  origin VARCHAR(255) NOT NULL,
  destination VARCHAR(255) NOT NULL
);
ALTER SEQUENCE shipment.shipments_id_seq RESTART WITH 10001;
ALTER TABLE shipment.shipments REPLICA IDENTITY FULL;
INSERT INTO shipment.shipments
VALUES (default, 'Hamburg', 'New York City'),
       (default, 'Hong Kong', 'Tokio');

GRANT SELECT ON ALL TABLES IN SCHEMA shipment TO dbz_user;

COMMIT;
