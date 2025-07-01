# Monitoring Postgres Replication Slots

An example set-up which shows how to monitor Postgres replication slots using Prometheus and Grafana.

## Prerequisites

Make sure to have the following software installed on your machine:

* Docker and Docker Compose
* kcat
* jq (optional)
* kcctl 🐻:

```bash
brew install kcctl/tap/kcctl
source <(wget -q -O - https://raw.githubusercontent.com/kcctl/kcctl/v1.0.0.CR4/kcctl_completion)
```

## Preparation

Start up all the components using Docker Compose:

```bash
docker compose up
```

Register the Debezium connectors:

```bash
kcctl apply -f public-source-1.json
kcctl apply -f public-source-2.json
kcctl apply -f shipment-source.json
```

Verify all connectors are running:

```bash
kcctl get connectors

 NAME                                TYPE     STATE     TASKS
 public-connector-1                  source   RUNNING   0: RUNNING
 public-connector-2                  source   RUNNING   0: RUNNING
 shipment-connector                  source   RUNNING   0: RUNNING
```

Create data changes in the `inventorydb` database:

```bash
docker-compose exec postgres bash

pgbench -i -s 10 -U user inventorydb
pgbench -U user -c 20 -j 4 -T 1800 inventorydb
```

In Grafana (localhost:3000), observe how the slot of the `shipment` connector retains more and more WAL, as it tracks another database (`shipmentdb`).
Stop and restart one of the public connectors to see how it falls behind the WAL and catches up again:

```bash
kcctl stop connector public-connector-2
kcctl resume connector public-connector-2
```

By registering the two `inventory` connectors, you can observe how one of them falls behind when doing changes in the `public` schema (as it doesn't emit any events),
whereas the other one regularly acknlowdges its replication slot by using Debezium heartbeats.

## Optional: Manually Inserting Data

Get a Postgres session:

```bash
docker run --tty --rm -i \
  --network postgres_replication_slots_default \
  quay.io/debezium/tooling:latest \
  bash -c 'pgcli postgresql://user:top-secret@postgres:5432/inventorydb'
```

Insert 100 records:

```sql
INSERT INTO inventory.customers (first_name, last_name, email, is_test_account)
SELECT    md5(random()::text),
          md5(random()::text),
          md5(random()::text),
          false
FROM   generate_series(1, 100) g;
```

Observe the replication slots:

```sql
SELECT
  slot_name,
  plugin,
  database,
  restart_lsn,
  CASE
    WHEN invalidation_reason IS NOT NULL THEN 'invalid'
    ELSE
      CASE
        WHEN active IS TRUE THEN 'active'
        ELSE 'inactive'
      END
    END as "status",
  pg_size_pretty(
    pg_wal_lsn_diff(
      pg_current_wal_lsn(), restart_lsn)) AS "retained_wal",
  pg_size_pretty(safe_wal_size) AS  "safe_wal_size"
FROM
  pg_replication_slots;
```
## Clean-up

Shut down all the components using Docker Compose:

```bash
docker compose down
```
