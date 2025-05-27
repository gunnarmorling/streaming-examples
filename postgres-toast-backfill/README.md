# Backfilling Postgres TOAST Fields in Debezium Events With Flink

When using Postgres default replica identity, logical replication does not expose the value of unchanged TOAST fields in update events.
Debezium will represent such values using the `__debezium_unavailable_value` constant.
With the help of Apache Flink, a stateful stream processing job can be implemented for backfilling these values from a state store,
thus always emitting complete change events downstream.
This project shows multiple ways for doing so.

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
kcctl apply -f inventory-source.json
kcctl apply -f inventory-reselect-source.json

Created connector inventory-connector
Created connector inventory-connector-with-reselect
```

Verify both connectors are running:

```bash
kcctl get connectors

 NAME                                TYPE     STATE     TASKS
 inventory-connector                 source   RUNNING   0: RUNNING
 inventory-connector-with-reselect   source   RUNNING   0: RUNNING
```

Obtain a Postgres client session:

```bash
docker run --tty --rm -i \
  --network toast-backfill-network \
  quay.io/debezium/tooling:latest \
  bash -c 'pgcli postgresql://postgres:postgres@postgres:5432/postgres'
```

Perform an update:

```sql
UPDATE inventory.authors SET dob = '1930-03-01 00:00:00' WHERE id = 1001;
```

Observe the placeholder value for the unchanged TOAST column `biography` in the corresponding Debezium event:

```bash
kcat -b localhost:9092 -t dbserver2.inventory.authors -C -q -o beginning -K "\t" -u | jq .
```

```json5
{
  "id": 1001
}
{
  "before": null,
  "after": {
    "id": 1001,
    "first_name": "Tom",
    "last_name": "Wolfe",
    # sentinel value for an unchanged TOAST column
    "biography": "__debezium_unavailable_value",
    "dob": -1257206400000000
  },
  "source": {
    "version": "3.1.0.Final",
    "connector": "postgresql",
    "name": "dbserver2",
    "ts_ms": 1746440315942,
    "snapshot": "false",
    "db": "postgres",
    "sequence": "[null,\"34415840\"]",
    "ts_us": 1746440315942179,
    "ts_ns": 1746440315942179000,
    "schema": "inventory",
    "table": "authors",
    "txId": 777,
    "lsn": 34415840,
    "xmin": null
  },
  "transaction": null,
  "op": "u",
  "ts_ms": 1746440316214,
  "ts_us": 1746440316214861,
  "ts_ns": 1746440316214861840
}
```

## Using the Debezium Column Reselect Post Processor

Debezium provides an [event post processor](https://debezium.io/documentation/reference/stable/post-processors/reselect-columns.html) which queries the database for obtaining the current value of fields with the `__debezium_unavailable_value` placeholder.

```bash
kcat -b localhost:9092 -t dbserver1.inventory.authors -C -q -o beginning -K "\t" -u | jq .
```

## Backfilling via Apache Flink

The project showcases three different approaches for backfilling TOAST columns using stateful stream processing with Flink.
Import the _toast-backfill_ Java project into your IDE.
You then can run the following jobs:

* `DataStreamJob`: Uses the DataStream API, writes to the topic _dbserver1.inventory.authors.backfilled-datastream_
* `SqlOverAggJob`: Uses Flink SQL (via the Table API) for running an `OVER` aggregation, writes to the topic _dbserver1.inventory.authors.backfilled-overagg_
* `SqlPtfJob`: Uses Flink SQL (via the Table API) for running a process table function (PTF, new in Flink 2.1), writes to the topic _dbserver1.inventory.authors.backfilled-ptf_

## Clean-up

Shut down all the components using Docker Compose:

```bash
docker compose down
```
