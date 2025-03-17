# A Deep Dive Into Ingesting Debezium Events From Kafka With Flink SQL

This demo shows different ways for ingesting Debezium data change events into Flink SQL.
It accompanies the blog post [A Deep Dive Into Ingesting Debezium Events From Kafka With Flink SQL](https://www.morling.dev/blog/ingesting-debezium-events-from-kafka-with-flink-sql/).

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
kcctl apply -f inventory-source-full.json
kcctl apply -f inventory-source-flat.json
```

Verify both connectors are running:

```bash
kcctl get connectors

 NAME                       TYPE     STATE     TASKS
 inventory-connector-flat   source   RUNNING   0: RUNNING
 inventory-connector-full   source   RUNNING   0: RUNNING
```

Obtain a Postgres client session:

```bash
docker run --tty --rm -i \
  --network partial-events-network \
  quay.io/debezium/tooling:latest \
  bash -c 'pgcli postgresql://postgres:postgres@postgres:5432/postgres'
```

## Running the Flink Jobs

Import the _debezium-ingest-job_ Java project into your IDE.
You then can run the following jobs:

* `KafkaAppendStreamJob`: processes full Debezium events in append-only mode, reading from `dbserver1.inventory.authors`.
* `KafkaChangelogJob`: processes full Debezium events in changelog mode, reading from `dbserver1.inventory.authors`.
* `KafkaUpsertJob`: processes flat events in upsert mode, reading from `dbserver2.inventory.authors`.
* `KafkaChangelogToUpsertJob`: processes full Debezium events in changelog mode and emits them as flat events in upsert mode, reading from `dbserver1.inventory.authors`.

All jobs write to the same output topic, which you can examine like so:

```bash
kcat -b localhost:9092 -t authors_processed -C -q -o beginning -K "\t" -u | jq .
```

Perform some data modifications in SQL and observe the corresponding events on the output topic:

```sql
SET search_path TO 'inventory';

INSERT INTO authors (first_name, last_name, biography, registered) VALUES ('Sarah', 'Hayes', 'AcbZdqauh', '2025-03-10 21:36:40');

UPDATE authors SET last_name = 'Williams' WHERE id = 1002;

DELETE FROM authors WHERE id = 1002;
```

## Clean-up

Shut down all the components using Docker Compose:

```bash
docker compose down
```
