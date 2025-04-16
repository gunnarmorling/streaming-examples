# Streaming Examples

This repository contains example projects and demos around data streaming (e.g. with Apache Kafka), stream processing (Apache Flink), change data capture (Debezium), open table formats (Apache Iceberg), and more.
The examples typically accompany blog posts on [morling.dev](https://morling.dev).

## Contents

| Example  | Description | Blog Post |
| ---------| ----------- | --------- |
| [debezium-kafka-flink-sql-ingest/README.md](debezium-kafka-flink-sql-ingest) | Demo of different connectors and formats for ingesting Debezium data change events into Flink SQL  | [A Deep Dive Into Ingesting Debezium Events From Kafka With Flink SQL](https://www.morling.dev/blog/ingesting-debezium-events-from-kafka-with-flink-sql/) |
| [postgres-toast-backfill/README.md](postgres-toast-backfill) | Different approaches for backfilling unchanged TOAST columns in Debezum data change events for Postgres | tbd. |

## License

This code base is available under the Apache License, version 2.
