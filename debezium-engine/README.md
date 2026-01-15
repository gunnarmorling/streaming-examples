# Using the Debezium Embedded Engine

An example demonstrating how to use Debezium, together with its connector for Postgres, embedded into a Java application.

## Prerequisites

Make sure to have the following software installed on your machine:

* Docker and Docker Compose
* Apache Maven
* Java 21+
* kcctl 🐻 (optional)

## Preparation

Start the Postgres database:

```bash
docker compose up
```
## Running the Example

Run the example like so:

```bash
./mvnw compile exec:java
```

This will take a snapshot of the `authors` and `books` tables,
printing the data change events in JSON format on stdout.

To do some data changes, you can get a psql shell like so:

```shell
docker exec -it debezium-engine-postgres-1 psql -U postgres
```

Then insert a record:

```sql
INSERT INTO inventory.books_00004 (title, author_id, isbn, published_date, price, in_stock
 , page_count, description, metadata) VALUES
   ('The Right Stuff', 1001, '9780312427566', '1979-01-01', 15.99, TRUE, 352, 'An account of the pilots who became th
 e first Project Mercury astronauts.', '{"genre": "non-fiction", "awards": ["National Book Award"], "language": "en",
  "film_adaptation": 1983}');
```

## Docker Compose

For testing purposes, the Compose file also contains a Kafka node and a Kafka Connect node (commented out by default).
When using these services, register a connector for the database like so:

```bash
kcctl apply -f postgres-connector.json
```

Receive the change events for the `authors` table:

```bash
docker-compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
     --bootstrap-server kafka:9092 --from-beginning=true \
     --property print.key=true \
     --topic dbserver1.inventory.authors
```

## Clean-up

Hit `Ctrl + C` to stop the example application.

Shut down the database using Docker Compose:

```bash
docker compose down
```