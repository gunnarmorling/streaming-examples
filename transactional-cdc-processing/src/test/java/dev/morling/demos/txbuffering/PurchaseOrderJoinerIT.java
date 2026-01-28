/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.demos.txbuffering;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.json.JSONException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Run with --add-opens java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED.
 */
class PurchaseOrderJoinerIT {

    static Network network;
    static PostgreSQLContainer<?> postgres;
    static KafkaContainer kafka;
    static GenericContainer<?> connect;
    static TestContext ctx;

    static final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeAll
    static void startContainers() throws Exception {
        network = Network.newNetwork();

        postgres = new PostgreSQLContainer<>(DockerImageName.parse("quay.io/debezium/example-postgres:3.4").asCompatibleSubstituteFor("postgres"))
                .withNetwork(network)
                .withNetworkAliases("postgres")
                .withDatabaseName("postgres")
                .withUsername("postgres")
                .withPassword("postgres")
                .withCopyFileToContainer(
                        MountableFile.forHostPath("postgres/public_inventory.sql"),
                        "/docker-entrypoint-initdb.d/zzz_public_inventory.sql"
                );

        kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:4.1.0"))
                .withNetwork(network)
                .withNetworkAliases("kafka")
                .withListener("kafka:19092");

        postgres.start();
        kafka.start();

        connect = new GenericContainer<>(DockerImageName.parse("quay.io/debezium/connect:3.4"))
                .withNetwork(network)
                .withNetworkAliases("connect")
                .withExposedPorts(8083)
                .withEnv("BOOTSTRAP_SERVERS", "kafka:19092")
                .withEnv("GROUP_ID", "connect-cluster")
                .withEnv("CONFIG_STORAGE_TOPIC", "connect_configs")
                .withEnv("OFFSET_STORAGE_TOPIC", "connect_offsets")
                .withEnv("STATUS_STORAGE_TOPIC", "connect_statuses")
                .withEnv("KEY_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
                .withEnv("VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
                .withEnv("KEY_CONVERTER_SCHEMAS_ENABLE", "false")
                .withEnv("VALUE_CONVERTER_SCHEMAS_ENABLE", "false")
                // Mount empty tmpfs over unused connectors to speed up startup
                .withTmpFs(Map.ofEntries(
                        Map.entry("/kafka/connect/debezium-connector-cockroachdb", "rw"),
                        Map.entry("/kafka/connect/debezium-connector-db2", "rw"),
                        Map.entry("/kafka/connect/debezium-connector-ibmi", "rw"),
                        Map.entry("/kafka/connect/debezium-connector-informix", "rw"),
                        Map.entry("/kafka/connect/debezium-connector-jdbc", "rw"),
                        Map.entry("/kafka/connect/debezium-connector-mariadb", "rw"),
                        Map.entry("/kafka/connect/debezium-connector-mongodb", "rw"),
                        Map.entry("/kafka/connect/debezium-connector-mysql", "rw"),
                        Map.entry("/kafka/connect/debezium-connector-oracle", "rw"),
                        Map.entry("/kafka/connect/debezium-connector-spanner", "rw"),
                        Map.entry("/kafka/connect/debezium-connector-sqlserver", "rw"),
                        Map.entry("/kafka/connect/debezium-connector-vitess", "rw")
                ))
                .dependsOn(kafka, postgres)
                .waitingFor(Wait.forHttp("/connectors").forPort(8083).forStatusCode(200));

        connect.start();

        registerDebeziumConnector();
        waitForConnectorRunning();

        ctx = new TestContext(kafka.getBootstrapServers());
    }

    @AfterAll
    static void stopContainers() {
        if (ctx != null) {
            ctx.close();
        }
        if (connect != null) {
            connect.stop();
        }
        if (kafka != null) {
            kafka.stop();
        }
        if (postgres != null) {
            postgres.stop();
        }
        if (network != null) {
            network.close();
        }
    }

    @Test
    void shouldJoinOrderWithLines() throws Exception {
        insertPurchaseOrderWithTwoLines();

        ConsumerRecord<Long, String> record = ctx.takeOne("orders_with_lines").get(60, TimeUnit.SECONDS);

        String expected = """
                {
                    "id": 10001,
                    "purchaser": 1001,
                    "shippingAddress": "123 Main St",
                    "lines": [
                        {"productId": 101, "quantity": 2, "price": 19.99},
                        {"productId": 102, "quantity": 1, "price": 49.99}
                    ]
                }
                """;

        assertThat(record.key()).isEqualTo(10001L);
        assertJsonEquals(expected, record.value());

        addOrderLineAndUpdateShippingAddress();

        ConsumerRecord<Long, String> updatedRecord = ctx.takeOne("orders_with_lines").get(60, TimeUnit.SECONDS);

        String expectedAfterUpdate = """
                {
                    "id": 10001,
                    "purchaser": 1001,
                    "shippingAddress": "456 Oak Ave",
                    "lines": [
                        {"productId": 101, "quantity": 2, "price": 19.99},
                        {"productId": 102, "quantity": 1, "price": 49.99},
                        {"productId": 103, "quantity": 3, "price": 29.99}
                    ]
                }
                """;

        assertThat(updatedRecord.key()).isEqualTo(10001L);
        assertJsonEquals(expectedAfterUpdate, updatedRecord.value());

        removeFirstTwoOrderLines();

        ConsumerRecord<Long, String> afterDeleteRecord = ctx.takeOne("orders_with_lines").get(60, TimeUnit.SECONDS);

        String expectedAfterDelete = """
                {
                    "id": 10001,
                    "purchaser": 1001,
                    "shippingAddress": "456 Oak Ave",
                    "lines": [
                        {"productId": 103, "quantity": 3, "price": 29.99}
                    ]
                }
                """;

        assertThat(afterDeleteRecord.key()).isEqualTo(10001L);
        assertJsonEquals(expectedAfterDelete, afterDeleteRecord.value());

        ctx.assertDrained();
    }

    @Test
    void shouldHandleInterleavedTransactions() throws Exception {
        String jdbcUrl = postgres.getJdbcUrl();

        // BEGIN A
        Connection connA = DriverManager.getConnection(jdbcUrl, "postgres", "postgres");
        connA.setAutoCommit(false);
        Statement stmtA = connA.createStatement();
        stmtA.execute("SET search_path TO inventory");
        ResultSet rsA = stmtA.executeQuery("INSERT INTO orders (order_date, purchaser, shipping_address) " +
                "VALUES (CURRENT_DATE, 1001, 'Address A') RETURNING id");
        rsA.next();
        int orderIdA = rsA.getInt(1);
        stmtA.execute(String.format("INSERT INTO order_lines (order_id, product_id, quantity, price) VALUES (%d, 101, 1, 10.00)", orderIdA));
        stmtA.execute(String.format("INSERT INTO order_lines (order_id, product_id, quantity, price) VALUES (%d, 102, 2, 20.00)", orderIdA));
        stmtA.execute(String.format("INSERT INTO order_lines (order_id, product_id, quantity, price) VALUES (%d, 103, 3, 30.00)", orderIdA));

        // BEGIN B
        Connection connB = DriverManager.getConnection(jdbcUrl, "postgres", "postgres");
        connB.setAutoCommit(false);
        Statement stmtB = connB.createStatement();
        stmtB.execute("SET search_path TO inventory");
        ResultSet rsB = stmtB.executeQuery("INSERT INTO orders (order_date, purchaser, shipping_address) " +
                "VALUES (CURRENT_DATE, 1002, 'Address B') RETURNING id");
        rsB.next();
        int orderIdB = rsB.getInt(1);
        stmtB.execute(String.format("INSERT INTO order_lines (order_id, product_id, quantity, price) VALUES (%d, 104, 4, 40.00)", orderIdB));
        stmtB.execute(String.format("INSERT INTO order_lines (order_id, product_id, quantity, price) VALUES (%d, 105, 5, 50.00)", orderIdB));
        stmtB.execute(String.format("INSERT INTO order_lines (order_id, product_id, quantity, price) VALUES (%d, 106, 6, 60.00)", orderIdB));

        // COMMIT B
        connB.commit();
        connB.close();

        // COMMIT A
        connA.commit();
        connA.close();

        // B committed first, so it should be output first
        ConsumerRecord<Long, String> recordB = ctx.takeOne("orders_with_lines").get(60, TimeUnit.SECONDS);

        String expectedB = """
                {
                    "id": %d,
                    "purchaser": 1002,
                    "shippingAddress": "Address B",
                    "lines": [
                        {"productId": 104, "quantity": 4, "price": 40.00},
                        {"productId": 105, "quantity": 5, "price": 50.00},
                        {"productId": 106, "quantity": 6, "price": 60.00}
                    ]
                }
                """.formatted(orderIdB);

        assertThat(recordB.key()).isEqualTo((long) orderIdB);
        assertJsonEquals(expectedB, recordB.value());

        // A committed second, so it should be output second
        ConsumerRecord<Long, String> recordA = ctx.takeOne("orders_with_lines").get(60, TimeUnit.SECONDS);

        String expectedA = """
                {
                    "id": %d,
                    "purchaser": 1001,
                    "shippingAddress": "Address A",
                    "lines": [
                        {"productId": 101, "quantity": 1, "price": 10.00},
                        {"productId": 102, "quantity": 2, "price": 20.00},
                        {"productId": 103, "quantity": 3, "price": 30.00}
                    ]
                }
                """.formatted(orderIdA);

        assertThat(recordA.key()).isEqualTo((long) orderIdA);
        assertJsonEquals(expectedA, recordA.value());

        ctx.assertDrained();
    }

    @Test
    void shouldAccumulateLinesAcrossMultipleTransactions() throws Exception {
        String jdbcUrl = postgres.getJdbcUrl();

        // TX1: Create order with 3 lines
        int orderId;
        try (Connection conn = DriverManager.getConnection(jdbcUrl, "postgres", "postgres");
             Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(false);
            stmt.execute("SET search_path TO inventory");
            ResultSet rs = stmt.executeQuery("INSERT INTO orders (order_date, purchaser, shipping_address) " +
                    "VALUES (CURRENT_DATE, 1003, 'Multi-TX Address') RETURNING id");
            rs.next();
            orderId = rs.getInt(1);
            stmt.execute(String.format("INSERT INTO order_lines (order_id, product_id, quantity, price) VALUES (%d, 101, 1, 10.00)", orderId));
            stmt.execute(String.format("INSERT INTO order_lines (order_id, product_id, quantity, price) VALUES (%d, 102, 1, 20.00)", orderId));
            stmt.execute(String.format("INSERT INTO order_lines (order_id, product_id, quantity, price) VALUES (%d, 103, 1, 30.00)", orderId));
            conn.commit();
        }

        // TX2: Update the order's shipping address
        try (Connection conn = DriverManager.getConnection(jdbcUrl, "postgres", "postgres");
             Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(false);
            stmt.execute("SET search_path TO inventory");
            stmt.execute(String.format("UPDATE orders SET shipping_address = 'Updated Multi-TX Address' WHERE id = %d", orderId));
            conn.commit();
        }

        // TX3: Add 5 more lines
        try (Connection conn = DriverManager.getConnection(jdbcUrl, "postgres", "postgres");
             Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(false);
            stmt.execute("SET search_path TO inventory");
            stmt.execute(String.format("INSERT INTO order_lines (order_id, product_id, quantity, price) VALUES (%d, 104, 1, 40.00)", orderId));
            stmt.execute(String.format("INSERT INTO order_lines (order_id, product_id, quantity, price) VALUES (%d, 105, 1, 50.00)", orderId));
            stmt.execute(String.format("INSERT INTO order_lines (order_id, product_id, quantity, price) VALUES (%d, 106, 1, 60.00)", orderId));
            stmt.execute(String.format("INSERT INTO order_lines (order_id, product_id, quantity, price) VALUES (%d, 107, 1, 70.00)", orderId));
            stmt.execute(String.format("INSERT INTO order_lines (order_id, product_id, quantity, price) VALUES (%d, 108, 1, 80.00)", orderId));
            conn.commit();
        }

        // TX4: Add 5 more lines (reuse product IDs with different quantities)
        try (Connection conn = DriverManager.getConnection(jdbcUrl, "postgres", "postgres");
             Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(false);
            stmt.execute("SET search_path TO inventory");
            stmt.execute(String.format("INSERT INTO order_lines (order_id, product_id, quantity, price) VALUES (%d, 109, 1, 90.00)", orderId));
            stmt.execute(String.format("INSERT INTO order_lines (order_id, product_id, quantity, price) VALUES (%d, 101, 2, 100.00)", orderId));
            stmt.execute(String.format("INSERT INTO order_lines (order_id, product_id, quantity, price) VALUES (%d, 102, 2, 110.00)", orderId));
            stmt.execute(String.format("INSERT INTO order_lines (order_id, product_id, quantity, price) VALUES (%d, 103, 2, 120.00)", orderId));
            stmt.execute(String.format("INSERT INTO order_lines (order_id, product_id, quantity, price) VALUES (%d, 104, 2, 130.00)", orderId));
            conn.commit();
        }

        // TX5: Add 5 more lines (reuse product IDs with different quantities)
        try (Connection conn = DriverManager.getConnection(jdbcUrl, "postgres", "postgres");
             Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(false);
            stmt.execute("SET search_path TO inventory");
            stmt.execute(String.format("INSERT INTO order_lines (order_id, product_id, quantity, price) VALUES (%d, 105, 2, 140.00)", orderId));
            stmt.execute(String.format("INSERT INTO order_lines (order_id, product_id, quantity, price) VALUES (%d, 106, 2, 150.00)", orderId));
            stmt.execute(String.format("INSERT INTO order_lines (order_id, product_id, quantity, price) VALUES (%d, 107, 2, 160.00)", orderId));
            stmt.execute(String.format("INSERT INTO order_lines (order_id, product_id, quantity, price) VALUES (%d, 108, 2, 170.00)", orderId));
            stmt.execute(String.format("INSERT INTO order_lines (order_id, product_id, quantity, price) VALUES (%d, 109, 2, 180.00)", orderId));
            conn.commit();
        }

        // Fetch and assert all 5 events
        ConsumerRecord<Long, String> record1 = ctx.takeOne("orders_with_lines").get(60, TimeUnit.SECONDS);
        String expected1 = """
                {
                    "id": %d,
                    "purchaser": 1003,
                    "shippingAddress": "Multi-TX Address",
                    "lines": [
                        {"productId": 101, "quantity": 1, "price": 10.00},
                        {"productId": 102, "quantity": 1, "price": 20.00},
                        {"productId": 103, "quantity": 1, "price": 30.00}
                    ]
                }
                """.formatted(orderId);
        assertThat(record1.key()).isEqualTo((long) orderId);
        assertJsonEquals(expected1, record1.value());

        ConsumerRecord<Long, String> record2 = ctx.takeOne("orders_with_lines").get(60, TimeUnit.SECONDS);
        String expected2 = """
                {
                    "id": %d,
                    "purchaser": 1003,
                    "shippingAddress": "Updated Multi-TX Address",
                    "lines": [
                        {"productId": 101, "quantity": 1, "price": 10.00},
                        {"productId": 102, "quantity": 1, "price": 20.00},
                        {"productId": 103, "quantity": 1, "price": 30.00}
                    ]
                }
                """.formatted(orderId);
        assertThat(record2.key()).isEqualTo((long) orderId);
        assertJsonEquals(expected2, record2.value());

        ConsumerRecord<Long, String> record3 = ctx.takeOne("orders_with_lines").get(60, TimeUnit.SECONDS);
        String expected3 = """
                {
                    "id": %d,
                    "purchaser": 1003,
                    "shippingAddress": "Updated Multi-TX Address",
                    "lines": [
                        {"productId": 101, "quantity": 1, "price": 10.00},
                        {"productId": 102, "quantity": 1, "price": 20.00},
                        {"productId": 103, "quantity": 1, "price": 30.00},
                        {"productId": 104, "quantity": 1, "price": 40.00},
                        {"productId": 105, "quantity": 1, "price": 50.00},
                        {"productId": 106, "quantity": 1, "price": 60.00},
                        {"productId": 107, "quantity": 1, "price": 70.00},
                        {"productId": 108, "quantity": 1, "price": 80.00}
                    ]
                }
                """.formatted(orderId);
        assertThat(record3.key()).isEqualTo((long) orderId);
        assertJsonEquals(expected3, record3.value());

        ConsumerRecord<Long, String> record4 = ctx.takeOne("orders_with_lines").get(60, TimeUnit.SECONDS);
        String expected4 = """
                {
                    "id": %d,
                    "purchaser": 1003,
                    "shippingAddress": "Updated Multi-TX Address",
                    "lines": [
                        {"productId": 101, "quantity": 1, "price": 10.00},
                        {"productId": 102, "quantity": 1, "price": 20.00},
                        {"productId": 103, "quantity": 1, "price": 30.00},
                        {"productId": 104, "quantity": 1, "price": 40.00},
                        {"productId": 105, "quantity": 1, "price": 50.00},
                        {"productId": 106, "quantity": 1, "price": 60.00},
                        {"productId": 107, "quantity": 1, "price": 70.00},
                        {"productId": 108, "quantity": 1, "price": 80.00},
                        {"productId": 109, "quantity": 1, "price": 90.00},
                        {"productId": 101, "quantity": 2, "price": 100.00},
                        {"productId": 102, "quantity": 2, "price": 110.00},
                        {"productId": 103, "quantity": 2, "price": 120.00},
                        {"productId": 104, "quantity": 2, "price": 130.00}
                    ]
                }
                """.formatted(orderId);
        assertThat(record4.key()).isEqualTo((long) orderId);
        assertJsonEquals(expected4, record4.value());

        ConsumerRecord<Long, String> record5 = ctx.takeOne("orders_with_lines").get(60, TimeUnit.SECONDS);
        String expected5 = """
                {
                    "id": %d,
                    "purchaser": 1003,
                    "shippingAddress": "Updated Multi-TX Address",
                    "lines": [
                        {"productId": 101, "quantity": 1, "price": 10.00},
                        {"productId": 102, "quantity": 1, "price": 20.00},
                        {"productId": 103, "quantity": 1, "price": 30.00},
                        {"productId": 104, "quantity": 1, "price": 40.00},
                        {"productId": 105, "quantity": 1, "price": 50.00},
                        {"productId": 106, "quantity": 1, "price": 60.00},
                        {"productId": 107, "quantity": 1, "price": 70.00},
                        {"productId": 108, "quantity": 1, "price": 80.00},
                        {"productId": 109, "quantity": 1, "price": 90.00},
                        {"productId": 101, "quantity": 2, "price": 100.00},
                        {"productId": 102, "quantity": 2, "price": 110.00},
                        {"productId": 103, "quantity": 2, "price": 120.00},
                        {"productId": 104, "quantity": 2, "price": 130.00},
                        {"productId": 105, "quantity": 2, "price": 140.00},
                        {"productId": 106, "quantity": 2, "price": 150.00},
                        {"productId": 107, "quantity": 2, "price": 160.00},
                        {"productId": 108, "quantity": 2, "price": 170.00},
                        {"productId": 109, "quantity": 2, "price": 180.00}
                    ]
                }
                """.formatted(orderId);
        assertThat(record5.key()).isEqualTo((long) orderId);
        assertJsonEquals(expected5, record5.value());

        ctx.assertDrained();
    }

    @Test
    void shouldHandleDeleteOfOrderWithLines() throws Exception {
        String jdbcUrl = postgres.getJdbcUrl();

        // TX1: Create order with 2 lines
        int orderId;
        int lineId1;
        int lineId2;
        try (Connection conn = DriverManager.getConnection(jdbcUrl, "postgres", "postgres");
             Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(false);
            stmt.execute("SET search_path TO inventory");
            ResultSet rsOrder = stmt.executeQuery("INSERT INTO orders (order_date, purchaser, shipping_address) " +
                    "VALUES (CURRENT_DATE, 1004, 'Delete Test Address') RETURNING id");
            rsOrder.next();
            orderId = rsOrder.getInt(1);

            ResultSet rsLine1 = stmt.executeQuery(String.format(
                    "INSERT INTO order_lines (order_id, product_id, quantity, price) VALUES (%d, 101, 1, 15.00) RETURNING id", orderId));
            rsLine1.next();
            lineId1 = rsLine1.getInt(1);

            ResultSet rsLine2 = stmt.executeQuery(String.format(
                    "INSERT INTO order_lines (order_id, product_id, quantity, price) VALUES (%d, 102, 2, 25.00) RETURNING id", orderId));
            rsLine2.next();
            lineId2 = rsLine2.getInt(1);

            conn.commit();
        }

        // Verify TX1 output
        ConsumerRecord<Long, String> record1 = ctx.takeOne("orders_with_lines").get(60, TimeUnit.SECONDS);
        String expected1 = """
                {
                    "id": %d,
                    "purchaser": 1004,
                    "shippingAddress": "Delete Test Address",
                    "lines": [
                        {"productId": 101, "quantity": 1, "price": 15.00},
                        {"productId": 102, "quantity": 2, "price": 25.00}
                    ]
                }
                """.formatted(orderId);
        assertThat(record1.key()).isEqualTo((long) orderId);
        assertJsonEquals(expected1, record1.value());

        // TX2: Delete order lines first (FK constraint), then delete the order
        try (Connection conn = DriverManager.getConnection(jdbcUrl, "postgres", "postgres");
             Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(false);
            stmt.execute("SET search_path TO inventory");
            stmt.execute(String.format("DELETE FROM order_lines WHERE id IN (%d, %d)", lineId1, lineId2));
            stmt.execute(String.format("DELETE FROM orders WHERE id = %d", orderId));
            conn.commit();
        }

        // Verify TX2 output - tombstone (null value) for deleted order
        ConsumerRecord<Long, String> record2 = ctx.takeOne("orders_with_lines").get(60, TimeUnit.SECONDS);
        assertThat(record2.key()).as("Tombstone should have the order ID as key").isEqualTo((long) orderId);
        assertThat(record2.value()).as("Deleted order should emit tombstone (null value)").isNull();

        ctx.assertDrained();
    }

    private void insertPurchaseOrderWithTwoLines() throws Exception {
        String jdbcUrl = postgres.getJdbcUrl();

        try (Connection conn = DriverManager.getConnection(jdbcUrl, "postgres", "postgres");
             Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(false);

            stmt.execute("SET search_path TO inventory");
            stmt.execute("INSERT INTO orders (order_date, purchaser, shipping_address) " +
                    "VALUES (CURRENT_DATE, 1001, '123 Main St')");
            stmt.execute("INSERT INTO order_lines (order_id, product_id, quantity, price) " +
                    "VALUES (10001, 101, 2, 19.99)");
            stmt.execute("INSERT INTO order_lines (order_id, product_id, quantity, price) " +
                    "VALUES (10001, 102, 1, 49.99)");

            conn.commit();
        }
    }

    private void addOrderLineAndUpdateShippingAddress() throws Exception {
        String jdbcUrl = postgres.getJdbcUrl();

        try (Connection conn = DriverManager.getConnection(jdbcUrl, "postgres", "postgres");
             Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(false);

            stmt.execute("SET search_path TO inventory");
            stmt.execute("UPDATE orders SET shipping_address = '456 Oak Ave' WHERE id = 10001");
            stmt.execute("INSERT INTO order_lines (order_id, product_id, quantity, price) " +
                    "VALUES (10001, 103, 3, 29.99)");

            conn.commit();
        }
    }

    private void removeFirstTwoOrderLines() throws Exception {
        String jdbcUrl = postgres.getJdbcUrl();

        try (Connection conn = DriverManager.getConnection(jdbcUrl, "postgres", "postgres");
             Statement stmt = conn.createStatement()) {
            conn.setAutoCommit(false);

            stmt.execute("SET search_path TO inventory");
            stmt.execute("DELETE FROM order_lines WHERE order_id = 10001 AND product_id IN (101, 102)");

            conn.commit();
        }
    }

    private static void assertJsonEquals(String expected, String actual) {
        try {
            JSONAssert.assertEquals(expected, actual, JSONCompareMode.LENIENT);
        }
        catch (JSONException | AssertionError e) {
            throw new AssertionError("JSON comparison failed.\nExpected: " + expected + "\nActual: " + actual, e);
        }
    }

    private static void registerDebeziumConnector() throws Exception {
        String connectorConfig = Files.readString(Path.of("postgres-connector.json"));

        String connectUrl = "http://" + connect.getHost() + ":" + connect.getMappedPort(8083);

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(connectUrl + "/connectors"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(connectorConfig))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).as("Connector registration should succeed: " + response.body()).isIn(201, 409);
    }

    private static void waitForConnectorRunning() {
        String connectUrl = "http://" + connect.getHost() + ":" + connect.getMappedPort(8083);
        HttpClient client = HttpClient.newHttpClient();

        await()
                .atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofSeconds(2))
                .untilAsserted(() -> {
                    HttpRequest request = HttpRequest.newBuilder()
                            .uri(URI.create(connectUrl + "/connectors/inventory-connector/status"))
                            .GET()
                            .build();

                    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                    assertThat(response.statusCode()).isEqualTo(200);

                    JsonNode status = objectMapper.readTree(response.body());
                    String connectorState = status.path("connector").path("state").asText();
                    assertThat(connectorState).isEqualTo("RUNNING");

                    JsonNode tasks = status.path("tasks");
                    assertThat(tasks.isArray()).isTrue();
                    assertThat(tasks.size()).isGreaterThan(0);
                    String taskState = tasks.get(0).path("state").asText();
                    assertThat(taskState).isEqualTo("RUNNING");
                });
    }
}
