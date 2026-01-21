/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.demos.txbuffering;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.skyscreamer.jsonassert.JSONAssert.assertEquals;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
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

/**
 * Run with --add-opens java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED.
 */
class PurchaseOrderJoinerIT {

    static Network network;
    static PostgreSQLContainer<?> postgres;
    static KafkaContainer kafka;
    static GenericContainer<?> connect;

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
                .dependsOn(kafka, postgres)
                .waitingFor(Wait.forHttp("/connectors").forPort(8083).forStatusCode(200));

        connect.start();
    }

    @AfterAll
    static void stopContainers() {
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
        registerDebeziumConnector();
        waitForConnectorRunning();

        try (TestContext ctx = createTestContext()) {
            insertPurchaseOrderWithTwoLines();

            ConsumerRecord<String, String> record = ctx.takeOne("orders_with_lines").get(60, TimeUnit.SECONDS);

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

            assertEquals(expected, record.value(), JSONCompareMode.LENIENT);

            addOrderLineAndUpdateShippingAddress();

            ConsumerRecord<String, String> updatedRecord = ctx.takeOne("orders_with_lines").get(60, TimeUnit.SECONDS);

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

            assertEquals(expectedAfterUpdate, updatedRecord.value(), JSONCompareMode.LENIENT);

            removeFirstTwoOrderLines();

            ConsumerRecord<String, String> afterDeleteRecord = ctx.takeOne("orders_with_lines").get(60, TimeUnit.SECONDS);

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

            assertEquals(expectedAfterDelete, afterDeleteRecord.value(), JSONCompareMode.LENIENT);

            ctx.assertDrained();
        }
    }

    private TestContext createTestContext() {
        return new TestContext(kafka.getBootstrapServers());
    }

    private void registerDebeziumConnector() throws Exception {
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

    private void waitForConnectorRunning() {
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
}
