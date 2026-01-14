/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.demos;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes.ByteArraySerde;
import org.apache.kafka.common.serialization.Serdes.WrapperSerde;
import org.apache.kafka.common.serialization.Serializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import dev.morling.demos.kroxy.validation.example.measurements.Measurement;
import dev.morling.demos.kroxy.validation.example.measurements.MeasurementKey;
import dev.morling.demos.kroxy.validation.example.measurements.Sensor;
import dev.morling.demos.kroxy.validation.example.measurements.SensorKey;
import dev.morling.demos.kroxy.validation.model.MovieV2;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientFactory;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.kroxylicious.proxy.config.ConfigParser;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.test.tester.KroxyliciousTester;
import io.kroxylicious.test.tester.KroxyliciousTesters;

@Testcontainers
public class SchemaValidationTest {

	private static final byte MAGIC_BYTE = 0x0;

    private static final String SENSORS_TOPIC = "sensors";
    private static final String MEASUREMENTS_TOPIC = "measurements";
    private static final String MOVIES_TOPIC = "movies";
    private static final String MOVIES_V2_TOPIC = "moviesV2";

    public static Network network = Network.newNetwork();

    @Container
    public static KafkaContainer kafka = new KafkaContainer("apache/kafka-native:4.0.0")
            .withNetwork(network)
            .withNetworkAliases("kafka")
            .withExposedPorts(9092);

    @Container
    public static GenericContainer<?> schemaRegistry = new GenericContainer<>(DockerImageName.parse("confluentinc/cp-schema-registry:8.0.0"))
            .withNetwork(network)
            .withNetworkAliases("schema-registry")
            .withExposedPorts(8081)
            .dependsOn(kafka)
            .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
            .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://" + kafka.getNetworkAliases().get(1) + ":9093");
    // .waitingFor(Wait.forHttp("/subjects").forStatusCode(200).withStartupTimeout(Duration.ofSeconds(5)));

    @BeforeAll
    public static void registerSchemas() throws Exception {
    	System.out.println("##### Registry: " + schemaRegistryEndpoint());

    	SchemaRegistryClient registryClient = SchemaRegistryClientFactory.newClient(schemaRegistryEndpoint(), 100, List.of(), Map.of(), Map.of());

    	registryClient.register("sensors-key",
    			new AvroSchema(new Schema.Parser().parse(SchemaValidationTest.class.getResourceAsStream("/avro/sensor-key.avsc"))));
    	registryClient.register("sensors-value",
    			new AvroSchema(new Schema.Parser().parse(SchemaValidationTest.class.getResourceAsStream("/avro/sensor-value.avsc"))));

    	registryClient.register("measurements-key",
    			new AvroSchema(new Schema.Parser().parse(SchemaValidationTest.class.getResourceAsStream("/avro/measurement-key.avsc"))));
    	registryClient.register("measurements-value",
    			new AvroSchema(new Schema.Parser().parse(SchemaValidationTest.class.getResourceAsStream("/avro/measurement-value.avsc"))));

    	registryClient.register("movies-value",
    			new JsonSchema(Files.readString(Path.of(SchemaValidationTest.class.getResource("/json/movie.schema.json").toURI()))));

    	registryClient.register("moviesV2-value",
    			new JsonSchema(Files.readString(Path.of(SchemaValidationTest.class.getResource("/json/movieV2.schema.json").toURI()))));
    }

    @Test
    public void shouldAcceptValidAvroRecord() throws Exception {
        try (KroxyliciousTester tester = getTester()) {
            MeasurementKey measurementKey = new MeasurementKey(1L);
            Measurement measurement = new Measurement("humidity", Instant.now().toEpochMilli(), 22);

            try (Producer<MeasurementKey, Measurement> producer = getMeasurementProducer(tester)) {
                producer.send(new ProducerRecord<>(MEASUREMENTS_TOPIC, measurementKey, measurement)).get();
            }

            try (KafkaConsumer<MeasurementKey, Measurement> consumer = getConsumer()) {
                ConsumerRecord<MeasurementKey, Measurement> received = consumer.poll(Duration.ofSeconds(5)).iterator().next();

                assertThat(received).isNotNull();
                assertThat(received.key()).isEqualTo(measurementKey);
                assertThat(received.value()).isEqualTo(measurement);
            }
        }
    }

    @Test
    public void shouldRejectRecordWithKeySchemaWithUnknownId() throws Exception {
        try (KroxyliciousTester tester = getTester()) {
            try (Producer<byte[], byte[]> producer = tester.producer(new ByteArraySerde(), new ByteArraySerde(), Map.of())) {
            	byte[] sensorKeyBytes = ByteBuffer.allocate(8).put(MAGIC_BYTE).putInt(42).array();

                producer.send(new ProducerRecord<>(MEASUREMENTS_TOPIC, sensorKeyBytes, null)).get();
                fail("Expected exception wasn't raised");
            }
            catch(Exception e) {
            	assertThat(e).rootCause().hasMessage("Invalid key. Reason: 42 is not a known schema id");
            }
        }
    }

    @Test
    public void shouldRejectRecordWithKeySchemaNotRegisteredForThisTopic() throws Exception {
        try (KroxyliciousTester tester = getTester()) {
            SensorKey sensorKey = new SensorKey(1L);
            Sensor sensor = new Sensor(sensorKey.getId(), "Hamburg", 9.993682, 53.551086);

            try (Producer<byte[], byte[]> producer = tester.producer(new ByteArraySerde(), new ByteArraySerde(), Map.of())) {
            	// using bytes serializer to send a "sensor" message to the "measurements" topic
            	byte[] sensorKeyBytes = this.<SensorKey>avroSerde(true).serializer().serialize(SENSORS_TOPIC, sensorKey);
            	byte[] sensorBytes = this.<Sensor>avroSerde(false).serializer().serialize(SENSORS_TOPIC, sensor);

                producer.send(new ProducerRecord<>(MEASUREMENTS_TOPIC, sensorKeyBytes, sensorBytes)).get();
                fail("Expected exception wasn't raised");
            }
            catch(Exception e) {
            	assertThat(e).rootCause().hasMessage("Invalid key. Reason: Schema id 1 invalid for subject 'measurements-key'");
            }
        }
    }

    @Test
    public void shouldRejectAvroRecordWithKeyValueNotAdheringToSchema() throws Exception {
        try (KroxyliciousTester tester = getTester()) {
            Measurement measurement = new Measurement("humidity", Instant.now().toEpochMilli(), 22);

            try (Producer<byte[], byte[]> producer = tester.producer(new ByteArraySerde(), new ByteArraySerde(), Map.of())) {
            	// using bytes serializer to send a "measurements" message whose key is the serialized value
            	byte[] measurementKeyBytes = this.<Measurement>avroSerde(false).serializer().serialize(MEASUREMENTS_TOPIC, measurement);
            	measurementKeyBytes = ByteBuffer.wrap(measurementKeyBytes).putInt(1, 3).array();
            	byte[] measurementBytes = this.<Measurement>avroSerde(false).serializer().serialize(MEASUREMENTS_TOPIC, measurement);


                producer.send(new ProducerRecord<>(MEASUREMENTS_TOPIC, measurementKeyBytes, measurementBytes)).get();
                fail("Expected exception wasn't raised");
            }
            catch(Exception e) {
            	assertThat(e).rootCause().hasMessage("Invalid key. Reason: Reached end of input while deserializing payload");
            }
        }
    }

    @Test
    public void shouldRejectJsonWithValueNotAdheringToSchema() throws Exception {
        try (KroxyliciousTester tester = getTester()) {
        	MovieV2 movie = new MovieV2(1L, "To catch a thief", "1957-11-20");

            try (Producer<byte[], byte[]> producer = tester.producer(new ByteArraySerde(), new ByteArraySerde(), Map.of())) {
            	byte[] movieKeyBytes = new LongSerializer().serialize(MOVIES_TOPIC, 1L);
            	byte[] movieBytes = this.<MovieV2>jsonSchemaSerde(false).serializer().serialize(MOVIES_V2_TOPIC, movie);
            	movieBytes[4] = 5;

                producer.send(new ProducerRecord<>(MOVIES_TOPIC, movieKeyBytes, movieBytes)).get();
                fail("Expected exception wasn't raised");
            }
            catch(Exception e) {
            	assertThat(e).rootCause().message().contains("required properties are missing: year");
            }
        }
    }



    private KroxyliciousTester getTester() {
        String config = """
                management:
                  endpoints:
                    prometheus: {}
                virtualClusters:
                  - name: demo
                    targetCluster:
                      bootstrapServers: %s
                    logNetwork: true
                    logFrames: true
                    gateways:
                    - name: default
                      portIdentifiesNode:
                        bootstrapAddress: localhost:9192
                        nodeIdRanges:
                        - name: mixed
                          start: 1
                          end: 1
                filterDefinitions:
                  - name: validation
                    type: RecordSchemaValidation
                    config:
                      schemaRegistryUrl: %s
                      rules:
                      - name: rule1
                        topics: measurements
                        keys:
                          validateId: true
                          validatePayload: true
                          schemaType: avro
                        values:
                          validateId: true
                          validatePayload: true
                          schemaType: avro
                      - name: rule2
                        topics: movies
                        values:
                          validateId: true
                          validatePayload: true
                          schemaType: json
                defaultFilters:
                  - validation
                """.formatted(kafka.getBootstrapServers(), schemaRegistryEndpoint());

        KroxyliciousTester tester = KroxyliciousTesters.kroxyliciousTester(new ConfigurationBuilder(new ConfigParser().parseConfiguration(config)));
        tester.admin().deleteTopics(List.of(MEASUREMENTS_TOPIC));
        return tester;
    }

    private Producer<MeasurementKey, Measurement> getMeasurementProducer(KroxyliciousTester tester) {
        return tester.producer(this.<MeasurementKey>avroSerde(true), this.<Measurement>avroSerde(false), Map.of());
    }

    private <T> Serde<T> avroSerde(boolean isKey) {
        @SuppressWarnings("unchecked")
		Serde<T> serde = new WrapperSerde<T>((Serializer<T>)new KafkaAvroSerializer(), (Deserializer<T>)new KafkaAvroDeserializer());
    	serde.configure(serdeConfig(), isKey);
    	return serde;
    }

    private <T> Serde<T> jsonSchemaSerde(boolean isKey) {
		Serde<T> serde = new WrapperSerde<T>(new KafkaJsonSchemaSerializer<T>(), new KafkaJsonSchemaDeserializer<T>());
    	serde.configure(serdeConfig(), isKey);
    	return serde;
    }

	private Map<String, Object> serdeConfig() {
		Map<String, Object> config = Map.of(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryEndpoint(),
                AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, "false");
		return config;
	}

    private static String schemaRegistryEndpoint() {
        return "http://%s:%s".formatted(schemaRegistry.getHost(), schemaRegistry.getFirstMappedPort());
    }

    private KafkaConsumer<MeasurementKey, Measurement> getConsumer() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());

        // properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "https://psrc-38d7km.eu-central-1.aws.confluent.cloud");

        properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://" + schemaRegistry.getHost() +
                ":" + schemaRegistry.getFirstMappedPort());

        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, Uuid.randomUuid().toString());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        properties.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, "true");

        KafkaConsumer<MeasurementKey, Measurement> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(List.of(SENSORS_TOPIC, MEASUREMENTS_TOPIC));
        return consumer;
    }
}
