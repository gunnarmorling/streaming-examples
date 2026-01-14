/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.demos;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dev.morling.demos.measurements.Measurement;
import dev.morling.demos.measurements.MeasurementKey;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.schema.id.HeaderSchemaIdSerializer;

public class MeasurementsProducer {

    private static final Logger log = LoggerFactory.getLogger(MeasurementsProducer.class);
    private static final String TOPIC = "measurements";

    @Test
    public void möp() throws InterruptedException, ExecutionException {
        // public static void main(String[] args) throws Exception {
        log.info("I am a Kafka Producer");

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, System.getenv("BOOTSTRAP_SERVERS"));
        properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        // properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        properties.setProperty(SaslConfigs.SASL_MECHANISM, "PLAIN");
        String username = System.getenv("USER_NAME");
        String password = System.getenv("PASSWORD");
        properties.put(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + username + "\" password=\"" + password + "\";");

        properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "https://psrc-38d7km.eu-central-1.aws.confluent.cloud");
        properties.put(AbstractKafkaSchemaSerDeConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO");
        properties.put(AbstractKafkaSchemaSerDeConfig.USER_INFO_CONFIG, "P42JVOVGFH6RGXM5:F3hh2r2b1AcTqOtkFCrWoU26UTLUnq+Ho5f+GfL8gCXjfqOPK/vr+lNQ5R/96hFs");
        properties.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, "false");
        properties.setProperty(AbstractKafkaSchemaSerDeConfig.VALUE_SCHEMA_ID_SERIALIZER, HeaderSchemaIdSerializer.class.getName());

        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

        try (KafkaProducer<MeasurementKey, Measurement> producer = new KafkaProducer<>(properties)) {
            final ProducerRecord<MeasurementKey, Measurement> record = new ProducerRecord<>(
                    TOPIC,
                    new MeasurementKey("hamburg-1"),
                    new Measurement("humidity", Instant.now().toEpochMilli(), 22));

            producer.send(record).get();
            System.out.printf("Successfully produced 1 message to a topic called %s%n", TOPIC);
        }
    }
}
