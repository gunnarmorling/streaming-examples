/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.demos.txbuffering.join;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Serialization schema for OrderWithLines that supports tombstones (null values).
 * Extracts the order ID as the Kafka key.
 */
public class OrderWithLinesSerializationSchema implements KafkaRecordSerializationSchema<String> {

	private static final long serialVersionUID = 1L;

	private final String topic;
	private transient ObjectMapper objectMapper;

	public OrderWithLinesSerializationSchema(String topic) {
		this.topic = topic;
	}

	@Override
	public void open(SerializationSchema.InitializationContext context, KafkaSinkContext sinkContext) throws Exception {
		this.objectMapper = new ObjectMapper();
	}

	@Override
	public ProducerRecord<byte[], byte[]> serialize(String value, KafkaSinkContext context, Long timestamp) {
		if (value == null) {
			return null;
		}

		try {
			// Extract the order ID from the JSON to use as key
			JsonNode node = objectMapper.readTree(value);
			long key = node.get("id").asLong();
			byte[] keyBytes = ByteBuffer.allocate(Long.BYTES).putLong(key).array();

			// Check if this is a deleted order - emit tombstone (null value)
			JsonNode deletedNode = node.get("deleted");
			if (deletedNode != null && deletedNode.asBoolean()) {
				return new ProducerRecord<byte[], byte[]>(
					topic,
					null, // partition
					timestamp,
					keyBytes,
					null  // null value = tombstone
				);
			}

			return new ProducerRecord<>(
				topic,
				null, // partition
				timestamp,
				keyBytes,
				value.getBytes(StandardCharsets.UTF_8)
			);
		}
		catch (Exception e) {
			throw new RuntimeException("Failed to serialize order", e);
		}
	}
}
