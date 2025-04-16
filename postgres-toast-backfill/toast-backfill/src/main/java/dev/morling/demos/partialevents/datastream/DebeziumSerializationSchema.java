/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright Gunnar Morling
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.demos.partialevents.datastream;

import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DebeziumSerializationSchema implements KafkaRecordSerializationSchema<KafkaRecord> {

	private final String topic;
	private final ObjectMapper mapper;

	public DebeziumSerializationSchema(String topic) {
		this.topic = topic;
		mapper = new ObjectMapper();
	}

	@Override
	public ProducerRecord<byte[], byte[]> serialize(KafkaRecord element, KafkaSinkContext context, Long timestamp) {
		try {
			return new ProducerRecord<byte[], byte[]>(topic, mapper.writeValueAsBytes(element.key()), mapper.writeValueAsBytes(element.value()));
		} catch (JsonProcessingException e) {
			throw new IllegalArgumentException("Couldn't serialize record", e);
		}
	}
}
