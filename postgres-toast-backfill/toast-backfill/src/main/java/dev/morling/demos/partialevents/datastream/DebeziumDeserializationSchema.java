/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright Gunnar Morling
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.demos.partialevents.datastream;

import java.io.IOException;
import java.util.Map;

import org.apache.flink.api.common.serialization.DeserializationSchema.InitializationContext;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DebeziumDeserializationSchema implements KafkaRecordDeserializationSchema<KafkaRecord> {

	private transient ObjectMapper mapper;

	@Override
	public TypeInformation<KafkaRecord> getProducedType() {
		return BasicTypeInfo.of(KafkaRecord.class);
	}

	@Override
	public void open(InitializationContext context) throws Exception {
		mapper = new ObjectMapper();
	}

	@Override
	public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<KafkaRecord> out) throws IOException {
		//out.collect(new KafkaRecord(mapper.readTree(record.key())., mapper.readTree(record.value())));
		out.collect(new KafkaRecord(mapper.readValue(record.key(), new TypeReference<Map<String, Object>>(){}), mapper.readValue(record.value(), new TypeReference<Map<String, Object>>(){})));
	}
}
