/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright Gunnar Morling
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.demos.partialevents;

import java.util.Map;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import dev.morling.demos.partialevents.datastream.DebeziumDeserializationSchema;
import dev.morling.demos.partialevents.datastream.DebeziumSerializationSchema;
import dev.morling.demos.partialevents.datastream.KafkaRecord;

/**
 * Backfills the "biography" TOAST column in Debezium update events using a state store.
 */
public class DataStreamJob {

	private static final String KAFKA_BROKER = "localhost:9092";

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		KafkaSource<KafkaRecord> source = KafkaSource.<KafkaRecord>builder().setBootstrapServers(KAFKA_BROKER)
				.setTopics("dbserver2.inventory.authors").setGroupId("my-group")
				.setStartingOffsets(OffsetsInitializer.earliest())
				.setDeserializer(new DebeziumDeserializationSchema())
				.build();

		KafkaSink<KafkaRecord> sink = KafkaSink.<KafkaRecord>builder()
				.setBootstrapServers(KAFKA_BROKER)
				.setRecordSerializer(new DebeziumSerializationSchema("dbserver2.inventory.authors.backfilled-datastream"))
				.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
				.build();

		env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
			.keyBy(k -> {
				return Long.valueOf((Integer) k.key().get("id"));
			})
			.process(new ToastBackfillFunction("biography"))
			.sinkTo(sink);

		env.execute("Flink TOAST Backfill");
	}

	private static class ToastBackfillFunction extends KeyedProcessFunction<Long, KafkaRecord, KafkaRecord> {

		private static final String UNCHANGED_TOAST_VALUE = "__debezium_unavailable_value";

		private final String fieldName;
		private ValueStateDescriptor<String> descriptor;

		public ToastBackfillFunction(String toastFieldName) {
			this.fieldName = toastFieldName;
		}

		@Override
		public void open(OpenContext openContext) throws Exception {
			descriptor = new ValueStateDescriptor<String>(fieldName, String.class);
		}

		@Override
		public void processElement(KafkaRecord in, Context ctx, Collector<KafkaRecord> out) throws Exception {
			ValueState<String> state = getRuntimeContext().getState(descriptor);

			@SuppressWarnings("unchecked")
			Map<String, Object> newRowState = (Map<String, Object>) in.value().get("after");

			switch ((String)in.value().get("op")) {
				case "r", "i" -> state.update((String) newRowState.get(fieldName));

				case "u" -> {
					if (UNCHANGED_TOAST_VALUE.equals(newRowState.get(fieldName))) {
						newRowState.put(fieldName, state.value());
					} else {
						state.update((String) newRowState.get(fieldName));
					}
				}

				case "d" -> {
					state.clear();
				}
			}

			out.collect(in);
		}
	}
}
