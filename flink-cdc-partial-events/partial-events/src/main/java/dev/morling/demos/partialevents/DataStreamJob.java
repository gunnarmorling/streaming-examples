/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.morling.demos.partialevents;

import java.util.Map;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import com.fasterxml.jackson.databind.ObjectMapper;

import dev.morling.demos.partialevents.ChangeDataEvent.Op;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>
 * For a tutorial how to write a Flink application, check the tutorials and
 * examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>
 * To package your application into a JAR file for execution, run 'mvn clean
 * package' on the command line.
 *
 * <p>
 * If you change the name of the main class (with the public static void
 * main(String[] args)) method, change the respective entry in the POM.xml file
 * (simply search for 'mainClass').
 */
public class DataStreamJob {

	public static void main(String[] args) throws Exception {
		// Sets up the execution environment, which is the main entry point
		// to building Flink applications.
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//		env.getConfig().disableGenericTypes();

		ObjectMapper mapper = new ObjectMapper();

		KafkaSource<KafkaRecord> source = KafkaSource.<KafkaRecord>builder().setBootstrapServers("localhost:9092")
				.setTopics("dbserver1.inventory.authors").setGroupId("my-group")
				.setStartingOffsets(OffsetsInitializer.earliest()).setDeserializer(new DebeziumDeserializationSchema())
//			    .setValueOnlyDeserializer(new JsonDeserializationSchema<>(KafkaRecord2.class))
//			    .setValueOnlyDeserializer(new SimpleStringSchema())
				.build();

		env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source").keyBy(k -> {
			System.out.println();
			return Long.valueOf((Integer) k.key().payload().get("id"));
		}).process(new ToastBackfillFunction()).map(m -> {
			// m = mapper.readTree(m).toPrettyString();
//				KafkaRecord event = mapper.readValue(m, KafkaRecord.class);
			return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(m.value().payload());
		}).print();

		// Execute program, beginning computation.
		env.execute("Flink Java API Skeleton");
	}

	private static class ToastBackfillFunction extends KeyedProcessFunction<Long, KafkaRecord, KafkaRecord> {

		private ValueStateDescriptor<String> descriptor;

		@Override
		public void open(OpenContext openContext) throws Exception {
			descriptor = new ValueStateDescriptor<String>("biographies", String.class);
		}

		@Override
		public void processElement(KafkaRecord in, Context ctx, Collector<KafkaRecord> out) throws Exception {
			ValueState<String> state = getRuntimeContext().getState(descriptor);
			Map<String, Object> newRowState = in.value().payload().after();
			
			switch (in.value().payload().op()) {
				case Op.READ, Op.INSERT -> state.update((String) newRowState.get("biography"));
	
				case Op.UPDATE -> {
					if ("__debezium_unavailable_value".equals(newRowState.get("biography"))) {
						newRowState.put("biography", state.value());
					} else {
						state.update((String) newRowState.get("biography"));
					}
				}
	
				case Op.DELETE -> {}
			}

			out.collect(in);
		}
	}
}
