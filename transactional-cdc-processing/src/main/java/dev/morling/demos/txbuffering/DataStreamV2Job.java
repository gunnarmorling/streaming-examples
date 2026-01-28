/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.demos.txbuffering;

import java.util.Collections;
import java.util.HashSet;

import org.apache.flink.api.common.state.StateDeclaration;
import org.apache.flink.api.common.state.v2.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.dsv2.DataStreamV2SinkUtils;
import org.apache.flink.api.connector.dsv2.DataStreamV2SourceUtils;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.datastream.api.ExecutionEnvironment;
import org.apache.flink.datastream.api.extension.join.JoinType;
import org.apache.flink.datastream.api.function.TwoInputNonBroadcastStreamProcessFunction;
import org.apache.flink.datastream.api.stream.BroadcastStream;
import org.apache.flink.datastream.api.stream.KeyedPartitionStream;
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream;
import org.apache.flink.datastream.impl.ExecutionEnvironmentImpl;
import org.apache.flink.datastream.impl.attribute.AttributeParser;
import org.apache.flink.datastream.impl.extension.join.operators.TwoInputNonBroadcastJoinProcessFunction;
import org.apache.flink.datastream.impl.extension.window.function.InternalTwoInputWindowStreamProcessFunction;
import org.apache.flink.datastream.impl.operators.KeyedTwoInputNonBroadcastProcessOperator;
import org.apache.flink.datastream.impl.stream.KeyedPartitionStreamImpl;
import org.apache.flink.datastream.impl.stream.NonKeyedPartitionStreamImpl;
import org.apache.flink.datastream.impl.utils.StreamUtils;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.kafka.common.Uuid;

import dev.morling.demos.txbuffering.join.CommitLsnFixer;
import dev.morling.demos.txbuffering.join.DataChangeEventJoinFunction;
import dev.morling.demos.txbuffering.join.NonPostCommitTopologySupportingKafkaSink;
import dev.morling.demos.txbuffering.join.OrderWithLinesSerializationSchema;
import dev.morling.demos.txbuffering.join.TxAwareAggregationFunction;
import dev.morling.demos.txbuffering.join.TxAwareJoinProcessFunction;
import dev.morling.demos.txbuffering.join.TxAwareTwoInputNonBroadcastJoinProcessOperator;
import dev.morling.demos.txbuffering.join.WatermarkInjector;
import dev.morling.demos.txbuffering.model.generic.DataChangeEvent;
import dev.morling.demos.txbuffering.model.generic.DataChangeEventPair;
import dev.morling.demos.txbuffering.model.generic.TransactionEvent;

import static org.apache.flink.datastream.impl.utils.StreamUtils.validateStates;

public class DataStreamV2Job {

	public static void main(String[] args) throws Exception {
		run("localhost:9092");
	}

	public static void run(String bootstrapServers) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getInstance();
		((ExecutionEnvironmentImpl)env).getConfiguration().set(RestOptions.PORT, 8081);
		((ExecutionEnvironmentImpl)env).getConfiguration().setString("state.backend.async", "false");
		((ExecutionEnvironmentImpl)env).getConfiguration().setString("state.backend", "hashmap");

		JsonDeserializationSchema<DataChangeEvent> jsonFormat=new JsonDeserializationSchema<>(DataChangeEvent.class);
		JsonDeserializationSchema<TransactionEvent> txFormat=new JsonDeserializationSchema<>(TransactionEvent.class);

		KafkaSource<DataChangeEvent> ordersSource = KafkaSource.<DataChangeEvent>builder()
		        .setBootstrapServers(bootstrapServers)
		        .setTopics("dbserver1.inventory.orders")
		        .setGroupId(Uuid.randomUuid().toString())
		        .setStartingOffsets(OffsetsInitializer.earliest())
		        .setValueOnlyDeserializer(jsonFormat)
		        .setProperty("acks", "all")
		        .build();

		// todo: explore watermark aligner

		KafkaSource<DataChangeEvent> orderLineSource = KafkaSource.<DataChangeEvent>builder()
		        .setBootstrapServers(bootstrapServers)
		        .setTopics("dbserver1.inventory.order_lines")
		        .setGroupId(Uuid.randomUuid().toString())
		        .setStartingOffsets(OffsetsInitializer.earliest())
		        .setValueOnlyDeserializer(jsonFormat)
		        .setProperty("acks", "all")
		        .build();

		KafkaSource<TransactionEvent> transactionSource = KafkaSource.<TransactionEvent>builder()
		        .setBootstrapServers(bootstrapServers)
		        .setTopics("dbserver1.transaction")
		        .setGroupId(Uuid.randomUuid().toString())
		        .setStartingOffsets(OffsetsInitializer.earliest())
		        .setValueOnlyDeserializer(txFormat)
		        .setProperty("acks", "all")
		        .build();

		KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
		        .setBootstrapServers(bootstrapServers)
		        .setRecordSerializer(new OrderWithLinesSerializationSchema("orders_with_lines"))
		        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
		        .build();

		BroadcastStream<TransactionEvent> transactionsStream = env.fromSource(DataStreamV2SourceUtils.wrapSource(transactionSource),
						"transactions-source")
		.broadcast();


		KeyedPartitionStream<Long, DataChangeEvent> orders =
				env.fromSource(
						DataStreamV2SourceUtils.wrapSource(ordersSource),
						"orders-source"
						)
				.connectAndProcess(transactionsStream, new CommitLsnFixer())
				.withName("orders-commit-lsn-fixer")
				.connectAndProcess(transactionsStream, new WatermarkInjector("inventory.orders"))
				.withName("orders-watermark-injector")
				.keyBy(DataChangeEvent::id)
				;

		KeyedPartitionStream<Long, DataChangeEvent> orderLines =
				env.fromSource(
						DataStreamV2SourceUtils.wrapSource(orderLineSource),
						"order-line-source"
						)
				.connectAndProcess(transactionsStream, new CommitLsnFixer())
				.withName("order-lines-commit-lsn-fixer")
				.connectAndProcess(transactionsStream, new WatermarkInjector("inventory.order_lines"))
				.withName("order-line-watermark-injector")
				.keyBy(r -> r.op().equals("d") ? Long.valueOf((int)r.before().get("order_id")) : Long.valueOf((int)r.after().get("order_id")))
				;

		connectAndProcess((KeyedPartitionStreamImpl<Long, DataChangeEvent>)orders, (KeyedPartitionStreamImpl<Long, DataChangeEvent>)orderLines, new TxAwareJoinProcessFunction(new DataChangeEventJoinFunction(), JoinType.INNER))
				.keyBy(DataChangeEventPair::id)
				.process(new TxAwareAggregationFunction())
				.toSink(DataStreamV2SinkUtils.wrapSink(NonPostCommitTopologySupportingKafkaSink.getInstance(kafkaSink)));

		env.execute("Purchase Order Joiner");
	}

	private static <K, V, T_OTHER, OUT> ProcessConfigurableAndNonKeyedPartitionStream<OUT> connectAndProcess(
			KeyedPartitionStreamImpl<K, V> first,
            KeyedPartitionStreamImpl<K, T_OTHER> other,
            TwoInputNonBroadcastStreamProcessFunction<V, T_OTHER, OUT> processFunction) {
        validateStates(
                processFunction.usesStates(),
                new HashSet<>(
                        Collections.singletonList(StateDeclaration.RedistributionMode.IDENTICAL)));

        TypeInformation<OUT> outTypeInfo =
                StreamUtils.getOutputTypeForTwoInputNonBroadcastProcessFunction(
                        processFunction,
                        first.getType(),
                        other.getType());

        Transformation<OUT> outTransformation;

        if (processFunction instanceof TwoInputNonBroadcastJoinProcessFunction) {
            outTransformation = getJoinTransformation(first, other, processFunction, outTypeInfo);
        } else if (processFunction instanceof InternalTwoInputWindowStreamProcessFunction) {
            outTransformation =
                    StreamUtils.transformTwoInputNonBroadcastWindow(
                            first.getEnvironment().getExecutionConfig(),
                            first.getTransformation(),
                            first.getType(),
                            other.getTransformation(),
                            other.getType(),
                            outTypeInfo,
                            (InternalTwoInputWindowStreamProcessFunction<V, T_OTHER, OUT, ?>)
                                    processFunction,
                            first.getKeySelector(),
                            first.getKeyType(),
                            other.getKeySelector(),
                            other.getKeyType());
        } else {
            KeyedTwoInputNonBroadcastProcessOperator<K, V, T_OTHER, OUT> processOperator =
                    new KeyedTwoInputNonBroadcastProcessOperator<>(processFunction);
            outTransformation =
                    StreamUtils.getTwoInputTransformation(
                            "Keyed-TwoInput-Process",
                            first,
                            other,
                            outTypeInfo,
                            processOperator);
        }

        outTransformation.setAttribute(AttributeParser.parseAttribute(processFunction));
        first.getEnvironment().addOperator(outTransformation);
        return StreamUtils.wrapWithConfigureHandle(
                new NonKeyedPartitionStreamImpl<>(first.getEnvironment(), outTransformation));
    }

	private static <K, V, T_OTHER, OUT> Transformation<OUT> getJoinTransformation(
			KeyedPartitionStreamImpl<K, V> first,
            KeyedPartitionStream<K, T_OTHER> other,
            TwoInputNonBroadcastStreamProcessFunction<V, T_OTHER, OUT> processFunction,
            TypeInformation<OUT> outTypeInfo) {
        ListStateDescriptor<V> leftStateDesc =
                new ListStateDescriptor<>("join-left-state", first.getType());
        ListStateDescriptor<T_OTHER> rightStateDesc =
                new ListStateDescriptor<>(
                        "join-right-state",
                        ((KeyedPartitionStreamImpl<Object, T_OTHER>) other).getType());
        KeyedTwoInputNonBroadcastProcessOperator<K, V, T_OTHER, OUT> joinProcessOperator =
                new TxAwareTwoInputNonBroadcastJoinProcessOperator<>(
                        processFunction, leftStateDesc, rightStateDesc);
        return StreamUtils.getTwoInputTransformation(
                "Keyed-Join-Process",
                first,
                (KeyedPartitionStreamImpl<K, T_OTHER>) other,
                outTypeInfo,
                joinProcessOperator);
    }
}
