package dev.morling.demos.txbuffering.join;

import java.lang.System.Logger.Level;
import java.util.Set;

import org.apache.flink.api.common.state.StateDeclaration;
import org.apache.flink.api.common.state.StateDeclarations;
import org.apache.flink.api.common.state.ValueStateDeclaration;
import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.api.common.typeinfo.TypeDescriptors;
import org.apache.flink.api.common.watermark.LongWatermark;
import org.apache.flink.api.common.watermark.Watermark;
import org.apache.flink.api.common.watermark.WatermarkHandlingResult;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.NonPartitionedContext;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import dev.morling.demos.txbuffering.model.generic.DataChangeEventPair;
import dev.morling.demos.txbuffering.model.purchaseorder.OrderWithLines;

public class TxAwareAggregationFunction implements OneInputStreamProcessFunction<DataChangeEventPair, String> {

	private static final System.Logger LOG = System.getLogger(TxAwareAggregationFunction.class.getName());

	private static final ValueStateDeclaration<String> ORDER_STATE = StateDeclarations.valueState("orders", TypeDescriptors.STRING);

	private transient ObjectMapper objectMapper;

	@Override
	public void open(NonPartitionedContext<String> ctx) throws Exception {
		objectMapper = new ObjectMapper();
		objectMapper.registerModule(new JavaTimeModule());
		objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
	}

	@Override
	public Set<StateDeclaration> usesStates() {
		return Set.of(ORDER_STATE);
	}

	@Override
	public void processRecord(DataChangeEventPair record, Collector<String> output, PartitionedContext<String> ctx) throws Exception {
		LOG.log(Level.DEBUG, "Storing {0}/{1} {2} - {3}", String.valueOf(record.left().commitLsn()), String.valueOf(record.right().commitLsn()), String.valueOf(record.left().id()), String.valueOf(record.right().id()));

		ValueState<String> state = ctx.getStateManager().getState(ORDER_STATE);
		OrderWithLines order = null;
		String currentTxState = state.value();

		if (currentTxState == null) {
			LOG.log(Level.DEBUG, "Initializing state");
			order = OrderWithLines.fromDataChangeEventPair(record);
		}
		else {
			LOG.log(Level.DEBUG, "Updating state for commitLsn {0}", String.valueOf(record.commitLsn()));
			order = objectMapper.readValue(currentTxState, OrderWithLines.class);
			order = order.updateFromDataChangeEventPair(record);
		}

		state.update(objectMapper.writeValueAsString(order));
	}

	@Override
	public WatermarkHandlingResult onWatermark(Watermark watermark, Collector<String> output,
			NonPartitionedContext<String> ctx) throws Exception {

		LOG.log(Level.DEBUG, "Receiving watermark {0}", watermark);

		ctx.applyToAllPartitions((collector, context) -> {
			String value = context.getStateManager().getState(ORDER_STATE).value();

			OrderWithLines order = objectMapper.readValue(value, OrderWithLines.class);
			if (order.commitLsn() == ((LongWatermark)watermark).getValue()) {
				LOG.log(Level.DEBUG, "Apply to all ({0}) {1} {2}", String.valueOf(context.getTaskInfo().getIndexOfThisSubtask()), String.valueOf(((LongWatermark)watermark).getValue()), value);
				collector.collect(value);
			}
		});

		return OneInputStreamProcessFunction.super.onWatermark(watermark, output, ctx);
	}
}
