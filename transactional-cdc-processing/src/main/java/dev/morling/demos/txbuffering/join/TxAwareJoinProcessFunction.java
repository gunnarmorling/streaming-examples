/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.demos.txbuffering.join;

import java.lang.System.Logger.Level;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.flink.api.common.state.ListStateDeclaration;
import org.apache.flink.api.common.state.StateDeclaration;
import org.apache.flink.api.common.state.StateDeclarations;
import org.apache.flink.api.common.state.ValueStateDeclaration;
import org.apache.flink.api.common.state.v2.ListState;
import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.api.common.typeinfo.TypeDescriptors;
import org.apache.flink.api.common.watermark.LongWatermark;
import org.apache.flink.api.common.watermark.Watermark;
import org.apache.flink.api.common.watermark.WatermarkHandlingResult;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.NonPartitionedContext;
import org.apache.flink.datastream.api.extension.join.JoinFunction;
import org.apache.flink.datastream.api.extension.join.JoinType;
import org.apache.flink.datastream.impl.extension.join.operators.TwoInputNonBroadcastJoinProcessFunction;

import dev.morling.demos.txbuffering.model.generic.DataChangeEvent;
import dev.morling.demos.txbuffering.model.generic.DataChangeEventPair;

public class TxAwareJoinProcessFunction extends TwoInputNonBroadcastJoinProcessFunction<DataChangeEvent, DataChangeEvent, DataChangeEventPair> {

	private static final System.Logger LOG = System.getLogger(TxAwareJoinProcessFunction.class.getName());

	private static final ValueStateDeclaration<Long> MIN_WATERMARK_FIRST_INPUT_STATE =
			StateDeclarations.valueState("min-watermark-first-input", TypeDescriptors.LONG);
	private static final ValueStateDeclaration<Long> MIN_WATERMARK_SECOND_INPUT_STATE =
			StateDeclarations.valueState("min-watermark-second-input", TypeDescriptors.LONG);
	private static final ValueStateDeclaration<Long> MIN_WATERMARK_STATE =
			StateDeclarations.valueState("min-watermark", TypeDescriptors.LONG);
	private static final ListStateDeclaration<Long> WATERMARKS_TO_FLUSH_STATE =
			StateDeclarations.listState("watermarks-to-flush", TypeDescriptors.LONG);

	@Override
	public Set<StateDeclaration> usesStates() {
		return Set.of(
				MIN_WATERMARK_FIRST_INPUT_STATE,
				MIN_WATERMARK_SECOND_INPUT_STATE,
				MIN_WATERMARK_STATE,
				WATERMARKS_TO_FLUSH_STATE
		);
	}

	public TxAwareJoinProcessFunction(JoinFunction<DataChangeEvent, DataChangeEvent, DataChangeEventPair> joinFunction,
			JoinType joinType) {
		super(joinFunction, joinType);
	}

	@Override
	public WatermarkHandlingResult onWatermarkFromFirstInput(Watermark watermark,
			Collector<DataChangeEventPair> output, NonPartitionedContext<DataChangeEventPair> ctx)
					throws Exception {

		long watermarkValue = ((LongWatermark) watermark).getValue();
		LOG.log(Level.DEBUG, "Receiving WM {0} (first)", watermarkValue);

		ctx.applyToAllPartitions((out, partitionCtx) -> {
			ValueState<Long> minWatermarkFirstInputState = partitionCtx.getStateManager().getState(MIN_WATERMARK_FIRST_INPUT_STATE);
			ValueState<Long> minWatermarkSecondInputState = partitionCtx.getStateManager().getState(MIN_WATERMARK_SECOND_INPUT_STATE);
			ValueState<Long> minWatermarkState = partitionCtx.getStateManager().getState(MIN_WATERMARK_STATE);
			ListState<Long> watermarksToFlushState = partitionCtx.getStateManager().getState(WATERMARKS_TO_FLUSH_STATE);

			minWatermarkFirstInputState.update(watermarkValue);
			watermarksToFlushState.add(watermarkValue);

			Long minWatermarkSecondInput = minWatermarkSecondInputState.value();
			if (minWatermarkSecondInput == null) {
				minWatermarkSecondInput = -1L;
			}

			long minWatermark = Math.min(watermarkValue, minWatermarkSecondInput);

			Long currentMinWatermark = minWatermarkState.value();
			if (currentMinWatermark == null) {
				currentMinWatermark = -1L;
			}

			if (minWatermark > currentMinWatermark) {
				minWatermarkState.update(minWatermark);
				flushWatermarks(watermarksToFlushState, minWatermark, ctx);
			}
		});

		return WatermarkHandlingResult.POLL;
	}

	@Override
	public WatermarkHandlingResult onWatermarkFromSecondInput(Watermark watermark,
			Collector<DataChangeEventPair> output, NonPartitionedContext<DataChangeEventPair> ctx)
					throws Exception {

		long watermarkValue = ((LongWatermark) watermark).getValue();
		LOG.log(Level.DEBUG, "Receiving WM {0} (second)", watermarkValue);

		ctx.applyToAllPartitions((out, partitionCtx) -> {
			ValueState<Long> minWatermarkFirstInputState = partitionCtx.getStateManager().getState(MIN_WATERMARK_FIRST_INPUT_STATE);
			ValueState<Long> minWatermarkSecondInputState = partitionCtx.getStateManager().getState(MIN_WATERMARK_SECOND_INPUT_STATE);
			ValueState<Long> minWatermarkState = partitionCtx.getStateManager().getState(MIN_WATERMARK_STATE);
			ListState<Long> watermarksToFlushState = partitionCtx.getStateManager().getState(WATERMARKS_TO_FLUSH_STATE);

			minWatermarkSecondInputState.update(watermarkValue);
			watermarksToFlushState.add(watermarkValue);

			Long minWatermarkFirstInput = minWatermarkFirstInputState.value();
			if (minWatermarkFirstInput == null) {
				minWatermarkFirstInput = -1L;
			}

			long minWatermark = Math.min(minWatermarkFirstInput, watermarkValue);

			Long currentMinWatermark = minWatermarkState.value();
			if (currentMinWatermark == null) {
				currentMinWatermark = -1L;
			}

			if (minWatermark > currentMinWatermark) {
				minWatermarkState.update(minWatermark);
				flushWatermarks(watermarksToFlushState, minWatermark, ctx);
			}
		});

		return WatermarkHandlingResult.POLL;
	}

	private void flushWatermarks(ListState<Long> watermarksToFlushState, long minWatermark,
			NonPartitionedContext<DataChangeEventPair> ctx) throws Exception {
		Iterable<Long> watermarks = watermarksToFlushState.get();
		if (watermarks == null) {
			return;
		}

		SortedSet<Long> sortedWatermarks = new TreeSet<>();
		for (Long wm : watermarks) {
			sortedWatermarks.add(wm);
		}

		List<Long> toKeep = new java.util.ArrayList<>();
		for (Long toFlush : sortedWatermarks) {
			if (toFlush <= minWatermark) {
				LOG.log(Level.DEBUG, "Emitting WM {0}", toFlush);
				LongWatermark outgoingWatermark = WatermarkInjector.WATERMARK_DECLARATION.newWatermark(toFlush);
				ctx.getWatermarkManager().emitWatermark(outgoingWatermark);
			} else {
				toKeep.add(toFlush);
			}
		}

		watermarksToFlushState.clear();
		for (Long wm : toKeep) {
			watermarksToFlushState.add(wm);
		}
	}
}
