/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.demos.txbuffering.join;

import java.lang.System.Logger.Level;
import java.util.ArrayList;
import java.util.List;

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

	/**
	 * Watermarks to emit, set by the operator before calling the watermark handlers.
	 * This removes the need for duplicate watermark tracking in this function.
	 */
	private List<Long> watermarksToEmit = new ArrayList<>();

	public TxAwareJoinProcessFunction(JoinFunction<DataChangeEvent, DataChangeEvent, DataChangeEventPair> joinFunction,
			JoinType joinType) {
		super(joinFunction, joinType);
	}

	/**
	 * Called by the operator to set watermarks that should be emitted downstream.
	 * The operator handles the min(first, second) tracking logic.
	 */
	public void setWatermarksToEmit(List<Long> watermarks) {
		this.watermarksToEmit = watermarks;
	}

	@Override
	public WatermarkHandlingResult onWatermarkFromFirstInput(Watermark watermark,
			Collector<DataChangeEventPair> output, NonPartitionedContext<DataChangeEventPair> ctx)
					throws Exception {
		emitWatermarks(ctx);
		return WatermarkHandlingResult.POLL;
	}

	@Override
	public WatermarkHandlingResult onWatermarkFromSecondInput(Watermark watermark,
			Collector<DataChangeEventPair> output, NonPartitionedContext<DataChangeEventPair> ctx)
					throws Exception {
		emitWatermarks(ctx);
		return WatermarkHandlingResult.POLL;
	}

	private void emitWatermarks(NonPartitionedContext<DataChangeEventPair> ctx) throws Exception {
		for (Long wm : watermarksToEmit) {
			LOG.log(Level.DEBUG, "Emitting WM {0}", wm);
			LongWatermark outgoingWatermark = WatermarkInjector.WATERMARK_DECLARATION.newWatermark(wm);
			ctx.getWatermarkManager().emitWatermark(outgoingWatermark);
		}
		watermarksToEmit.clear();
	}
}
