/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.demos.txbuffering.join;

import java.util.Iterator;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.flink.api.common.state.StateDeclaration;
import org.apache.flink.api.common.state.StateDeclarations;
import org.apache.flink.api.common.state.ValueStateDeclaration;
import org.apache.flink.api.common.typeinfo.TypeDescriptors;
import org.apache.flink.api.common.watermark.LongWatermark;
import org.apache.flink.api.common.watermark.Watermark;
import org.apache.flink.api.common.watermark.WatermarkHandlingResult;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.NonPartitionedContext;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.extension.join.JoinFunction;
import org.apache.flink.datastream.api.extension.join.JoinType;
import org.apache.flink.datastream.impl.extension.join.operators.TwoInputNonBroadcastJoinProcessFunction;

import dev.morling.demos.txbuffering.model.generic.DataChangeEvent;
import dev.morling.demos.txbuffering.model.generic.DataChangeEventPair;

import static java.lang.System.Logger.Level;

public class TxAwareJoinProcessFunction extends TwoInputNonBroadcastJoinProcessFunction<DataChangeEvent, DataChangeEvent, DataChangeEventPair> {

	private static final System.Logger LOG = System.getLogger(TxAwareJoinProcessFunction.class.getName());

	ValueStateDeclaration<Long> valueStateDeclaration = StateDeclarations.valueState("min-watermark-state", TypeDescriptors.LONG);

	private long minWatermarkFromFirstInput = -1;
	private long minWatermarkFromSecondInput = -1;
	private long minWatermark = -1;
	private SortedSet<Long> waterMarksToFlush = new TreeSet<Long>();

	@Override
	public Set<StateDeclaration> usesStates() {
		return Set.of(valueStateDeclaration);
	}


	public TxAwareJoinProcessFunction(JoinFunction<DataChangeEvent, DataChangeEvent, DataChangeEventPair> joinFunction,
			JoinType joinType) {
		super(joinFunction, joinType);
	}

	@Override
	public void processRecordFromFirstInput(DataChangeEvent record, Collector<DataChangeEventPair> output,
			PartitionedContext<DataChangeEventPair> ctx) throws Exception {

		LOG.log(Level.DEBUG, "Processing record {0}", record);

		super.processRecordFromFirstInput(record, output, ctx);
	}


	@Override
	public WatermarkHandlingResult onWatermarkFromFirstInput(Watermark watermark,
			Collector<DataChangeEventPair> output, NonPartitionedContext<DataChangeEventPair> ctx)
					throws Exception {

		//            Long previousTxn = txnState.value();

		LOG.log(Level.DEBUG, "Receiving WM {0} (first)", ((LongWatermark)watermark).getValue());
		waterMarksToFlush.add(((LongWatermark)watermark).getValue());
		minWatermarkFromFirstInput = ((LongWatermark)watermark).getValue();

		long minWatermark = Math.min(minWatermarkFromFirstInput, minWatermarkFromSecondInput);

		//			System.out.println(this.minWatermark + " " + minWatermarkFromFirstInput + " " + minWatermarkFromSecondInput);

		if (minWatermark > this.minWatermark) {
			this.minWatermark = minWatermark;

			Iterator<Long> waterMarkstoFlush = waterMarksToFlush.iterator();

			while(waterMarkstoFlush.hasNext()) {
				Long toFlush = waterMarkstoFlush.next();

				if (toFlush <= minWatermark) {
					waterMarkstoFlush.remove();

					LOG.log(Level.DEBUG, "Emitting WM {0} (first)", toFlush);
					LongWatermark outgoingWatermark = WatermarkInjector.WATERMARK_DECLARATION.newWatermark(toFlush);
					ctx.getWatermarkManager().emitWatermark(outgoingWatermark);
				}
				else {
					break;
				}
			}
		}

		return WatermarkHandlingResult.POLL;
	}

	@Override
	public WatermarkHandlingResult onWatermarkFromSecondInput(Watermark watermark,
			Collector<DataChangeEventPair> output, NonPartitionedContext<DataChangeEventPair> ctx)
					throws Exception {

		LOG.log(Level.DEBUG, "Receiving WM {0} (second)", ((LongWatermark)watermark).getValue());
		waterMarksToFlush.add(((LongWatermark)watermark).getValue());
		minWatermarkFromSecondInput = ((LongWatermark)watermark).getValue();

		long minWatermark = Math.min(minWatermarkFromFirstInput, minWatermarkFromSecondInput);

		if (minWatermark > this.minWatermark) {
			this.minWatermark = minWatermark;

			Iterator<Long> waterMarkstoFlush = waterMarksToFlush.iterator();

			while(waterMarkstoFlush.hasNext()) {
				Long toFlush = waterMarkstoFlush.next();

				if (toFlush <= minWatermark) {
					waterMarkstoFlush.remove();

					LOG.log(Level.DEBUG, "Emitting WM {0} (second)", toFlush);
					LongWatermark outgoingWatermark = WatermarkInjector.WATERMARK_DECLARATION.newWatermark(toFlush);
					ctx.getWatermarkManager().emitWatermark(outgoingWatermark);
				}
				else {
					break;
				}
			}
		}


		return WatermarkHandlingResult.POLL;
	}

}
