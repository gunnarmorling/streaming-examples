/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.demos.txbuffering.join;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.flink.api.common.state.BroadcastStateDeclaration;
import org.apache.flink.api.common.state.StateDeclarations;
import org.apache.flink.api.common.typeinfo.TypeDescriptors;
import org.apache.flink.api.common.watermark.LongWatermark;
import org.apache.flink.api.common.watermark.LongWatermarkDeclaration;
import org.apache.flink.api.common.watermark.WatermarkDeclaration;
import org.apache.flink.api.common.watermark.WatermarkDeclarations;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.NonPartitionedContext;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.function.TwoInputBroadcastStreamProcessFunction;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import dev.morling.demos.txbuffering.model.generic.DataChangeEvent;
import dev.morling.demos.txbuffering.model.generic.TransactionEvent;
import dev.morling.demos.txbuffering.model.generic.TransactionEvent.Status;

import static java.lang.System.Logger.Level;

/**
 * Injects watermarks into an event stream, when all the events of a given type originating from one and the same transaction have been emitted.
 */
public class WatermarkInjector implements TwoInputBroadcastStreamProcessFunction<DataChangeEvent, TransactionEvent, DataChangeEvent> {

	private static final System.Logger LOG = System.getLogger(WatermarkInjector.class.getName());

	public static final LongWatermarkDeclaration WATERMARK_DECLARATION = WatermarkDeclarations
			.newBuilder("TX_WATERMARK")
			.typeLong()
			.combineFunctionMin()
			.combineWaitForAllChannels(true)
			//				.defaultHandlingStrategyForward()
			.defaultHandlingStrategyIgnore()
			.build();

	private static final String TRANSACTIONS_KEY = "TRANSACTIONS";
	private static final String CURRENT_WATERMARK_KEY = "CURRENT_WATERMARK";

	private static final BroadcastStateDeclaration<String, String> TRANSACTIONS_STATE = StateDeclarations.mapStateBuilder("transactions", TypeDescriptors.STRING, TypeDescriptors.STRING)
			.buildBroadcast();

	private static final BroadcastStateDeclaration<Long, Integer> COUNTS_STATE = StateDeclarations.mapStateBuilder("counts", TypeDescriptors.LONG, TypeDescriptors.INT)
			.buildBroadcast();

	private transient ObjectMapper objectMapper;
	private String qualifiedTable;


	public WatermarkInjector(String qualifiedTable) {
		this.qualifiedTable = qualifiedTable;
	}

	@Override
	public void open(NonPartitionedContext<DataChangeEvent> ctx) throws Exception {
		objectMapper = new ObjectMapper();
	}

	@Override
	public void processRecordFromNonBroadcastInput(DataChangeEvent record, Collector<DataChangeEvent> output,
			PartitionedContext<DataChangeEvent> ctx) throws Exception {

		Integer count = ctx.getStateManager().getState(COUNTS_STATE).get(record.commitLsn());

		LOG.log(Level.DEBUG, "Emitting record {0} - {1} ({2})", String.valueOf(record.commitLsn()), String.valueOf(record.id()), qualifiedTable);
		int newCount = (count == null) ? 1 : count + 1;
		ctx.getStateManager().getState(COUNTS_STATE).put(record.commitLsn(), newCount);

		output.collect(record);

		increaseWatermarkIfPossible(ctx);
	}

	private void increaseWatermarkIfPossible(PartitionedContext<DataChangeEvent> ctx) throws Exception {
		String transactions = ctx.getStateManager().getState(TRANSACTIONS_STATE).get(TRANSACTIONS_KEY);
		long currentWatermark = ctx.getStateManager().getState(TRANSACTIONS_STATE).contains(CURRENT_WATERMARK_KEY) ? Long.valueOf(ctx.getStateManager().getState(TRANSACTIONS_STATE).get(CURRENT_WATERMARK_KEY)) : -1;

		//			if (qualifiedTable.equals("inventory.customers")) {
		//				System.out.println();
		//			}

		if (transactions == null) {
			return;
		}

		long newWatermark = currentWatermark;

		List<TransactionEvent> transactionsObj = objectMapper.readValue(transactions, new TypeReference<ArrayList<TransactionEvent>>() {});

		Iterator<TransactionEvent> it = transactionsObj.iterator();

		while(it.hasNext()) {
			TransactionEvent next = it.next();
			if (next.commitLsn() < currentWatermark) {
				it.remove();
				continue;
			}

			Integer count = ctx.getStateManager().getState(COUNTS_STATE).get(next.commitLsn());
			if (count == null) {
				if (currentWatermark == -1) {
					continue;
				}
				else {
					count = 0;
				}
			}

			if (count == next.countFor(qualifiedTable)) {
				newWatermark = Long.valueOf(next.commitLsn());

				if (newWatermark != currentWatermark) {
					ctx.getStateManager().getState(TRANSACTIONS_STATE).put(CURRENT_WATERMARK_KEY, String.valueOf(newWatermark));
					ctx.getStateManager().getState(TRANSACTIONS_STATE).put(TRANSACTIONS_KEY, objectMapper.writeValueAsString(transactionsObj));

					LongWatermark watermark = WATERMARK_DECLARATION.newWatermark(newWatermark);
					LOG.log(Level.DEBUG, "Emitting WM {0} ({1})", String.valueOf(watermark.getValue()), qualifiedTable);
					ctx.getNonPartitionedContext().getWatermarkManager().emitWatermark(watermark);

					currentWatermark = newWatermark;
				}
			}
			else {
				return;
			}
		}
	}

	@Override
	public void processRecordFromBroadcastInput(TransactionEvent record, NonPartitionedContext<DataChangeEvent> ctx)
			throws Exception {

		if (record.status() == Status.END) {
			ctx.applyToAllPartitions((out, context) -> {
				String transactions = context.getStateManager().getState(TRANSACTIONS_STATE).get(TRANSACTIONS_KEY);
				List<TransactionEvent> transactionsObj = null;
				if(transactions != null) {
					transactionsObj = objectMapper.readValue(transactions, new TypeReference<ArrayList<TransactionEvent>>() {});
				}
				else {
					transactionsObj = new ArrayList<>();
				}
				transactionsObj.add(record);
				context.getStateManager().getState(TRANSACTIONS_STATE).put(TRANSACTIONS_KEY, objectMapper.writeValueAsString(transactionsObj));

				increaseWatermarkIfPossible(context);
			});
		}

	}

	@Override
	public Set<? extends WatermarkDeclaration> declareWatermarks() {
		return Set.of(WATERMARK_DECLARATION);
	}
}
