/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.demos.txbuffering.join;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.state.BroadcastStateDeclaration;
import org.apache.flink.api.common.state.StateDeclarations;
import org.apache.flink.api.common.typeinfo.TypeDescriptors;
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
 * Fixes the commit LSN in data change events by buffering them until the transaction END event
 * arrives, then re-emitting them with the correct commit LSN from the END event's source.lsn field.
 *
 * This works around a Debezium bug where transaction.id contains the event LSN instead of the
 * actual commit LSN. See: https://github.com/debezium/dbz/issues/1555
 */
public class CommitLsnFixer implements TwoInputBroadcastStreamProcessFunction<DataChangeEvent, TransactionEvent, DataChangeEvent> {

	private static final System.Logger LOG = System.getLogger(CommitLsnFixer.class.getName());

	// State to buffer events by txId until we receive the END event with correct commit LSN
	// Key: txId, Value: JSON-serialized list of DataChangeEvents
	private static final BroadcastStateDeclaration<Integer, String> BUFFERED_EVENTS_STATE =
			StateDeclarations.mapStateBuilder("bufferedEvents", TypeDescriptors.INT, TypeDescriptors.STRING)
					.buildBroadcast();

	// State to store the correct commit LSN for each transaction once we receive the END event
	// Key: txId, Value: correct commit LSN
	private static final BroadcastStateDeclaration<Integer, Long> COMMIT_LSN_STATE =
			StateDeclarations.mapStateBuilder("commitLsn", TypeDescriptors.INT, TypeDescriptors.LONG)
					.buildBroadcast();

	private transient ObjectMapper objectMapper;

	@Override
	public void open(NonPartitionedContext<DataChangeEvent> ctx) throws Exception {
		objectMapper = new ObjectMapper();
	}

	@Override
	public void processRecordFromNonBroadcastInput(DataChangeEvent record, Collector<DataChangeEvent> output,
			PartitionedContext<DataChangeEvent> ctx) throws Exception {

		int txId = record.txId();

		// Check if we already have the correct commit LSN for this transaction
		Long correctCommitLsn = ctx.getStateManager().getState(COMMIT_LSN_STATE).get(txId);

		if (correctCommitLsn != null) {
			// END event already received, emit with corrected LSN immediately
			DataChangeEvent corrected = record.withCommitLsn(correctCommitLsn);
			LOG.log(Level.DEBUG, "Emitting record with corrected LSN {0} -> {1} (txId={2}, id={3})",
					String.valueOf(record.commitLsn()), String.valueOf(correctCommitLsn), String.valueOf(txId), String.valueOf(record.id()));
			output.collect(corrected);
		} else {
			// Buffer the event until we receive the END event
			LOG.log(Level.DEBUG, "Buffering record txId={0}, id={1}, eventLsn={2}",
					String.valueOf(txId), String.valueOf(record.id()), String.valueOf(record.commitLsn()));

			String existingJson = ctx.getStateManager().getState(BUFFERED_EVENTS_STATE).get(txId);
			List<DataChangeEvent> bufferedEvents;
			if (existingJson != null) {
				bufferedEvents = objectMapper.readValue(existingJson, new TypeReference<ArrayList<DataChangeEvent>>() {});
			} else {
				bufferedEvents = new ArrayList<>();
			}
			bufferedEvents.add(record);
			ctx.getStateManager().getState(BUFFERED_EVENTS_STATE).put(txId, objectMapper.writeValueAsString(bufferedEvents));
		}
	}

	@Override
	public void processRecordFromBroadcastInput(TransactionEvent record, NonPartitionedContext<DataChangeEvent> ctx)
			throws Exception {

		if (record.status() == Status.END) {
			int txId = record.txId();
			long correctCommitLsn = record.commitLsn();

			LOG.log(Level.DEBUG, "Received END event for txId={0}, correctCommitLsn={1}",
					String.valueOf(txId), String.valueOf(correctCommitLsn));

			ctx.applyToAllPartitions((collector, context) -> {
				// Store the correct commit LSN
				context.getStateManager().getState(COMMIT_LSN_STATE).put(txId, correctCommitLsn);

				// Flush any buffered events for this transaction
				String bufferedJson = context.getStateManager().getState(BUFFERED_EVENTS_STATE).get(txId);
				if (bufferedJson != null) {
					List<DataChangeEvent> bufferedEvents = objectMapper.readValue(bufferedJson, new TypeReference<ArrayList<DataChangeEvent>>() {});

					LOG.log(Level.DEBUG, "Flushing {0} buffered events for txId={1} with corrected LSN {2}",
							String.valueOf(bufferedEvents.size()), String.valueOf(txId), String.valueOf(correctCommitLsn));

					for (DataChangeEvent event : bufferedEvents) {
						DataChangeEvent corrected = event.withCommitLsn(correctCommitLsn);
						collector.collect(corrected);
					}

					// Remove buffered events after flushing
					context.getStateManager().getState(BUFFERED_EVENTS_STATE).remove(txId);
				}

				// Cleanup old commit LSN entries to prevent memory leak
				cleanupOldEntries(context, txId);
			});
		}
	}

	private void cleanupOldEntries(PartitionedContext<DataChangeEvent> context, int currentTxId) throws Exception {
		// Remove entries for transactions that are significantly older
		// This is a simple heuristic - transactions more than 1000 txIds behind are cleaned up
		final int cleanupThreshold = 1000;
		final int cleanupTarget = currentTxId - cleanupThreshold;

		// Clean up old commit LSN entries
		if (context.getStateManager().getState(COMMIT_LSN_STATE).contains(cleanupTarget)) {
			context.getStateManager().getState(COMMIT_LSN_STATE).remove(cleanupTarget);
		}

		// Clean up old buffered events (shouldn't happen normally, but safety measure)
		if (context.getStateManager().getState(BUFFERED_EVENTS_STATE).contains(cleanupTarget)) {
			String bufferedJson = context.getStateManager().getState(BUFFERED_EVENTS_STATE).get(cleanupTarget);
			if (bufferedJson != null) {
				List<DataChangeEvent> orphaned = objectMapper.readValue(bufferedJson, new TypeReference<ArrayList<DataChangeEvent>>() {});
				LOG.log(Level.WARNING, "Removing {0} orphaned buffered events for old txId={1}",
						String.valueOf(orphaned.size()), String.valueOf(cleanupTarget));
			}
			context.getStateManager().getState(BUFFERED_EVENTS_STATE).remove(cleanupTarget);
		}
	}
}
