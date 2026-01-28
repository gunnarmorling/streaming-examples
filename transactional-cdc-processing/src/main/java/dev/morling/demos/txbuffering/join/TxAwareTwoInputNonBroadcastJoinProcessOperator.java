/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.demos.txbuffering.join;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.v2.ListState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.watermark.LongWatermark;
import org.apache.flink.datastream.api.function.TwoInputNonBroadcastStreamProcessFunction;
import org.apache.flink.datastream.impl.extension.join.operators.TwoInputNonBroadcastJoinProcessFunction;
import org.apache.flink.datastream.impl.operators.KeyedTwoInputNonBroadcastProcessOperator;
import org.apache.flink.runtime.event.WatermarkEvent;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import dev.morling.demos.txbuffering.model.generic.DataChangeEvent;

import static java.lang.System.Logger.Level;

public class TxAwareTwoInputNonBroadcastJoinProcessOperator<K, V, T_OTHER, OUT> extends KeyedTwoInputNonBroadcastProcessOperator<K, V, T_OTHER, OUT> {

	private static final System.Logger LOG = System.getLogger(TxAwareTwoInputNonBroadcastJoinProcessOperator.class.getName());

	private final org.apache.flink.api.common.state.v2.ListStateDescriptor<V> leftStateDescriptor;

    private final org.apache.flink.api.common.state.v2.ListStateDescriptor<T_OTHER> rightStateDescriptor;

    /** The state that stores the left input records. */
    private transient ListState<V> leftState1;

    /** The state that stores the right input records. */
    private transient ListState<T_OTHER> rightState1;

	/** Operator state for checkpointing watermark tracking. */
	private transient org.apache.flink.api.common.state.ListState<Long> minWatermarkFirstInputState;
	private transient org.apache.flink.api.common.state.ListState<Long> minWatermarkSecondInputState;
	private transient org.apache.flink.api.common.state.ListState<Long> minWatermarkState;
	private transient org.apache.flink.api.common.state.ListState<Long> watermarksToFlushState;

	private long minWatermarkFromFirstInput = -1;
	private long minWatermarkFromSecondInput = -1;
	private long minWatermark = -1;

	private SortedSet<Long> watermarksToFlush = new TreeSet<Long>();

	private TwoInputNonBroadcastJoinProcessFunction<V, T_OTHER, OUT> joinProcessFunction;

	public TxAwareTwoInputNonBroadcastJoinProcessOperator(
			TwoInputNonBroadcastStreamProcessFunction<V, T_OTHER, OUT> userFunction,
			org.apache.flink.api.common.state.v2.ListStateDescriptor<V> leftStateDescriptor,
			org.apache.flink.api.common.state.v2.ListStateDescriptor<T_OTHER> rightStateDescriptor) {
		super(userFunction);

		this.joinProcessFunction =
                (TwoInputNonBroadcastJoinProcessFunction<V, T_OTHER, OUT>) userFunction;

		this.leftStateDescriptor =
                new org.apache.flink.api.common.state.v2.ListStateDescriptor<>("buffer-left-state", leftStateDescriptor.getTypeInformation());
		this.rightStateDescriptor =
                new org.apache.flink.api.common.state.v2.ListStateDescriptor<>("buffer-right-state", rightStateDescriptor.getTypeInformation());
	}

	@Override
	public void open() throws Exception {
		super.open();

		LOG.log(Level.DEBUG, "{0}", getAsyncKeyedStateBackend());

		leftState1 =
				getOrCreateKeyedState(
						VoidNamespace.INSTANCE,
						VoidNamespaceSerializer.INSTANCE,
						leftStateDescriptor);
		rightState1 =
				getOrCreateKeyedState(
						VoidNamespace.INSTANCE,
						VoidNamespaceSerializer.INSTANCE,
						rightStateDescriptor);
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);

		// Initialize operator state for watermark tracking
		ListStateDescriptor<Long> firstInputDesc = new ListStateDescriptor<>(
			"min-watermark-first-input", Types.LONG);
		ListStateDescriptor<Long> secondInputDesc = new ListStateDescriptor<>(
			"min-watermark-second-input", Types.LONG);
		ListStateDescriptor<Long> minWmDesc = new ListStateDescriptor<>(
			"min-watermark", Types.LONG);
		ListStateDescriptor<Long> toFlushDesc = new ListStateDescriptor<>(
			"watermarks-to-flush", Types.LONG);

		minWatermarkFirstInputState = context.getOperatorStateStore()
			.getListState(firstInputDesc);
		minWatermarkSecondInputState = context.getOperatorStateStore()
			.getListState(secondInputDesc);
		minWatermarkState = context.getOperatorStateStore()
			.getListState(minWmDesc);
		watermarksToFlushState = context.getOperatorStateStore()
			.getListState(toFlushDesc);

		// Restore state on recovery
		if (context.isRestored()) {
			for (Long value : minWatermarkFirstInputState.get()) {
				minWatermarkFromFirstInput = value;
			}
			for (Long value : minWatermarkSecondInputState.get()) {
				minWatermarkFromSecondInput = value;
			}
			for (Long value : minWatermarkState.get()) {
				minWatermark = value;
			}
			watermarksToFlush.clear();
			for (Long value : watermarksToFlushState.get()) {
				watermarksToFlush.add(value);
			}
			LOG.log(Level.INFO, "Restored watermark state: firstInput={0}, secondInput={1}, min={2}, toFlush={3}",
				minWatermarkFromFirstInput, minWatermarkFromSecondInput, minWatermark, watermarksToFlush.size());
		}
	}

	@Override
	public void snapshotState(StateSnapshotContext context) throws Exception {
		super.snapshotState(context);

		// Clear and update operator state
		minWatermarkFirstInputState.clear();
		minWatermarkFirstInputState.add(minWatermarkFromFirstInput);

		minWatermarkSecondInputState.clear();
		minWatermarkSecondInputState.add(minWatermarkFromSecondInput);

		minWatermarkState.clear();
		minWatermarkState.add(minWatermark);

		watermarksToFlushState.clear();
		for (Long wm : watermarksToFlush) {
			watermarksToFlushState.add(wm);
		}
	}

	@Override
	public void processWatermark1Internal(WatermarkEvent watermark) throws Exception {
		LOG.log(Level.DEBUG, "Receiving WM {0} (first)", String.valueOf(((LongWatermark)watermark.getWatermark()).getValue()));

		minWatermarkFromFirstInput = ((LongWatermark)watermark.getWatermark()).getValue();
		watermarksToFlush.add(minWatermarkFromFirstInput);

		long newMinWatermark = Math.min(minWatermarkFromFirstInput, minWatermarkFromSecondInput);

		List<Long> toEmit = new ArrayList<>();
		if (newMinWatermark > this.minWatermark) {
			this.minWatermark = newMinWatermark;
			flushBuffers(newMinWatermark);
			cleanupState(newMinWatermark);
			toEmit = extractWatermarksToEmit(newMinWatermark);
		}

		// Pass watermarks to emit to the function before calling super
		((TxAwareJoinProcessFunction) joinProcessFunction).setWatermarksToEmit(toEmit);
		super.processWatermark1Internal(watermark);
	}

	@Override
	public void processWatermark2Internal(WatermarkEvent watermark) throws Exception {
		LOG.log(Level.DEBUG, "Receiving WM {0} (second)", String.valueOf(((LongWatermark)watermark.getWatermark()).getValue()));

		minWatermarkFromSecondInput = ((LongWatermark)watermark.getWatermark()).getValue();
		watermarksToFlush.add(minWatermarkFromSecondInput);

		long newMinWatermark = Math.min(minWatermarkFromFirstInput, minWatermarkFromSecondInput);

		List<Long> toEmit = new ArrayList<>();
		if (newMinWatermark > this.minWatermark) {
			this.minWatermark = newMinWatermark;
			flushBuffers(newMinWatermark);
			cleanupState(newMinWatermark);
			toEmit = extractWatermarksToEmit(newMinWatermark);
		}

		// Pass watermarks to emit to the function before calling super
		((TxAwareJoinProcessFunction) joinProcessFunction).setWatermarksToEmit(toEmit);
		super.processWatermark2Internal(watermark);
	}

	private List<Long> extractWatermarksToEmit(long upToWatermark) {
		List<Long> toEmit = new ArrayList<>();
		Iterator<Long> it = watermarksToFlush.iterator();
		while (it.hasNext()) {
			Long wm = it.next();
			if (wm <= upToWatermark) {
				toEmit.add(wm);
				it.remove();
			} else {
				break; // SortedSet, so remaining are all > upToWatermark
			}
		}
		return toEmit;
	}

	private void flushBuffers(long watermark) throws Exception {
		// Process each watermark <= the current min watermark
		// Note: watermark removal is handled separately by extractWatermarksToEmit
		for (Long toFlush : watermarksToFlush) {
			if (toFlush > watermark) {
				break; // SortedSet, so remaining are all > watermark
			}

			LOG.log(Level.DEBUG, "Flushing buffers {0}", String.valueOf(toFlush));

			Set<Object> keys = keySet;
			LOG.log(Level.DEBUG, "Keys: {0}", keys);

			for (Object key : keys) {
				try {
					setAsyncKeyedContextElement(new StreamRecord<Long>((Long) key), r -> r);
				} catch (Exception e) {
					e.printStackTrace();
				}

				V leftChanged = getLeftByCommitLsn(toFlush);
				if (leftChanged != null) {
					Map<Long, DataChangeEvent> latestRightPerIdByCommitLsn = getLatestRightPerIdByCommitLsn(toFlush);

					StreamRecord<V> element = new StreamRecord<V>(leftChanged);
					collector.setTimestampFromStreamRecord(element);
					V leftRecord = element.getValue();

					for (Entry<Long, DataChangeEvent> right : latestRightPerIdByCommitLsn.entrySet()) {
						try {
							joinProcessFunction
								.getJoinFunction()
								.processRecord(leftRecord, (T_OTHER) right.getValue(), collector, partitionedContext);
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
				else {
					V left = getLatestLeftByCommitLsn(toFlush);
					Map<Long, DataChangeEvent> rightChanged = getRightPerIdByCommitLsn(toFlush);

					for (Entry<Long, DataChangeEvent> right : rightChanged.entrySet()) {
						StreamRecord<T_OTHER> element = new StreamRecord<T_OTHER>((T_OTHER)right.getValue());
						collector.setTimestampFromStreamRecord(element);

						try {
							joinProcessFunction
								.getJoinFunction()
								.processRecord(left, element.getValue(), collector, partitionedContext);
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
			}
		}
	}

	private V getLeftByCommitLsn(long commitLsn) {
		Iterable<V> left = leftState1.get();
		DataChangeEvent latestSoFar = null;

		if (left == null) {
			return null;
		}

		for (V record : left) {
			DataChangeEvent dce = (DataChangeEvent) record;

			if (dce.commitLsn() == commitLsn) {
				latestSoFar = dce;
			}
		}

		return (V) latestSoFar;
	}

	private V getLatestLeftByCommitLsn(long commitLsn) {
		Iterable<V> left = leftState1.get();
		DataChangeEvent latestSoFar = null;

		if (left == null) {
			return null;
		}

		for (V record : left) {
			DataChangeEvent dce = (DataChangeEvent) record;

			if (latestSoFar == null || dce.commitLsn() >= latestSoFar.commitLsn()) {
				if (dce.commitLsn() <= commitLsn) {
					latestSoFar = dce;
				}
			}
		}

		return (V) latestSoFar;
	}

	private Map<Long, DataChangeEvent> getLatestRightPerIdByCommitLsn(long commitLsn) {
		Map<Long, DataChangeEvent> latestPerId = new HashMap<>();

		Iterable<T_OTHER> right = rightState1.get();

		if (right != null) {
			for (T_OTHER record : right) {
				DataChangeEvent dce = (DataChangeEvent) record;

				if (dce.commitLsn() > commitLsn) {
					continue;
				}

				Long key = dce.id();
				DataChangeEvent latestSoFar = latestPerId.get(key);

				if (latestSoFar == null || dce.commitLsn() >= latestSoFar.commitLsn()) {
					latestPerId.put(key, dce);
				}
			}
		}

		return latestPerId;
	}

	private Map<Long, DataChangeEvent> getRightPerIdByCommitLsn(long commitLsn) {
		Map<Long, DataChangeEvent> right = new HashMap<>();

		Iterable<T_OTHER> allRights = rightState1.get();

		if (allRights != null) {
			for (T_OTHER record : allRights) {
				DataChangeEvent dce = (DataChangeEvent) record;

				if (dce.commitLsn() == commitLsn) {
					Long key = dce.id();
					right.put(key, dce);
				}
			}
		}

		return right;
	}

	/**
	 * Cleans up old state after a watermark has been processed.
	 * Retains only the latest record per key for processed transactions,
	 * plus all records from future (not yet processed) transactions.
	 */
	private void cleanupState(long watermark) throws Exception {
		for (Object key : keySet) {
			try {
				setAsyncKeyedContextElement(new StreamRecord<Long>((Long) key), r -> r);
				cleanupLeftState(watermark);
				cleanupRightState(watermark);
			} catch (Exception e) {
				throw new RuntimeException("Failed to cleanup state for key: " + key, e);
			}
		}
	}

	private void cleanupLeftState(long watermark) {
		Iterable<V> left = leftState1.get();
		if (left == null) {
			return;
		}

		DataChangeEvent latestProcessed = null;
		List<V> futureRecords = new ArrayList<>();

		for (V record : left) {
			DataChangeEvent dce = (DataChangeEvent) record;
			if (dce.commitLsn() <= watermark) {
				// Keep only the latest among processed records
				if (latestProcessed == null || dce.commitLsn() > latestProcessed.commitLsn()) {
					latestProcessed = dce;
				}
			} else {
				// Preserve future (unprocessed) records
				futureRecords.add(record);
			}
		}

		leftState1.clear();
		if (latestProcessed != null) {
			leftState1.add((V) latestProcessed);
		}
		for (V record : futureRecords) {
			leftState1.add(record);
		}
	}

	private void cleanupRightState(long watermark) {
		Iterable<T_OTHER> right = rightState1.get();
		if (right == null) {
			return;
		}

		// Keep latest per order line ID (multiple lines per order)
		Map<Long, DataChangeEvent> latestPerId = new HashMap<>();
		List<T_OTHER> futureRecords = new ArrayList<>();

		for (T_OTHER record : right) {
			DataChangeEvent dce = (DataChangeEvent) record;
			if (dce.commitLsn() <= watermark) {
				Long lineId = dce.id();
				DataChangeEvent existing = latestPerId.get(lineId);
				if (existing == null || dce.commitLsn() > existing.commitLsn()) {
					latestPerId.put(lineId, dce);
				}
			} else {
				futureRecords.add(record);
			}
		}

		rightState1.clear();
		for (DataChangeEvent dce : latestPerId.values()) {
			rightState1.add((T_OTHER) dce);
		}
		for (T_OTHER record : futureRecords) {
			rightState1.add(record);
		}
	}

	@Override
	public void processElement1(StreamRecord<V> element) throws Exception {
		V leftRecord = element.getValue();
		LOG.log(Level.DEBUG, "Receiving record {0} - {1} (first)", String.valueOf(((DataChangeEvent)leftRecord).commitLsn()), String.valueOf(((DataChangeEvent)leftRecord).id()));
        leftState1.add(leftRecord);
	}

	@Override
	public void processElement2(StreamRecord<T_OTHER> element) throws Exception {
		T_OTHER rightRecord = element.getValue();
		LOG.log(Level.DEBUG, "Receiving record {0} - {1} (second)", String.valueOf(((DataChangeEvent)rightRecord).commitLsn()), String.valueOf(((DataChangeEvent)rightRecord).id()));
        rightState1.add(rightRecord);
	}
}
