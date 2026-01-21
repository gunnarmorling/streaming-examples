/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.demos.txbuffering.join;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.flink.api.common.state.v2.ListState;
import org.apache.flink.api.common.state.v2.ListStateDescriptor;
import org.apache.flink.api.common.watermark.LongWatermark;
import org.apache.flink.datastream.api.function.TwoInputNonBroadcastStreamProcessFunction;
import org.apache.flink.datastream.impl.extension.join.operators.TwoInputNonBroadcastJoinProcessFunction;
import org.apache.flink.datastream.impl.operators.KeyedTwoInputNonBroadcastProcessOperator;
import org.apache.flink.runtime.event.WatermarkEvent;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import dev.morling.demos.txbuffering.model.generic.DataChangeEvent;

import static java.lang.System.Logger.Level;

public class TxAwareTwoInputNonBroadcastJoinProcessOperator<K, V, T_OTHER, OUT> extends KeyedTwoInputNonBroadcastProcessOperator<K, V, T_OTHER, OUT> {

	private static final System.Logger LOG = System.getLogger(TxAwareTwoInputNonBroadcastJoinProcessOperator.class.getName());

	private final ListStateDescriptor<V> leftStateDescriptor;

    private final ListStateDescriptor<T_OTHER> rightStateDescriptor;

    /** The state that stores the left input records. */
    private transient ListState<V> leftState1;

    /** The state that stores the right input records. */
    private transient ListState<T_OTHER> rightState1;

	private long minWatermarkFromFirstInput = -1;
	private long minWatermarkFromSecondInput = -1;
	private long minWatermark = -1;

	private SortedSet<Long> watermarksToFlush = new TreeSet<Long>();

	private TwoInputNonBroadcastJoinProcessFunction<V, T_OTHER, OUT> joinProcessFunction;

	public TxAwareTwoInputNonBroadcastJoinProcessOperator(
			TwoInputNonBroadcastStreamProcessFunction<V, T_OTHER, OUT> userFunction,
			ListStateDescriptor<V> leftStateDescriptor, ListStateDescriptor<T_OTHER> rightStateDescriptor) {
		super(userFunction);

		this.joinProcessFunction =
                (TwoInputNonBroadcastJoinProcessFunction<V, T_OTHER, OUT>) userFunction;

		this.leftStateDescriptor =
                new ListStateDescriptor<>("buffer-left-state", leftStateDescriptor.getTypeInformation());
		this.rightStateDescriptor =
                new ListStateDescriptor<>("buffer-right-state", rightStateDescriptor.getTypeInformation());
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
	public void processWatermark1Internal(WatermarkEvent watermark) throws Exception {
		LOG.log(Level.DEBUG, "Receiving WM {0} (first)", String.valueOf(((LongWatermark)watermark.getWatermark()).getValue()));

		minWatermarkFromFirstInput = ((LongWatermark)watermark.getWatermark()).getValue();
		watermarksToFlush.add(minWatermarkFromFirstInput);


		long minWatermark = Math.min(minWatermarkFromFirstInput, minWatermarkFromSecondInput);

		if (minWatermark > this.minWatermark) {
			this.minWatermark = minWatermark;
			flushBuffers(minWatermark);
		}

		super.processWatermark1Internal(watermark);
	}

	@Override
	public void processWatermark2Internal(WatermarkEvent watermark) throws Exception {
		LOG.log(Level.DEBUG, "Receiving WM {0} (second)", String.valueOf(((LongWatermark)watermark.getWatermark()).getValue()));

		minWatermarkFromSecondInput = ((LongWatermark)watermark.getWatermark()).getValue();
		watermarksToFlush.add(minWatermarkFromSecondInput);

		long minWatermark = Math.min(minWatermarkFromFirstInput, minWatermarkFromSecondInput);

		if (minWatermark > this.minWatermark) {
			this.minWatermark = minWatermark;
			flushBuffers(minWatermark);
		}

		super.processWatermark2Internal(watermark);
	}

	private void flushBuffers(long watermark) throws Exception {
		Iterator<Long> waterMarkstoFlush = watermarksToFlush.iterator();

		while(waterMarkstoFlush.hasNext()) {
			Long toFlush = waterMarkstoFlush.next();

			if(toFlush <= watermark) {

				LOG.log(Level.DEBUG, "Flushing buffers {0}", String.valueOf(toFlush));
				waterMarkstoFlush.remove();

				Set<Object> keys = keySet;

				LOG.log(Level.DEBUG, "Keys: {0}", keys);

	//
				keys.forEach(key -> {
					try {
						setAsyncKeyedContextElement(new StreamRecord<Long>((Long) key), r -> r);
					} catch (Exception e) {
						// TODO Auto-generated catch block
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
								// TODO Auto-generated catch block
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
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}

					}

				});
			}
			else {
				break;
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

				Long key = Long.valueOf((Integer) dce.after().get("id"));
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
