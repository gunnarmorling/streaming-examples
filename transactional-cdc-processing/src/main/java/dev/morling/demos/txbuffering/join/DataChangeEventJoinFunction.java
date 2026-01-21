/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.demos.txbuffering.join;

import java.lang.System.Logger.Level;

import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.RuntimeContext;
import org.apache.flink.datastream.api.extension.join.JoinFunction;

import dev.morling.demos.txbuffering.model.generic.DataChangeEvent;
import dev.morling.demos.txbuffering.model.generic.DataChangeEventPair;

public class DataChangeEventJoinFunction implements JoinFunction<DataChangeEvent, DataChangeEvent, DataChangeEventPair> {

		private static final System.Logger LOG = System.getLogger(DataChangeEventJoinFunction.class.getName());

		@Override
		public void processRecord(DataChangeEvent leftRecord, DataChangeEvent rightRecord, Collector<DataChangeEventPair> output,
				RuntimeContext ctx) throws Exception {

			LOG.log(Level.DEBUG, "Joining {0}/{1} {2} - {3}", String.valueOf(leftRecord.commitLsn()), String.valueOf(rightRecord.commitLsn()), String.valueOf(leftRecord.id()), String.valueOf(rightRecord.id()));

			output.collect(new DataChangeEventPair(leftRecord, rightRecord));
		}
	}
