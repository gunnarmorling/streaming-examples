/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.demos.txbuffering.model.generic;

import java.util.List;

/**
 * {
  "status": "END",
  "id": "775:34365024",
  "event_count": 1,
  "data_collections": [
    {
      "data_collection": "inventory.customers",
      "event_count": 1
    }
  ],
  "ts_ms": 1760426071299
}
 */
public record TransactionEvent(Status status, String id, int event_count, List<DataCollection> data_collections, long ts_ms) {
	public static enum Status { BEGIN, END };

	public int txId() {
		return Integer.valueOf(id.split(":")[0]);
	}

	public long commitLsn() {
		if (status == Status.BEGIN) {
			throw new IllegalArgumentException("BEGIN events currently don't expose the commit LSN, see https://github.com/debezium/dbz/issues/1555");
		}

		return Long.valueOf(id.split(":")[1]);
	}

	public int countFor(String collection) {
		for (DataCollection dataCollection : data_collections) {
			if (dataCollection.data_collection().equals(collection)) {
				return dataCollection.event_count();
			}
		}

		return 0;
	}
}
