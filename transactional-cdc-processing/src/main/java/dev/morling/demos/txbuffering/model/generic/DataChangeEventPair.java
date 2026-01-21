/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.demos.txbuffering.model.generic;

public record DataChangeEventPair(DataChangeEvent left, DataChangeEvent right) {

	public long commitLsn() {
		return Math.max(left.commitLsn(), right.commitLsn());
	}

	public long id() {
		return left.id();
	}
}
