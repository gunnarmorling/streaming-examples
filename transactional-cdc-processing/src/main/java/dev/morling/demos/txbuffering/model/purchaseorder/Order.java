/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.demos.txbuffering.model.purchaseorder;

import java.time.LocalDate;
import java.util.Map;

import dev.morling.demos.txbuffering.model.generic.DataChangeEvent;

public record Order(int id, LocalDate orderDate, int purchaser, String shippingAddress) {

	public static Order fromDataChangeEvent(DataChangeEvent dataChangeEvent) {
		if (!(dataChangeEvent.op().equals("c") || dataChangeEvent.op().equals("r") || dataChangeEvent.op().equals("u"))) {
			throw new IllegalStateException("Expecting INSERT or UPDATE event");
		}

		Map<String, Object> order = dataChangeEvent.after();

		return new Order(
			(int)order.get("id"),
			LocalDate.ofEpochDay(((Number)order.get("order_date")).longValue()),
			(int)order.get("purchaser"),
			(String)order.get("shipping_address")
		);
	}
}
