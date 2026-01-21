/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.demos.txbuffering.model.purchaseorder;

import java.math.BigDecimal;
import java.util.Map;

import dev.morling.demos.txbuffering.model.generic.DataChangeEvent;

public record OrderLine(int id, int orderId, int productId, int quantity, BigDecimal price) {

	public static OrderLine fromDataChangeEvent(DataChangeEvent dataChangeEvent) {
		if (!(dataChangeEvent.op().equals("c") || dataChangeEvent.op().equals("r") || dataChangeEvent.op().equals("u"))) {
			throw new IllegalStateException("Expecting INSERT or UPDATE event");
		}

		Map<String, Object> line = dataChangeEvent.after();

		return new OrderLine(
			(int)line.get("id"),
			(int)line.get("order_id"),
			(int)line.get("product_id"),
			(int)line.get("quantity"),
			new BigDecimal(line.get("price").toString())
		);
	}
}
