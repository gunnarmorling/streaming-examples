/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.demos.txbuffering.model.purchaseorder;

import java.time.LocalDate;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import dev.morling.demos.txbuffering.model.generic.DataChangeEventPair;

public record OrderWithLines(long id, LocalDate orderDate, int purchaser, String shippingAddress, @JsonIgnore Map<Integer, OrderLine> linesById, long commitLsn) {

	@JsonGetter("lines")
	public Collection<OrderLine> lines() {
		return linesById != null ? linesById.values() : null;
	}

	@JsonCreator
	public static OrderWithLines fromJson(
			@JsonProperty("id") long id,
			@JsonProperty("orderDate") LocalDate orderDate,
			@JsonProperty("purchaser") int purchaser,
			@JsonProperty("shippingAddress") String shippingAddress,
			@JsonProperty("lines") List<OrderLine> lines,
			@JsonProperty("commitLsn") long commitLsn) {
		Map<Integer, OrderLine> linesById = lines != null
				? lines.stream().collect(Collectors.toMap(OrderLine::id, Function.identity()))
				: new HashMap<>();
		return new OrderWithLines(id, orderDate, purchaser, shippingAddress, linesById, commitLsn);
	}

	public static OrderWithLines fromDataChangeEventPair(DataChangeEventPair changeEventPair) {
		if (!(changeEventPair.left().op().equals("c") || changeEventPair.left().op().equals("r") || changeEventPair.left().op().equals("u"))) {
			throw new IllegalStateException("Expecting INSERT or UPDATE event");
		}

		Map<String, Object> order = changeEventPair.left().after();

		OrderLine line = OrderLine.fromDataChangeEvent(changeEventPair.right());
		return new OrderWithLines(
				(int)order.get("id"),
				LocalDate.ofEpochDay(((Number)order.get("order_date")).longValue()),
				(int)order.get("purchaser"),
				(String)order.get("shipping_address"),
				new HashMap<>(Map.of(line.id(), line)),
				changeEventPair.commitLsn()
		);
	}

	public OrderWithLines updateFromDataChangeEventPair(DataChangeEventPair changeEventPair) {
		if (changeEventPair.left().op().equals("c") || changeEventPair.left().op().equals("r") || changeEventPair.left().op().equals("u")) {
			Map<String, Object> order = changeEventPair.left().after();

			if (changeEventPair.right().op().equals("c") || changeEventPair.right().op().equals("r") || changeEventPair.right().op().equals("u")) {
				OrderLine line = OrderLine.fromDataChangeEvent(changeEventPair.right());
				linesById.put(line.id(), line);
			}
			else if (changeEventPair.right().op().equals("d")) {
				int id = (int) changeEventPair.right().before().get("id");
				linesById.remove(id);
			}

			return new OrderWithLines(
					(int)order.get("id"),
					LocalDate.ofEpochDay(((Number)order.get("order_date")).longValue()),
					(int)order.get("purchaser"),
					(String)order.get("shipping_address"),
					linesById,
					changeEventPair.commitLsn());
		}
		else {
			Map<String, Object> order = changeEventPair.left().before();
			return new OrderWithLines(
					(int)order.get("id"),
					null,
					0,
					null,
					null,
					changeEventPair.commitLsn());
		}
	}
}
