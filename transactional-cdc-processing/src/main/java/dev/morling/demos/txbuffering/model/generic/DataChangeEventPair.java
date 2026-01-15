package dev.morling.demos.txbuffering.model.generic;

public record DataChangeEventPair(DataChangeEvent left, DataChangeEvent right) {

	public long commitLsn() {
		return Math.max(left.commitLsn(), right.commitLsn());
	}

	public long id() {
		return left.id();
	}
}
