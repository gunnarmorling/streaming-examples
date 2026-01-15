package dev.morling.demos.txbuffering.model.generic;

import java.util.Map;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public record DataChangeEvent(
		Map<String, Object> before,
		Map<String, Object> after,
		Map<String, Object> source,
		String op,
		Transaction transaction) {

	@JsonIgnoreProperties(ignoreUnknown = true)
	public record Transaction(String id) {
		public int txId() {
			return Integer.valueOf(id.split(":")[0]);
		}

		public long commitLsn() {
			return Long.valueOf(id.split(":")[1]);
		}

		public Transaction withCommitLsn(long commitLsn) {
			String txId = id.split(":")[0];
			return new Transaction(txId + ":" + commitLsn);
		}
	}

	public int txId() {
		return transaction != null ? transaction.txId() : -1;
	}

	public long commitLsn() {
		return transaction != null ? transaction.commitLsn() : -1L;
	}

	public DataChangeEvent withCommitLsn(long commitLsn) {
		Transaction correctedTransaction = transaction != null
			? transaction.withCommitLsn(commitLsn)
			: new Transaction("-1:" + commitLsn);
		return new DataChangeEvent(before, after, source, op, correctedTransaction);
	}

	public long id() {
		return after != null ? (int) after.get("id") : (int) before.get("id");
	}

	public String qualifiedTable() {
		return (String)source().get("schema") + "." + (String)source().get("table");
	}
}
