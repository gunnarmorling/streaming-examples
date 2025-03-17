package dev.morling.demos.cdcingest;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

public class KafkaAppendStreamJob {

	public static void main(String[] args) {
		EnvironmentSettings settings = EnvironmentSettings
			    .newInstance()
			    .inStreamingMode()
			    .build();

		TableEnvironment tableEnv = TableEnvironment.create(settings);
		
		tableEnv.executeSql("""
				CREATE TABLE authors_source (
				  `id` BIGINT NOT NULL,
				  `before` ROW(
				    `id` BIGINT,
				    `first_name` STRING,
				    `last_name` STRING,
				    `biography` STRING,
				    `registered` BIGINT
				  ),
				  `after` ROW(
				    `id` BIGINT,
				    `first_name` STRING,
				    `last_name` STRING,
				    `biography` STRING,
				    `registered` BIGINT
				  ),
				  `source` ROW(
				    `version` STRING,
				    `connector` STRING,
				    `name` STRING,
				    `ts_ms` BIGINT,
				    `snapshot` BOOLEAN,
				    `db` STRING,
				    `sequence` STRING,
				    `table` STRING,
				    `txid` BIGINT,
				    `lsn` BIGINT,
				    `xmin` BIGINT
				  ),
				  `op` STRING,
				  `ts_ms` BIGINT
				) WITH (
				  'connector' = 'kafka',
				  'topic' = 'dbserver1.inventory.authors',
				  'properties.bootstrap.servers' = 'localhost:9092',
				  'scan.startup.mode' = 'earliest-offset',
				  'key.format' = 'json',
				  'key.fields' = 'id',
				  'value.format' = 'json',
				  'value.fields-include' = 'EXCEPT_KEY'
				);
				""");
		
		tableEnv.executeSql("""
				CREATE TABLE authors_sink (
				  `id` BIGINT NOT NULL,
				  `before` ROW(
				    `id` BIGINT,
				    `first_name` STRING,
				    `last_name` STRING,
				    `biography` STRING,
				    `registered` BIGINT
				  ),
				  `after` ROW(
				    `id` BIGINT,
				    `first_name` STRING,
				    `last_name` STRING,
				    `biography` STRING,
				    `registered` BIGINT
				  ),
				  `source` ROW(
				    `version` STRING,
				    `connector` STRING,
				    `name` STRING,
				    `ts_ms` BIGINT,
				    `snapshot` BOOLEAN,
				    `db` STRING,
				    `sequence` STRING,
				    `table` STRING,
				    `txid` BIGINT,
				    `lsn` BIGINT,
				    `xmin` BIGINT
				  ),
				  `op` STRING,
				  `ts_ms` BIGINT
				) WITH (
				  'connector' = 'kafka',
				  'topic' = 'authors_processed',
				  'properties.bootstrap.servers' = 'localhost:9092',
				  'key.format' = 'json',
				  'key.fields' = 'id',
				  'value.format' = 'json',
 				  'value.fields-include' = 'EXCEPT_KEY'
				);
				""");
		
		Table authors = tableEnv.sqlQuery("SELECT id, before, after, source, op, ts_ms FROM authors_source");
		authors.insertInto("authors_sink").execute();
		authors.execute().print();
	}
}
