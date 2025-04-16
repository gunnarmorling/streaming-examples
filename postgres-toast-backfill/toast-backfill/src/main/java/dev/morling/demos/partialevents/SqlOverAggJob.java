/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright Gunnar Morling
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.demos.partialevents;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Backfills the "biography" TOAST column in Debezium update events using an OVER aggregation.
 */
public class SqlOverAggJob {

	public static void main(String[] args) {
		Configuration configuration = new Configuration();
		// configuration.setString("table.exec.source.idle-timeout", "1000");

		EnvironmentSettings settings = EnvironmentSettings.newInstance()
				.inStreamingMode()
				.withConfiguration(configuration)
				.build();

		TableEnvironment tableEnv = TableEnvironment.create(settings);

		tableEnv.executeSql("""
				CREATE TABLE authors (
				id BIGINT NOT NULL,
				before ROW(),
				after ROW(
				  `id` BIGINT,
				  `first_name` STRING,
				  `last_name` STRING,
				  `biography` STRING,
				  `dob` BIGINT
				),
				source ROW(
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
				  `xmin` BIGINT),
				op STRING,
				ts_ms BIGINT,
				--time_ltz AS TO_TIMESTAMP_LTZ(ts_ms, 3),
				--WATERMARK FOR time_ltz AS time_ltz
				proctime AS PROCTIME()
				) WITH (
				  'connector' = 'kafka',
				  'topic' = 'dbserver2.inventory.authors',
				  'properties.bootstrap.servers' = 'localhost:9092',
				  'properties.group.id' = 'testGroup',
				  'scan.startup.mode' = 'earliest-offset',
				  'key.format' = 'json',
				  'key.fields' = 'id',
				  'value.format' = 'json',
 				  'value.fields-include' = 'EXCEPT_KEY'
				);
				""");

		tableEnv.executeSql("""
				CREATE TABLE authors_backfilled (
				id BIGINT NOT NULL,
				before ROW(),
				after ROW(
				  `id` BIGINT,
				  `first_name` STRING,
				  `last_name` STRING,
				  `biography` STRING,
				  `dob` BIGINT
				),
				source ROW(
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
				  `xmin` BIGINT),
				op STRING,
				ts_ms BIGINT
				) WITH (
				  'connector' = 'kafka',
				  'topic' = 'dbserver2.inventory.authors.backfilled-overagg',
				  'properties.bootstrap.servers' = 'localhost:9092',
				  'properties.group.id' = 'testGroup',
				  'key.format' = 'json',
				  'key.fields' = 'id',
				  'value.format' = 'json',
 				  'value.fields-include' = 'EXCEPT_KEY'
				);
				""");

		Table authors = tableEnv.sqlQuery("""
				SELECT
				  id,
				  before,
				  ROW(
				    id,
				    after.first_name,
				    after.last_name,
				    CASE
				      WHEN after.biography IS NULL THEN NULL
				      ELSE
				        LAST_VALUE(NULLIF(after.biography, '__debezium_unavailable_value')) OVER (
				          PARTITION BY id
				          --ORDER BY time_ltz
				          ORDER BY proctime
				          RANGE UNBOUNDED PRECEDING
				        )
				    END,
				    after.dob
				  ),
				  source,
				  op,
				  ts_ms
				FROM
				  authors
				""");
		authors.insertInto("authors_backfilled").execute();
		authors.execute().print();
	}
}
