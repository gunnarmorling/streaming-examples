/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright Gunnar Morling
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.morling.demos.partialevents;

import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Optional;

import org.apache.flink.table.annotation.StateHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.types.inference.StateTypeStrategy;
import org.apache.flink.table.types.inference.StaticArgument;
import org.apache.flink.table.types.inference.StaticArgumentTrait;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.types.Row;

/**
 * Backfills the "biography" TOAST column in Debezium update events using a PTF (process table function) managing a state store.
 */
public class SqlPtfJob {

	public static void main(String[] args) {
		EnvironmentSettings settings = EnvironmentSettings.newInstance()
				.inStreamingMode()
				.build();

		TableEnvironment tableEnv = TableEnvironment.create(settings);
		tableEnv.createTemporarySystemFunction("ToastBackfill", ToastBackfillFunction.class);

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
				ts_ms BIGINT
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
				  'topic' = 'dbserver2.inventory.authors.backfilled-ptf',
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
				  after,
				  source,
				  op,
				  ts_ms
				FROM
				  ToastBackfill(TABLE authors PARTITION BY id, 'biography')
				""");
		authors.insertInto("authors_backfilled").execute();
		authors.execute().print();
	}

	public static class ToastBackfillFunction extends ProcessTableFunction<Row> {

		private static final String UNCHANGED_TOAST_VALUE = "__debezium_unavailable_value";

		public static class ToastState {
			public String value;
		}

		public void eval(
				@StateHint ToastState state,
				//@ArgumentHint(ArgumentTrait.TABLE_AS_SET)
				Row input,
				//@ArgumentHint(value = ArgumentTrait.SCALAR, name = "column")
				String column) {
			Row newRowState = (Row)input.getField("after");

			switch ((String)input.getField("op")) {
				case "r", "c" -> state.value = (String) newRowState.getField(column);

				case "u" -> {
					if (UNCHANGED_TOAST_VALUE.equals(newRowState.getField(column))) {
						newRowState.setField(column, state.value);
					} else {
						state.value = (String) newRowState.getField(column);
					}
				}

				case "d" -> {
					state.value = null;
				}
			}

			collect(input);
		}

		@Override
		public TypeInference getTypeInference(DataTypeFactory typeFactory) {
			LinkedHashMap<String, StateTypeStrategy> stateTypeStrategies = LinkedHashMap.newLinkedHashMap(1);
			stateTypeStrategies.put("state", StateTypeStrategy.of(TypeStrategies.explicit(DataTypes.of(ToastState.class).toDataType(typeFactory))));

			return TypeInference.newBuilder()
					.staticArguments(
							StaticArgument.table(
									"input",
									Row.class,
									false,
									EnumSet.of(StaticArgumentTrait.TABLE_AS_SET
							)),
							StaticArgument.scalar("column", DataTypes.STRING(), false)
					)
					.stateTypeStrategies(stateTypeStrategies)
					.outputTypeStrategy(callContext -> Optional.of(callContext.getArgumentDataTypes().get(0)))
					.build();
		}
	}
}
