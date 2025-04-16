package dev.morling.demos.partialevents;

import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Optional;

import org.apache.flink.connector.datagen.table.DataGenConnectorOptions;
import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.ArgumentTrait;
import org.apache.flink.table.annotation.StateHint;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.types.inference.StateTypeStrategy;
import org.apache.flink.table.types.inference.StaticArgument;
import org.apache.flink.table.types.inference.StaticArgumentTrait;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.table.types.inference.strategies.ExplicitTypeStrategy;
import org.apache.flink.types.Row;

public class TableApiJob {

	public static void main(String[] args) {
		EnvironmentSettings settings = EnvironmentSettings
			    .newInstance()
			    .inStreamingMode()
			    //.inBatchMode()
			    .build();

		TableEnvironment tableEnv = TableEnvironment.create(settings);
		tableEnv.createTemporarySystemFunction("ToastBackfill", ToastBackfillFunction.class);
		
		// Create a source table
		tableEnv.createTemporaryTable("SourceTable", TableDescriptor.forConnector("datagen")
		    .schema(Schema.newBuilder()
		      .column("f0", DataTypes.STRING())
		      .build())
		    .option(DataGenConnectorOptions.ROWS_PER_SECOND, 100L)
		    .build());

		// Create a sink table (using SQL DDL)
		tableEnv.executeSql("CREATE TEMPORARY TABLE SinkTable WITH ('connector' = 'blackhole') LIKE SourceTable (EXCLUDING OPTIONS) ");

		// Create a Table object from a Table API query
		Table table1 = tableEnv.from("SourceTable");

		// Create a Table object from a SQL query
		Table table2 = tableEnv.sqlQuery("SELECT * FROM SourceTable");

		// Emit a Table API result Table to a TableSink, same for SQL result
		TableResult tableResult = table1.insertInto("SinkTable").execute();
		
		tableEnv.executeSql("""
				CREATE TABLE authors (
				id BIGINT NOT NULL,
				before ROW(),
				after ROW(
					`id` BIGINT,
					`first_name` STRING,
					`last_name` STRING,
					`biography` STRING,
					`registered` BIGINT
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
				
				//,
// 				PRIMARY KEY (id) NOT ENFORCED
				) WITH (
				  'connector' = 'kafka',
				  'topic' = 'dbserver10.inventory.authors',
				  'properties.bootstrap.servers' = 'localhost:9092',
				  'properties.group.id' = 'testGroup',
				  'scan.startup.mode' = 'earliest-offset',
//				  'key.fields' = 'id',
				  'key.format' = 'json',
				  'key.fields' = 'id',
				  'value.format' = 'json',
 				  'value.fields-include' = 'EXCEPT_KEY'

//				  'key.debezium-json.schema-include' = 'true',
//				  'format' = 'debezium-json',
//				  'debezium-json.schema-include' = 'true'
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
					`registered` BIGINT
					
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
				  'topic' = 'authors_enriched',
				  'properties.bootstrap.servers' = 'localhost:9092',
				  'properties.group.id' = 'testGroup',
				  'key.format' = 'json',
				  'key.fields' = 'id',
				  'value.format' = 'json',
 				  'value.fields-include' = 'EXCEPT_KEY'
				);
				""");
		
		Table authors = tableEnv.sqlQuery("SELECT id, before, after, source, op, ts_ms FROM ToastBackfill(TABLE authors PARTITION BY id)");
		
		TableResult authorsEnrichedResult = authors.insertInto("authors_backfilled").execute();
		
//		Table authors = tableEnv.sqlQuery("SELECT * from authors");
		
		authors.execute().print();
//		tableResult.print();

	}
	
	public static class ToastBackfillFunction extends ProcessTableFunction<Row> {
		
		public static class ToastState {
			public String value;
		}

		public void eval(@StateHint ToastState state, @ArgumentHint(ArgumentTrait.TABLE_AS_SET) Row input) {
			Row newRowState = (Row)input.getField("after");
			
			switch ((String)input.getField("op")) {
				case "r", "c" -> state.value = (String) newRowState.getField("biography");
	
				case "u" -> {
					if ("__debezium_unavailable_value".equals(newRowState.getField("biography"))) {
						newRowState.setField("biography", state.value);
					} else {
						state.value = (String) newRowState.getField("biography");
					}
				}
	
				case "d" -> {}
			}

			
//			((Row)input.getField("after")).setField("biography", "Hello" + ((Row)input.getField("after")).getField("biography"));
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
								))
						)
						.stateTypeStrategies(stateTypeStrategies)
						.outputTypeStrategy(callContext -> Optional.of(callContext.getArgumentDataTypes().get(0)))
						.build();
			}
	}
}
