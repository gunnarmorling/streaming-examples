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

public class DebeziumKafkaTableApiJob {

	public static void main(String[] args) {
		EnvironmentSettings settings = EnvironmentSettings
			    .newInstance()
			    .inStreamingMode()
			    .build();

		TableEnvironment tableEnv = TableEnvironment.create(settings);
		
		tableEnv.executeSql("""
				CREATE TABLE authors_source (
					key STRING,
					id BIGINT,
					first_name STRING,
					last_name STRING,
					biography STRING,
  `origin_table` STRING METADATA FROM 'value.source.table' VIRTUAL,
  `source_schema` STRING METADATA FROM 'value.source.schema' VIRTUAL,
  origin_properties MAP<STRING, STRING> METADATA FROM 'value.source.properties' VIRTUAL
  // `source_timestamp` 	TIMESTAMP_LTZ(3) METADATA FROM 'value.source.timestamp' VIRTUAL
  
// 				PRIMARY KEY (id) NOT ENFORCED
				) WITH (
				  'connector' = 'kafka',
				  'topic' = 'full.inventory.authors',
				  'properties.bootstrap.servers' = 'localhost:9092',
//				  'properties.group.id' = 'testGroup',
				  'scan.startup.mode' = 'earliest-offset',
				  'key.format' = 'raw',
				  'key.fields' = 'key',
				  'value.format' = 'debezium-json',
				  'value.fields-include' = 'EXCEPT_KEY'
				);
				""");
		
		tableEnv.executeSql("""
				CREATE TABLE authors_sink (
					id BIGINT,
					first_name STRING,
					last_name STRING,
					biography STRING,
					origin_table STRING METADATA FROM 'value.source.table' VIRTUAL
				) WITH (
				  'connector' = 'kafka',
				  'topic' = 'full.inventory.authors.out',
				  'properties.bootstrap.servers' = 'localhost:9092',
				  'value.format' = 'debezium-json'
				);
				""");
		
//		Table authors = tableEnv.sqlQuery("SELECT id, before, after, source, op, ts_ms FROM ToastBackfill(TABLE authors PARTITION BY id)");
//		
		
		Table authors = tableEnv.sqlQuery("SELECT * from authors_source");
		authors.execute().print();
		
		TableResult authors_sinkResult = authors.insertInto("authors_sink").execute();
//		authors_sinkResult.print();
//		tableResult.print();

	}
}
