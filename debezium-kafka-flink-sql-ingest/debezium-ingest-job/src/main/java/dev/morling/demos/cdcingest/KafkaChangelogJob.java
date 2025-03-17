package dev.morling.demos.cdcingest;

import java.util.Map;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

public class KafkaChangelogJob {

	public static void main(String[] args) {
		Configuration configuration = Configuration.fromMap(Map.of("table.exec.source.cdc-events-duplicate", "true"));
		
		EnvironmentSettings settings = EnvironmentSettings
			    .newInstance()
			    .inStreamingMode()
			    .withConfiguration(configuration)
			    .build();

//		StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
//		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, settings);
		
		TableEnvironment tableEnv = TableEnvironment.create(settings);
		
		tableEnv.executeSql("""
				CREATE TABLE authors_source (
					id BIGINT,
					first_name STRING,
					last_name STRING,
					biography STRING,
					registered BIGINT,
					ts_ms TIMESTAMP_LTZ METADATA FROM 'value.ingestion-timestamp' VIRTUAL,
					source_table STRING METADATA FROM 'value.source.table' VIRTUAL,
					source_properties MAP<STRING, STRING> METADATA FROM 'value.source.properties' VIRTUAL,
					PRIMARY KEY (id) NOT ENFORCED
				) WITH (
				  'connector' = 'kafka',
				  'topic' = 'dbserver1.inventory.authors',
				  'properties.bootstrap.servers' = 'localhost:9092',
				  'scan.startup.mode' = 'earliest-offset',
//				  'key.format' = 'json',
//				  'key.fields' = 'id',
				  'value.format' = 'debezium-json'
//				  'value.fields-include' = 'EXCEPT_KEY'

				);
				""");
		
		tableEnv.executeSql("""
				CREATE TABLE authors_sink (
					id BIGINT,
					first_name STRING,
					last_name STRING,
					biography STRING,
					registered BIGINT
				) WITH (
				  'connector' = 'kafka',
				  'topic' = 'authors_processed',
				  'properties.bootstrap.servers' = 'localhost:9092',
				  'key.format' = 'json',
				  'key.fields' = 'id',
				  'value.format' = 'debezium-json'
				);
				""");
		
		Table authors = tableEnv.sqlQuery("SELECT id, first_name, last_name, biography, registered FROM authors_source");
		authors.insertInto("authors_sink").execute();
		authors.execute().print();
	}
}
