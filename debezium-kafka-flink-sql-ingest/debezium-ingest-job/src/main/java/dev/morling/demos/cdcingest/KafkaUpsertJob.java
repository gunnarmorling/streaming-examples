package dev.morling.demos.cdcingest;

import java.util.Map;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

public class KafkaUpsertJob {

	public static void main(String[] args) {
		Configuration configuration = Configuration.fromMap(Map.of("table.exec.source.cdc-events-duplicate", "true"));
		
		EnvironmentSettings settings = EnvironmentSettings
			    .newInstance()
			    .inStreamingMode()
			    .withConfiguration(configuration)
			    .build();

		TableEnvironment tableEnv = TableEnvironment.create(settings);
		
		tableEnv.executeSql("""
				CREATE TABLE authors_source (
					id BIGINT,
					first_name STRING,
					last_name STRING,
					biography STRING,
					registered BIGINT,
					PRIMARY KEY (id) NOT ENFORCED
				) WITH (
				  'connector' = 'upsert-kafka',
				  'topic' = 'dbserver2.inventory.authors',
				  'properties.bootstrap.servers' = 'localhost:9092',
				  'key.format' = 'json',
				  'value.format' = 'json'
				);
				""");
		
		tableEnv.executeSql("""
				CREATE TABLE authors_sink (
					id BIGINT,
					first_name STRING,
					last_name STRING,
					biography STRING,
					registered BIGINT,
					PRIMARY KEY (id) NOT ENFORCED	
				) WITH (
				  'connector' = 'upsert-kafka',
				  'topic' = 'authors_processed',
				  'properties.bootstrap.servers' = 'localhost:9092',
				  'key.format' = 'json',
				  'value.format' = 'json'
				);
				""");
		
		Table authors = tableEnv.sqlQuery("SELECT id, first_name, last_name, biography, registered FROM authors_source");
		authors.insertInto("authors_sink").execute();
		authors.execute().print();
	}
}
