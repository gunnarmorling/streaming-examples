package dev.morling.demos.partialevents;

import java.io.IOException;
import java.util.HashMap;

import org.apache.flink.api.common.serialization.DeserializationSchema.InitializationContext;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DebeziumDeserializationSchema implements KafkaRecordDeserializationSchema<KafkaRecord> {

	private transient ObjectMapper mapper;

	@Override
	public TypeInformation<KafkaRecord> getProducedType() {
		return BasicTypeInfo.of(KafkaRecord.class);
	}
	
	@Override
	public void open(InitializationContext context) throws Exception {
		mapper = new ObjectMapper();
	}

	@Override
	public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<KafkaRecord> out) throws IOException {
		//out.collect(new KafkaRecord(mapper.readTree(record.key())., mapper.readTree(record.value())));
		out.collect(new KafkaRecord(mapper.readValue(record.key(), SchematizedKey.class), mapper.readValue(record.value(), SchematizedChangeDataEvent.class)));
	}
}
