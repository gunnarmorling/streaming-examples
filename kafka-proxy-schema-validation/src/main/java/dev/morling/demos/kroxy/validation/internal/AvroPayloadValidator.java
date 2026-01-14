package dev.morling.demos.kroxy.validation.internal;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

import dev.morling.demos.kroxy.validation.internal.model.PayloadValidationResult;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;

public class AvroPayloadValidator implements PayloadValidator<AvroSchema> {

	@Override
	public PayloadValidationResult validate(byte[] bytes, AvroSchema schema) {
		try {
			GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema.rawSchema());
			BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(bytes), null);

			reader.read(null, decoder);

			if (!decoder.isEnd()) {
				return PayloadValidationResult.invalid("Reached end of input while deserializing payload");
			}

			return PayloadValidationResult.valid();  // If deserialization succeeds, it's valid
		} catch (IOException e) {
			return PayloadValidationResult.invalid("Validation failed: " + e.getMessage());
		}
	}
}
