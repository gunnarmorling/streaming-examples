package dev.morling.demos.kroxy.validation.internal;

import org.everit.json.schema.ValidationException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import dev.morling.demos.kroxy.validation.internal.model.PayloadValidationResult;
import io.confluent.kafka.schemaregistry.json.JsonSchema;

public class JsonPayloadValidator implements PayloadValidator<JsonSchema> {

	private final ObjectMapper objectMapper;

	public JsonPayloadValidator() {
		objectMapper = new ObjectMapper();
	}

	@Override
	public PayloadValidationResult validate(byte[] bytes, JsonSchema schema) {

		try {
			JsonNode deserialized = objectMapper.reader().readTree(bytes);
			schema.validate(deserialized);
			return PayloadValidationResult.valid();
		} catch(ValidationException e) {
			return PayloadValidationResult.invalid("Invalid payload: " + e.getMessage());

		} catch (Exception e) {
			return PayloadValidationResult.invalid("Validation failed: " + e.getMessage());
		}
	}
}
