package dev.morling.demos.kroxy.validation.internal;

import dev.morling.demos.kroxy.validation.RecordSchemaValidationConfig.ValidationConfig;
import dev.morling.demos.kroxy.validation.RecordSchemaValidationConfig.ValidationRule;

public class RecordValidator {

	private final ValidationRule validationRule;

	public RecordValidator(ValidationRule validationRule) {
		this.validationRule = validationRule;
	}

	public PayloadValidator<?> keyValidator() {
		return payloadValidator(validationRule.keys());
	}

	public PayloadValidator<?> valueValidator() {
		return payloadValidator(validationRule.values());
	}

	private PayloadValidator<?> payloadValidator(ValidationConfig config) {
		if (config == null || !config.validatePayload()) {
			return PayloadValidator.NO_OP;
		}

		// TODO make pluggable via service loader
		switch(config.schemaType().toLowerCase()) {
			case "avro": return new AvroPayloadValidator();
			case "json": return new JsonPayloadValidator();
			default: throw new IllegalArgumentException("Unknown schema type: " + config.schemaType());
		}
	}
}
