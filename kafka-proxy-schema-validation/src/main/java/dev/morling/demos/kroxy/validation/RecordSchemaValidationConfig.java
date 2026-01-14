package dev.morling.demos.kroxy.validation;

import java.util.List;

import com.google.re2j.Pattern;

public record RecordSchemaValidationConfig(String schemaRegistryUrl, List<ValidationRule> rules) {

	public static record ValidationRule(String topics, ValidationConfig keys, ValidationConfig values) {

		public boolean matches(String topic) {
			return Pattern.matches(topics, topic);
		}
	}

	public static record ValidationConfig(boolean validateId, boolean validatePayload, String schemaType) {
	}
}
