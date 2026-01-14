package dev.morling.demos.kroxy.validation.internal.model;

public record PayloadValidationResult(String errorMessage) {

	public boolean isValid() {
		return errorMessage == null;
	}

	public static PayloadValidationResult valid() {
		return new PayloadValidationResult(null);
	}

	public static PayloadValidationResult invalid(String errorMessage) {
		return new PayloadValidationResult(errorMessage);
	}
}
