package dev.morling.demos.kroxy.validation.internal.model;

import org.apache.kafka.common.message.ProduceResponseData.BatchIndexAndErrorMessage;

public record RecordValidationResult(String errorMessage, int index) {

	public boolean isValid() {
		return errorMessage == null;
	}

	public static RecordValidationResult valid() {
		return new RecordValidationResult(null, -1);
	}

	public static RecordValidationResult invalid(String errorMessage, int index) {
		return new RecordValidationResult(errorMessage, index);
	}

	public BatchIndexAndErrorMessage toResponse() {
		BatchIndexAndErrorMessage response = new BatchIndexAndErrorMessage();
		response.setBatchIndex(index);
		response.setBatchIndexErrorMessage(errorMessage);
		return response;
	}
}
