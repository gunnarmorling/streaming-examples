package dev.morling.demos.kroxy.validation.internal.model;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.common.message.ProduceResponseData.BatchIndexAndErrorMessage;
import org.apache.kafka.common.message.ProduceResponseData.PartitionProduceResponse;
import org.apache.kafka.common.protocol.Errors;

public record PartitionValidationResult(int partitionIndex, List<RecordValidationResult> recordResults, boolean allValid) {

	public PartitionProduceResponse toResponse() {
		PartitionProduceResponse response = new PartitionProduceResponse();
		response.setErrorCode(Errors.INVALID_RECORD.code());
		response.setIndex(partitionIndex);

		if (allValid) {
			response.setErrorMessage("Invalid record in another topic-partition");
		}
		else {
			response.setErrorMessage("Invalid record in this topic-partition");

			List<BatchIndexAndErrorMessage> errors = new ArrayList<>();

			for (RecordValidationResult recordValidationResult : recordResults) {
				if(!recordValidationResult.isValid()) {
					errors.add(recordValidationResult.toResponse());
				}
			}

			response.setRecordErrors(errors);
		}

		return response;
	}
}