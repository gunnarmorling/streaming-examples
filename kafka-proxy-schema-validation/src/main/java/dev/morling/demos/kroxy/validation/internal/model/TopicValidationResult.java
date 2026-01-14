package dev.morling.demos.kroxy.validation.internal.model;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.common.message.ProduceResponseData.PartitionProduceResponse;
import org.apache.kafka.common.message.ProduceResponseData.TopicProduceResponse;

public record TopicValidationResult(String name, List<PartitionValidationResult> partitionResults, boolean allValid) {

	public TopicProduceResponse toResponse() {
		TopicProduceResponse response = new TopicProduceResponse();
		response.setName(name);

		List<PartitionProduceResponse> partitionResponses = new ArrayList<>();
		for (PartitionValidationResult partitionResult : partitionResults) {
			partitionResponses.add(partitionResult.toResponse());
		}

		response.setPartitionResponses(partitionResponses);
		return response;
	}

	public static TopicValidationResult valid(String name) {
		return new TopicValidationResult(name, List.of(), true);
	}
}