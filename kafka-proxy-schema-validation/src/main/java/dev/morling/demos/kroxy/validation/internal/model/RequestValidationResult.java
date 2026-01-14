package dev.morling.demos.kroxy.validation.internal.model;

import java.util.List;

import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.ProduceResponseData.TopicProduceResponseCollection;

public record RequestValidationResult(List<TopicValidationResult> topicResults) {

	public boolean allValid() {
		for (TopicValidationResult topicValidationResult : topicResults) {
			if(!topicValidationResult.allValid()) {
				return false;
			}
		}

		return true;
	}

	public ProduceResponseData toResponse() {
		ProduceResponseData response = new ProduceResponseData();
		TopicProduceResponseCollection responses = new TopicProduceResponseCollection();

		for (TopicValidationResult topicValidationResult : topicResults) {
			responses.add(topicValidationResult.toResponse());
		}

		response.setResponses(responses);
		return response;
	}

}