package dev.morling.demos.kroxy.validation.internal;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceRequestData.PartitionProduceData;
import org.apache.kafka.common.message.ProduceRequestData.TopicProduceData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;

import dev.morling.demos.kroxy.validation.RecordSchemaValidationConfig;
import dev.morling.demos.kroxy.validation.RecordSchemaValidationConfig.ValidationConfig;
import dev.morling.demos.kroxy.validation.RecordSchemaValidationConfig.ValidationRule;
import dev.morling.demos.kroxy.validation.internal.model.PartitionValidationResult;
import dev.morling.demos.kroxy.validation.internal.model.PayloadValidationResult;
import dev.morling.demos.kroxy.validation.internal.model.RecordValidationResult;
import dev.morling.demos.kroxy.validation.internal.model.RequestValidationResult;
import dev.morling.demos.kroxy.validation.internal.model.TopicValidationResult;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientFactory;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.ProduceRequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;

public class RecordSchemaValidationFilter implements ProduceRequestFilter {

	private static final byte MAGIC_BYTE = 0x0;

	private static Logger LOGGER = System.getLogger(RecordSchemaValidationFilter.class.getName());

	private final RecordSchemaValidationConfig config;
	private final SchemaRegistryClient registryClient;


	public RecordSchemaValidationFilter(RecordSchemaValidationConfig config) {
		this.config = config;
		this.registryClient = SchemaRegistryClientFactory.newClient(config.schemaRegistryUrl(), 100, List.of(new AvroSchemaProvider(), new JsonSchemaProvider()), Map.of(), Map.of());
	}

	@Override
	public CompletionStage<RequestFilterResult> onProduceRequest(short apiVersion, RequestHeaderData header, ProduceRequestData request, FilterContext context) {
		List<CompletableFuture<TopicValidationResult>> topicFutures = new ArrayList<>();

		for (TopicProduceData topicData : request.topicData()) {
			topicFutures.add(validateTopicData(topicData));
		}

		return allOf(topicFutures).thenCompose(r -> {
			RequestValidationResult result = new RequestValidationResult(r);

			if (result.allValid()) {
				return context.forwardRequest(header, request);
			}
			else {
				return context.requestFilterResultBuilder().shortCircuitResponse(result.toResponse()).completed();
			}
		});
	}

	private CompletableFuture<TopicValidationResult> validateTopicData(TopicProduceData topicData) {
		String topic = topicData.name();
		String keySubjectName = topic + "-key";
		String valueSubjectName = topic + "-value";

		ValidationRule validationRule = getValidationRule(topic);
		if (validationRule == null) {
			return CompletableFuture.completedFuture(TopicValidationResult.valid(topic));
		}

		List<CompletableFuture<PartitionValidationResult>> partitionFutures = new ArrayList<>();

		for (PartitionProduceData partitionData : topicData.partitionData()) {
			partitionFutures.add(validatePartitionData(topic, keySubjectName, valueSubjectName, partitionData, validationRule));
		}

		return allOf(partitionFutures).thenApply(partitionResults -> {
			boolean allValid = true;
			for (PartitionValidationResult partitionResult : partitionResults) {
				if (!partitionResult.allValid()) {
					allValid = false;
					break;
				}
			}

			return new TopicValidationResult(topic, partitionResults, allValid);
		});
	}

	private ValidationRule getValidationRule(String topic) {
		for(ValidationRule topicConfig : config.rules()) {
			if (topicConfig.matches(topic)) {
				return topicConfig;
			}
		}

		return null;
	}

	private CompletableFuture<PartitionValidationResult> validatePartitionData(String topic, String keySubjectName, String valueSubjectName, PartitionProduceData partitionData, ValidationRule validationRule) {
		List<CompletableFuture<RecordValidationResult>> recordFutures = new ArrayList<>();
		int i = 0;
		for (RecordBatch batch : ((AbstractRecords) partitionData.records()).batches()) {
			for (Record record : batch) {
				recordFutures.add(validateRecord(validationRule, topic, keySubjectName, valueSubjectName, record, i));
			}
			i++;
		}

		return allOf(recordFutures).thenApply(recordResults -> {
			boolean allValid = true;

			for (RecordValidationResult r : recordResults) {
				if (!r.isValid()) {
					allValid = false;
					break;
				}
			}

			return new PartitionValidationResult(partitionData.index(), recordResults, allValid);
		});
	}

	private CompletableFuture<RecordValidationResult> validateRecord(ValidationRule validationRule, String topic, String keySubjectName, String valueSubjectName, Record record, int index) {
		return CompletableFuture.supplyAsync(() -> {
			RecordValidator recordValidator = new RecordValidator(validationRule);

			PayloadValidationResult keyValidationResult = validateBytes(recordValidator.keyValidator(), validationRule.keys(), keySubjectName, record.key());

			if (!keyValidationResult.isValid()) {
				return RecordValidationResult.invalid("Invalid key. Reason: " + keyValidationResult.errorMessage(), index);
			}

			PayloadValidationResult valueValidationResult = validateBytes(recordValidator.valueValidator(), validationRule.values(), valueSubjectName, record.value());

			if (!valueValidationResult.isValid()) {
				return RecordValidationResult.invalid("Invalid value. Reason: " + valueValidationResult.errorMessage(), index);
			}

			return RecordValidationResult.valid();
		});
	}

	private <S> PayloadValidationResult validateBytes(PayloadValidator<S> validator, ValidationConfig config, String subjectName, ByteBuffer keyOrValue) {
		try {
			// value is null, or nothing to validate
			if (keyOrValue == null || config == null || !(config.validateId() || config.validatePayload())) {
				return PayloadValidationResult.valid();
			}

			// no embedded schema; should we log a warning?
			if (keyOrValue.get() != MAGIC_BYTE) {
				return PayloadValidationResult.valid();
			}

			int schemaId = keyOrValue.getInt();

			if (config.validateId()) {
				try {
					Collection<String> subjects = registryClient.getAllSubjectsById(schemaId);
					if (!subjects.contains(subjectName)) {
						return PayloadValidationResult.invalid("Schema id %s invalid for subject '%s'".formatted(schemaId, subjectName));
					}
				}
				catch (RestClientException e) {
					if (e.getStatus() == 404) {
						return PayloadValidationResult.invalid("%s is not a known schema id".formatted(schemaId));
					}
					else {
						throw e;
					}
				}
			}

			@SuppressWarnings("unchecked")
			S schema = (S) registryClient.getSchemaBySubjectAndId(subjectName, schemaId);

			return validator.validate(getBytesFromBuffer(keyOrValue), schema);

		}
		catch (Exception e) {
			LOGGER.log(Level.WARNING, "Couldn't validate record, ignoring.", e);

		}

		return PayloadValidationResult.valid();
	}

//	public static boolean validateAvro(byte[] avroBytes, Schema schema) {
//	    try {
//	        SeekableByteArrayInput input = new SeekableByteArrayInput(avroBytes);
//	        GenericDatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
//	        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(input, reader);
//
//	        while (dataFileReader.hasNext()) {
//	            GenericRecord record = dataFileReader.next();
//	            System.out.println("Read record: " + record);
//	        }
//
//	        dataFileReader.close();
//	        return true;
//	    } catch (IOException e) {
//	        System.err.println("Validation failed: " + e.getMessage());
//	        return false;
//	    }
//	}

	public static byte[] getBytesFromBuffer(ByteBuffer buffer) {
		byte[] array = new byte[buffer.remaining()];
		buffer.get(array);
		return array;
	}

	private static <T> CompletableFuture<List<T>> allOf(List<CompletableFuture<T>> futures) {
		return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
				.thenApply(v -> futures.stream()
						.map(CompletableFuture::join)
						.collect(Collectors.toList()));
	}
}
