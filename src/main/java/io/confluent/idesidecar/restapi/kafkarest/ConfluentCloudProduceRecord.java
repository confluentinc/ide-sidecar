package io.confluent.idesidecar.restapi.kafkarest;

import static io.confluent.idesidecar.restapi.util.MutinyUtil.uniStage;

import io.confluent.idesidecar.restapi.kafkarest.model.ProduceRequest;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceRequestData;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceResponse;
import io.confluent.idesidecar.restapi.processors.Processor;
import io.confluent.idesidecar.restapi.proxy.KafkaRestProxyContext;
import io.confluent.kafka.serializers.KafkaJsonDeserializer;
import io.smallrye.mutiny.Uni;
import io.vertx.core.Future;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import java.util.Base64;
import java.util.Map;

@ApplicationScoped
public class ConfluentCloudProduceRecord extends GenericProduceRecord {
	private static final Base64.Encoder BASE64_ENCODER = Base64.getEncoder();

	// These are the types supported by Confluent Cloud Kafka REST
	private static final String BINARY_TYPE = "BINARY";
	private static final String JSON_TYPE = "JSON";

	@Inject
	@Named("ccloudProduceProcessor")
	Processor<KafkaRestProxyContext<ProduceRequest, ProduceResponse>,
			Future<KafkaRestProxyContext<ProduceRequest, ProduceResponse>>> ccloudProduceProcessor;

	@Override
	protected Uni<ProduceContext> sendSerializedRecord(ProduceContext c) {

		// Construct a new ProduceRequest that's apprioriate
		var produceRequest = new ProduceRequest();
		produceRequest.setPartitionId(c.produceRequest().getPartitionId());
		produceRequest.setHeaders(c.produceRequest().getHeaders());

		// For key
		if (c.keySchema().isPresent()) {
			produceRequest.setKey(
					ProduceRequestData
							.builder()
							.data(BASE64_ENCODER.encodeToString(c.serializedKey().toByteArray()))
							.type(BINARY_TYPE)
							.build()
			);
		} else {
			produceRequest.setKey(
					ProduceRequestData
							.builder()
							// Deserialize the serialized JSON back into UTF-8
							.data(deserializeJson(c.serializedKey().toByteArray(), true))
							.type(JSON_TYPE)
							.build()
			);
		}

		// For value
		if (c.valueSchema().isPresent()) {
			produceRequest.setValue(
					ProduceRequestData
							.builder()
							.data(BASE64_ENCODER.encodeToString(c.serializedValue().toByteArray()))
							.type(BINARY_TYPE)
							.build()
			);
		} else {
			produceRequest.setValue(
					ProduceRequestData
							.builder()
							// Deserialize the serialized JSON back into UTF-8
							.data(deserializeJson(c.serializedValue().toByteArray(), false))
							.type(JSON_TYPE)
							.build()
			);
		}

		return uniStage(
				() -> ccloudProduceProcessor.process(new KafkaRestProxyContext<>(
						c.connectionId(),
						c.clusterId(),
						c.topicName(),
						produceRequest
				)).toCompletionStage()
		).map(kafkaRestProxyContext -> c
					.with()
					.recordMetadata(convertToRecordMetadata(kafkaRestProxyContext.getResponse()))
					.build()
		);
	}

	private RecordMetadata convertToRecordMetadata(ProduceResponse response) {
		return new RecordMetadata(
				new TopicPartition(response.getTopicName(), response.getPartitionId()),
				response.getOffset(),
				// batchIndex is ignored if baseOffset is set, which we expect to be set for sure
				-1,
				response.getTimestamp().getTime(),
				response.getKey().getSize().intValue(),
				response.getValue().getSize().intValue()
		);
	}

	private Object deserializeJson(byte[] serializedJsonBytes, boolean isKey) {
		try (var kafkaJsonDeserializer = new KafkaJsonDeserializer<>()) {
			kafkaJsonDeserializer.configure(Map.of(), isKey);
			return kafkaJsonDeserializer.deserialize(null, serializedJsonBytes);
		}
	}
}
