package io.confluent.idesidecar.restapi.kafkarest;

import com.google.protobuf.ByteString;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceRequest;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.soabase.recordbuilder.core.RecordBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Optional;

/**
 * Context object for the produce record operation. This object is used to pass around
 * the various request parameters, clients, and computed fields from intermediate steps.
 */
@RecordBuilder
public record ProduceContext(
		// Provided
		String connectionId,
		String clusterId,
		String topicName,
		ProduceRequest produceRequest,
		// Fetched
		KafkaProducer<byte[], byte[]> producer,
		SchemaRegistryClient srClient,
		// Computed fields
		Optional<SchemaManager.RegisteredSchema> keySchema,
		Optional<SchemaManager.RegisteredSchema> valueSchema,
		ByteString serializedKey,
		ByteString serializedValue,
		RecordMetadata recordMetadata
) implements ProduceContextBuilder.With {
	static ProduceContext fromRequest(
			String connectionId,
			String clusterId,
			String topicName,
			ProduceRequest produceRequest
	) {
		return ProduceContextBuilder.ProduceContext(
				connectionId,
				clusterId,
				topicName,
				produceRequest,
				null,
				null,
				Optional.empty(),
				Optional.empty(),
				null,
				null,
				null
		);
	}
}
