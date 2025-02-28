package io.confluent.idesidecar.restapi.kafkarest;

import static io.confluent.idesidecar.restapi.util.MutinyUtil.uniItem;

import com.google.protobuf.ByteString;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceRequest;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceResponse;
import io.confluent.idesidecar.restapi.util.MutinyUtil;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import java.time.Instant;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

@ApplicationScoped
public class NativeProduceRecord extends GenericProduceRecord {

	@Override
	public Uni<ProduceResponse> produce(
			String connectionId,
			String clusterId,
			String topicName,
			boolean dryRun,
			ProduceRequest produceRequest
	) {
		var produceContext = ProduceContext.fromRequest(connectionId, clusterId, topicName, produceRequest);
		return ensureTopicPartitionExists(produceContext)
				.chain(() -> super.produce(produceContext, dryRun));
	}

	@Override
	protected Uni<ProduceContext> sendSerializedRecord(ProduceContext c) {
		return ensureTopicPartitionExists(c)
				.chain(this::fetchProducerClient)
				// ctx is just c but with the Producer instance set
				.chain(ctx ->
						MutinyUtil.uniStage(sendSerializedRecord(
								ctx.producer(),
								ctx.topicName(),
								ctx.produceRequest().getPartitionId(),
								ctx.produceRequest().getTimestamp(),
								Optional.ofNullable(ctx.serializedKey()).map(ByteString::toByteArray).orElse(null),
								Optional.ofNullable(ctx.serializedValue()).map(ByteString::toByteArray).orElse(null),
								getRecordHeaders(ctx.produceRequest())
						)).map(recordMetadata -> c
										.with()
										.recordMetadata(recordMetadata)
										.build()
						)
				);
	}

	private Uni<ProduceContext> fetchProducerClient(ProduceContext c) {
		return uniItem(
				() -> kafkaProducerClients.getClient(c.connectionId(), c.clusterId())
		)
				.runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
				.map(producer -> c
						.with()
						.producer(producer)
						.build()
				);
	}

	/**
	 * Check that the topic-partition exists. If partition id was not provided,
	 * we simply pass through.
	 *
	 * @param c The context object.
	 * @return A Uni that emits the context object after checking the partition. The context object
	 * is left unchanged in all cases.
	 */
	private Uni<ProduceContext> ensureTopicPartitionExists(ProduceContext c) {
		return topicManager
				// First, check that the topic exists
				.getKafkaTopic(c.clusterId(), c.topicName(), false)
				// Then, check that the partition exists, if provided
				.chain(ignored -> Optional
						.ofNullable(c.produceRequest().getPartitionId())
						.map(partitionId ->
								partitionManager.getKafkaPartition(c.clusterId(), c.topicName(), partitionId)
						)
						.orElse(Uni.createFrom().nullItem()))
				.onItem().transform(ignored -> c);
	}

	private CompletableFuture<RecordMetadata> sendSerializedRecord(
			KafkaProducer<byte[], byte[]> producer,
			String topicName,
			Integer partitionId,
			Date timestamp,
			byte[] key,
			byte[] value,
			Iterable<Header> headers
	) {
		var completableFuture = new CompletableFuture<RecordMetadata>();
		producer.send(
				new ProducerRecord<>(
						topicName,
						partitionId,
						Optional.ofNullable(timestamp).orElse(Date.from(Instant.now())).getTime(),
						key,
						value,
						headers
				),
				(metadata, exception) -> {
					if (exception != null) {
						completableFuture.completeExceptionally(exception);
					} else {
						completableFuture.complete(metadata);
					}
				});
		return completableFuture;
	}
}
