package io.confluent.idesidecar.restapi.kafkarest;

import static io.confluent.idesidecar.restapi.util.MutinyUtil.uniItem;

import com.google.protobuf.ByteString;
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
	protected Uni<ProduceContext> sendSerializedRecord(ProduceContext c) {
		return fetchProducerClient(c)
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
