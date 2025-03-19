package io.confluent.idesidecar.restapi.kafkarest;

import static io.confluent.idesidecar.restapi.util.ExceptionUtil.unwrapWithCombinedMessage;
import static io.confluent.idesidecar.restapi.util.MutinyUtil.uniItem;

import io.confluent.idesidecar.restapi.clients.KafkaProducerClients;
import io.confluent.idesidecar.restapi.clients.SchemaRegistryClients;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceRequest;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceResponse;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceResponseData;
import io.confluent.idesidecar.restapi.util.ExceptionUtil;
import io.confluent.idesidecar.restapi.util.MutinyUtil;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaJsonSerializer;
import io.smallrye.mutiny.CompositeException;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.inject.Inject;
import jakarta.ws.rs.BadRequestException;
import java.util.Date;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;

public abstract class GenericProduceRecord {

  @Inject
  SchemaRegistryClients schemaRegistryClients;

  @Inject
  KafkaProducerClients kafkaProducerClients;

  @Inject
  RecordSerializer recordSerializer;
  @Inject
  SchemaManager schemaManager;

  @Inject
  PartitionManager partitionManager;

  @Inject
  TopicManager topicManager;

  public Uni<ProduceResponse> produce(
      String connectionId,
      String clusterId,
      String topicName,
      boolean dryRun,
      ProduceRequest produceRequest
  ) {
    return produce(
        ProduceContext.fromRequest(connectionId, clusterId, topicName, produceRequest),
        dryRun
    );
  }

  public Uni<ProduceResponse> produce(ProduceContext produceContext, boolean dryRun) {
    var uptoDryRun = uniItem(produceContext)
        .chain(this::ensureKeyOrValueDataExists)
        .chain(this::fetchSchemaRegistryClient)
        .chain(this::getSchemas)
        .chain(this::serialize)
        .onFailure(RuntimeException.class)
        .transform(this::lookForRestClientException);

    if (dryRun) {
      return uptoDryRun
          // Return a 200 response with the cluster and topic name set.
          // This should indicate that the request as a whole is valid.
          .map(c -> ProduceResponse
              .builder()
              .clusterId(c.clusterId())
              .topicName(c.topicName())
              .build()
          );
    } else {
      return uptoDryRun
          .chain(this::sendSerializedRecord)
          .onFailure(RuntimeException.class)
          .transform(this::lookForRestClientException)
          .map(this::toProduceResponse);
    }
  }

  private Throwable lookForRestClientException(Throwable throwable) {
    if (throwable.getCause() instanceof RestClientException) {
      return throwable.getCause();
    }

    return throwable;
  }


  /**
   * Ensure that either key or value data is provided in the request. If neither is provided, throw
   * a 400.
   *
   * @param c The context object.
   * @return A Uni that emits the context object if key or value data is provided.
   */
  private Uni<ProduceContext> ensureKeyOrValueDataExists(ProduceContext c) {
    if (c.produceRequest().getKey().getData() == null
        && c.produceRequest().getValue().getData() == null) {
      return Uni.createFrom().failure(
          new BadRequestException("Key and value data cannot both be null")
      );
    }
    return MutinyUtil.uniItem(c);
  }

  /**
   * Fetch the Schema Registry and Kafka Producer clients for the given cluster.
   *
   * @param c The context object.
   * @return A Uni that emits the context object with the clients set.
   */
  private Uni<ProduceContext> fetchSchemaRegistryClient(ProduceContext c) {
    return uniItem(
        () -> schemaRegistryClients.getClientByKafkaClusterId(c.connectionId(), c.clusterId())
    )
        .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
        .map(client -> c
            .with()
            .srClient(client)
            .build()
        );
  }

  /**
   * Use {@link SchemaManager#getSchema} to fetch schema information that may be provided for the
   * key and/or value data.
   *
   * @param c The context object.
   * @return A Uni that emits the context object with key/value schema set (can be empty).
   */
  private Uni<ProduceContext> getSchemas(ProduceContext c) {
    return MutinyUtil.combineUnis(
            () -> schemaManager.getSchema(
                c.srClient(),
                c.topicName(),
                c.produceRequest().getKey(),
                true
            ),
            () -> schemaManager.getSchema(
                c.srClient(),
                c.topicName(),
                c.produceRequest().getValue(),
                false
            )
        )
        .asTuple()
        // The SchemaRegistryClient uses java.net.HttpURLConnection under the hood
        // which is a synchronous I/O blocking client, so we run this on a worker pool of threads.
        .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
        .map(tuple -> c
            .with()
            .keySchema(tuple.getItem1())
            .valueSchema(tuple.getItem2())
            .build()
        );
  }

  /**
   * Use the {@link RecordSerializer#serialize} to serialize the key and value data based on
   * optionally provided schema information. If the computed schema is null, we use the
   * {@link KafkaJsonSerializer} to serialize the data.
   *
   * @param c The context object.
   * @return A Uni that emits the context object with the serialized key and value set.
   */
  private Uni<ProduceContext> serialize(ProduceContext c) {
    return MutinyUtil.combineUnis(
            () -> recordSerializer.serialize(
                c.srClient(),
                c.keySchema(),
                c.topicName(),
                c.produceRequest().getKey().getData(),
                true
            ),
            () -> recordSerializer.serialize(
                c.srClient(),
                c.valueSchema(),
                c.topicName(),
                c.produceRequest().getValue().getData(),
                false
            ))
        // If several failures have been collected,
        // a CompositeException is fired wrapping the different failures.
        .collectFailures()
        .with((key, value) -> c
            .with()
            .serializedKey(key)
            .serializedValue(value)
            .build()
        )
        .onFailure()
        .recoverWithUni(t -> {
          if (t instanceof CompositeException e) {
            return Uni.createFrom().failure(new BadRequestException(
                "Failed to serialize both key and value: %s".formatted(
                    e
                        .getCauses()
                        .stream()
                        .map(ExceptionUtil::unwrapWithCombinedMessage)
                        .map(Throwable::getMessage)
                        .collect(Collectors.joining(", "))
                ), e)
            );
          } else {
            var rootCause = unwrapWithCombinedMessage(t);
            return Uni.createFrom().failure(
                new BadRequestException(rootCause.getMessage(), rootCause)
            );
          }
        });
  }

  protected abstract Uni<ProduceContext> sendSerializedRecord(ProduceContext c);

  protected Set<Header> getRecordHeaders(ProduceRequest produceRequest) {
    return produceRequest
        .getHeaders()
        .stream()
        .map(h -> new RecordHeader(h.getName(), h.getValue()))
        .collect(Collectors.toUnmodifiableSet());
  }

  private ProduceResponse toProduceResponse(ProduceContext c) {
    return ProduceResponse
        .builder()
        .clusterId(c.clusterId())
        .topicName(c.topicName())
        .partitionId(c.recordMetadata().partition())
        .offset(c.recordMetadata().offset())
        .timestamp(new Date(c.recordMetadata().timestamp()))
        .key(toProduceResponseData(c.keySchema(), c.recordMetadata(), true))
        .value(toProduceResponseData(c.valueSchema(), c.recordMetadata(), false))
        .build();
  }

  private ProduceResponseData toProduceResponseData(
      Optional<SchemaManager.RegisteredSchema> schema,
      RecordMetadata metadata,
      boolean isKey
  ) {
    return ProduceResponseData
        .builder()
        .size((long) (isKey ? metadata.serializedKeySize() : metadata.serializedValueSize()))
        .schemaId(schema.map(SchemaManager.RegisteredSchema::schemaId).orElse(null))
        .subject(schema.map(SchemaManager.RegisteredSchema::subject).orElse(null))
        .schemaVersion(schema.map(SchemaManager.RegisteredSchema::schemaVersion).orElse(null))
        .type(schema.map(s -> s.parsedSchema().schemaType()).orElse(null))
        .build();
  }
}