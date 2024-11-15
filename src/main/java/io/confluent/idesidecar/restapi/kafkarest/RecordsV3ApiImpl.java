package io.confluent.idesidecar.restapi.kafkarest;

import static io.confluent.idesidecar.restapi.util.MutinyUtil.combineUnis;
import static io.confluent.idesidecar.restapi.util.MutinyUtil.uniItem;
import static io.confluent.idesidecar.restapi.util.MutinyUtil.uniStage;
import static io.confluent.idesidecar.restapi.util.RequestHeadersConstants.CONNECTION_ID_HEADER;

import com.google.protobuf.ByteString;
import io.confluent.idesidecar.restapi.cache.KafkaProducerClients;
import io.confluent.idesidecar.restapi.cache.SchemaRegistryClients;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceRequest;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceResponse;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceResponseData;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.soabase.recordbuilder.core.RecordBuilder;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.validation.Valid;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import java.time.Instant;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;


@RequestScoped
@Path("/internal/kafka/v3/clusters/{cluster_id}/topics/{topic_name}/records")
public class RecordsV3ApiImpl {
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

  @POST
  @Consumes({ "application/json" })
  @Produces({ "application/json", "text/html" })
  public Uni<ProduceResponse> produceRecord(
      @HeaderParam(CONNECTION_ID_HEADER) String connectionId,
      @PathParam("cluster_id") String clusterId,
      @PathParam("topic_name") String topicName,
      @Valid ProduceRequest produceRequest
  ) {
    return uniItem(ProduceContext.fromRequest(connectionId, clusterId, topicName, produceRequest))
        .chain(this::ensureTopicPartitionExists)
        .chain(this::ensureKeyOrValueDataExists)
        .chain(this::fetchClients)
        .chain(this::getSchemas)
        .chain(this::serialize)
        .chain(this::sendSerializedRecord)
        .onFailure(RuntimeException.class)
        .transform(this::unwrapRootCause)
        .map(this::toProduceResponse);
  }

  private Throwable unwrapRootCause(Throwable throwable) {
    if (throwable.getCause() instanceof RestClientException) {
      return throwable.getCause();
    }

    return throwable;
  }

  /**
   * Check that the topic-partition exists. If partition id was not provided,
   * we simply pass through.
   * @param c The context object.
   * @return  A Uni that emits the context object after checking the partition. The context object
   *          is left unchanged in all cases.
   */
  private Uni<ProduceContext> ensureTopicPartitionExists(ProduceContext c) {
    return topicManager
        // First, check that the topic exists
        .getKafkaTopic(c.clusterId, c.topicName, false)
        // Then, check that the partition exists, if provided
        .chain(ignored -> Optional
            .ofNullable(c.produceRequest.getPartitionId())
            .map(partitionId ->
                partitionManager.getKafkaPartition(c.clusterId, c.topicName, partitionId)
            )
            .orElse(Uni.createFrom().nullItem()))
        .onItem().transform(ignored -> c);
  }

  /**
   * Ensure that either key or value data is provided in the request. If neither is provided,
   * throw a 400.
   * @param c The context object.
   * @return  A Uni that emits the context object if key or value data is provided.
   */
  private Uni<ProduceContext> ensureKeyOrValueDataExists(ProduceContext c) {
    if (c.produceRequest.getKey().getData() == null
        && c.produceRequest.getValue().getData() == null) {
      return Uni.createFrom().failure(
          new BadRequestException("Key and value data cannot both be null")
      );
    }
    return uniItem(c);
  }

  /**
   * Fetch the Schema Registry and Kafka Producer clients for the given cluster.
   * @param c The context object.
   * @return  A Uni that emits the context object with the clients set.
   */
  private Uni<ProduceContext> fetchClients(ProduceContext c) {
    return combineUnis(
        () -> schemaRegistryClients.getClientByKafkaClusterId(c.connectionId, c.clusterId),
        () -> kafkaProducerClients.getClient(c.connectionId, c.clusterId)
    )
        .asTuple()
        // The getClient* methods may end up performing HTTP requests to fetch cluster information,
        // so we run this on a worker pool of threads.
        .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
        .map(tuple -> c
            .with()
            .srClient(tuple.getItem1())
            .producer(tuple.getItem2())
            .build()
        );
  }

  /**
   * Use {@link SchemaManager#getSchema} to fetch schema information that may be provided
   * for the key and/or value data.
   * @param c The context object.
   * @return  A Uni that emits the context object with key/value schema set (can be empty).
   */
  private Uni<ProduceContext> getSchemas(ProduceContext c) {
    return combineUnis(
        () -> schemaManager.getSchema(
            c.srClient,
            c.topicName,
            c.produceRequest.getKey(),
            true
        ),
        () -> schemaManager.getSchema(
            c.srClient,
            c.topicName,
            c.produceRequest.getValue(),
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
   * Use the {@link RecordSerializer#serialize} to serialize the key and value data
   * based on optionally provided schema information. If the computed schema is null,
   * we use the {@link io.confluent.kafka.serializers.KafkaJsonSerializer} to serialize the data.
   * @param c The context object.
   * @return  A Uni that emits the context object with the serialized key and value set.
   */
  private Uni<ProduceContext> serialize(ProduceContext c) {
    return combineUnis(
        () -> recordSerializer.serialize(
            c.srClient,
            c.keySchema,
            c.topicName,
            c.produceRequest.getKey().getData(),
            true
        ),
        () -> recordSerializer.serialize(
            c.srClient,
            c.valueSchema,
            c.topicName,
            c.produceRequest.getValue().getData(),
            false
        ))
        .with((key, value) -> c
            .with()
            .serializedKey(key)
            .serializedValue(value)
            .build()
        );
  }

  private Uni<ProduceContext> sendSerializedRecord(ProduceContext c) {
    return uniStage(sendSerializedRecord(
            c.producer,
            c.topicName,
            c.produceRequest.getPartitionId(),
            c.produceRequest.getTimestamp(),
            Optional.ofNullable(c.serializedKey()).map(ByteString::toByteArray).orElse(null),
            Optional.ofNullable(c.serializedValue()).map(ByteString::toByteArray).orElse(null)
        ))
        .map(recordMetadata -> c
            .with()
            .recordMetadata(recordMetadata)
            .build()
        );
  }

  private static CompletableFuture<RecordMetadata> sendSerializedRecord(
      KafkaProducer<byte[], byte[]> producer,
      String topicName,
      Integer partitionId,
      Date timestamp,
      byte[] key,
      byte[] value
  ) {
    var completableFuture = new CompletableFuture<RecordMetadata>();
    producer.send(
        new ProducerRecord<>(
            topicName,
            partitionId,
            Optional.ofNullable(timestamp).orElse(Date.from(Instant.now())).getTime(),
            key,
            value
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

  private ProduceResponse toProduceResponse(ProduceContext c) {
    return ProduceResponse
        .builder()
        .clusterId(c.clusterId)
        .topicName(c.topicName)
        .partitionId(c.recordMetadata.partition())
        .offset(c.recordMetadata.offset())
        .timestamp(new Date(c.recordMetadata.timestamp()))
        .key(toProduceResponseData(c.keySchema(), c.recordMetadata, true))
        .value(toProduceResponseData(c.valueSchema(), c.recordMetadata, false))
        .build();
  }

  private static ProduceResponseData toProduceResponseData(
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

  /**
   * Context object for the produce record operation. This object is used to pass around
   * the various request parameters, clients, and computed fields from intermediate steps.
   */
  @RecordBuilder
  record ProduceContext(
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
  ) implements RecordsV3ApiImplProduceContextBuilder.With {
    static ProduceContext fromRequest(
        String connectionId,
        String clusterId,
        String topicName,
        ProduceRequest produceRequest
    ) {
      return RecordsV3ApiImplProduceContextBuilder.ProduceContext(
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
}
