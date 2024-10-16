package io.confluent.idesidecar.restapi.kafkarest;

import static io.confluent.idesidecar.restapi.util.MutinyUtil.combineUnis;
import static io.confluent.idesidecar.restapi.util.MutinyUtil.uniItem;
import static io.confluent.idesidecar.restapi.util.MutinyUtil.uniStage;
import static io.confluent.idesidecar.restapi.util.RequestHeadersConstants.CONNECTION_ID_HEADER;

import com.google.protobuf.ByteString;
import io.confluent.idesidecar.restapi.cache.KafkaProducerClients;
import io.confluent.idesidecar.restapi.cache.SchemaRegistryClients;
import io.confluent.idesidecar.restapi.kafkarest.api.RecordsV3Api;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceBatchRequest;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceBatchResponse;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceRequest;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceRequestData;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceResponse;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceResponseData;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.vertx.core.http.HttpServerRequest;
import jakarta.annotation.Nullable;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.BadRequestException;
import java.time.Instant;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;


@RequestScoped
public class RecordsV3ApiImpl implements RecordsV3Api {
  @Inject
  RecordSerializer recordSerializer;

  @Inject
  HttpServerRequest request;

  @Inject
  SchemaManager schemaManager;

  @Inject
  SchemaRegistryClients schemaRegistryClients;

  @Inject
  KafkaProducerClients kafkaProducerClients;

  @Inject
  PartitionManager partitionManager;

  Supplier<String> connectionId = () -> request.getHeader(CONNECTION_ID_HEADER);

  @Override
  public Uni<ProduceResponse> produceRecord(
      String clusterId,
      String topicName,
      ProduceRequest produceRequest
  ) {
    return Optional
        .ofNullable(produceRequest.getPartitionId())
        .map(partitionId -> partitionManager.getKafkaPartition(clusterId, topicName, partitionId))
        .orElse(Uni.createFrom().nullItem())
        .onItem()
        .transformToUni(ignored -> combineUnis(
            () -> schemaRegistryClients.getClientByKafkaClusterId(connectionId.get(), clusterId),
            () -> kafkaProducerClients.getClient(connectionId.get(), clusterId)
        )
        .asTuple()
        .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
        .onItem().transformToUni(tuple -> produceRecord(
                clusterId, topicName, produceRequest, tuple.getItem1(), tuple.getItem2()
            )
        ));
  }

  private Uni<ProduceResponse> produceRecord(
      String clusterId,
      String topicName,
      ProduceRequest produceRequest,
      @Nullable SchemaRegistryClient schemaRegistryClient,
      KafkaProducer<byte[], byte[]> producer
  ) {
    return combineUnis(
        getSchema(schemaRegistryClient, topicName, produceRequest.getKey(), true),
        getSchema(schemaRegistryClient, topicName, produceRequest.getValue(), false)
    )
        .with((keySchema, valueSchema) -> new ProducePipeline()
                .withKeySchema(keySchema)
                .withValueSchema(valueSchema)
        )
        .chain(pipeline -> combineUnis(
            () -> recordSerializer.serialize(
                schemaRegistryClient,
                Optional
                    .ofNullable(pipeline.keySchema())
                    .map(RegisteredSchema::format)
                    .orElse(SchemaManager.SchemaFormat.JSON),
                Optional
                    .ofNullable(pipeline.keySchema())
                    .map(RegisteredSchema::parsedSchema).orElse(null),
                topicName,
                produceRequest.getKey().getData()
            ),
            () -> recordSerializer.serialize(
                schemaRegistryClient,
                Optional
                    .ofNullable(pipeline.valueSchema())
                    .map(RegisteredSchema::format)
                    .orElse(SchemaManager.SchemaFormat.JSON),
                Optional
                    .ofNullable(pipeline.valueSchema())
                    .map(RegisteredSchema::parsedSchema).orElse(null),
                topicName,
                produceRequest.getValue().getData()
            ))
            .with((key, value) -> pipeline.withKey(key).withValue(value))
        )
        .chain(pipeline ->
            uniStage(sendRecord(
                producer,
                topicName,
                produceRequest.getPartitionId(),
                produceRequest.getTimestamp(),
                pipeline.serializedKey().toByteArray(),
                pipeline.serializedValue().toByteArray())
            )
            .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
            .onItem()
            .transform(metadata -> toProduceResponse(clusterId, topicName, pipeline, metadata))
        );
  }

  private static CompletableFuture<RecordMetadata> sendRecord(
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
            Optional.ofNullable(timestamp)
                .orElse(Date.from(Instant.now()))
                .getTime(),
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

  private static ProduceResponse toProduceResponse(
      String clusterId,
      String topicName,
      ProducePipeline pipeline,
      RecordMetadata metadata
  ) {
    return ProduceResponse
        .builder()
        .clusterId(clusterId)
        .topicName(topicName)
        .partitionId(metadata.partition())
        .offset(metadata.offset())
        .timestamp(new Date(metadata.timestamp()))
        .key(toProduceResponseData(pipeline.keySchema(), metadata, true))
        .value(toProduceResponseData(pipeline.valueSchema(), metadata, false))
        .build();
  }

  private static ProduceResponseData toProduceResponseData(
      RegisteredSchema schema,
      RecordMetadata metadata,
      boolean isKey
  ) {
    return ProduceResponseData
        .builder()
        .size((long) (isKey ? metadata.serializedKeySize() : metadata.serializedValueSize()))
        .schemaId(Optional
            .ofNullable(schema)
            .map(RegisteredSchema::schemaId).orElse(null))
        .subject(Optional
            .ofNullable(schema)
            .map(RegisteredSchema::subject).orElse(null))
        .schemaVersion(Optional
            .ofNullable(schema)
            .map(RegisteredSchema::schemaVersion).orElse(null))
        .type(Optional
            .ofNullable(schema)
            .map(s -> s.parsedSchema().schemaType()).orElse(null))
        .build();
  }

  @Override
  public Uni<ProduceBatchResponse> produceRecordsBatch(
      String clusterId,
      String topicName,
      ProduceBatchRequest produceBatchRequest
  ) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  private Uni<RegisteredSchema> getSchema(
      SchemaRegistryClient schemaRegistryClient,
      String topicName,
      ProduceRequestData produceRequestData,
      boolean isKey
  ) {
    if (supportsSchemaVersion(produceRequestData)) {
      return getSchemaFromSchemaVersion(
          schemaRegistryClient,
          topicName,
          produceRequestData.getSchemaVersion(),
          isKey
      );
    }
    return Uni.createFrom().nullItem();
  }

  /**
   * Check if the ProduceRequestData contains a non-null subject and schema version.
   */
  static boolean supportsSchemaVersion(ProduceRequestData produceRequestData) {
    return produceRequestData.getSchemaVersion() != null;
  }

  private Uni<RegisteredSchema> getSchemaFromSchemaVersion(
      @Nullable SchemaRegistryClient schemaRegistryClient,
      String topicName,
      Integer schemaVersion,
      boolean isKey
  ) {
    if (schemaRegistryClient == null) {
      if (schemaVersion != null) {
        throw new BadRequestException("Schema version requested without a schema registry client");
      }
      return Uni.createFrom().nullItem();
    }

    return uniItem(() -> schemaRegistryClient.getByVersion(
        // Note: We default to TopicNameStrategy for the subject name for the sake of simplicity.
        (isKey ? topicName + "-key" : topicName + "-value"),
        schemaVersion,
        // do not lookup deleted schemas
        false
    ))
        .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
        .onFailure(RuntimeException.class)
        .transform(Throwable::getCause)
        .onItem()
        .ifNotNull()
        .transform(schema -> new RegisteredSchema(
            schema.getSubject(),
            schema.getId(),
            schema.getVersion(),
            schemaManager.parseSchema(schema))
        );
  }

  private record ProducePipeline(
      RegisteredSchema keySchema,
      RegisteredSchema valueSchema,
      ByteString serializedKey,
      ByteString serializedValue,
      Producer<byte[], byte[]> producer,
      SchemaRegistryClient srClient
  ) {
    ProducePipeline() {
      this(
          null, null, null, null, null, null);
    }

    ProducePipeline withKeySchema(RegisteredSchema keySchema) {
      return new ProducePipeline(
          keySchema, valueSchema, serializedKey, serializedValue, producer, srClient);
    }

    ProducePipeline withValueSchema(RegisteredSchema valueSchema) {
      return new ProducePipeline(
          keySchema, valueSchema, serializedKey, serializedValue, producer, srClient);
    }

    ProducePipeline withKey(ByteString key) {
      return new ProducePipeline(
          keySchema, valueSchema, key, serializedValue, producer, srClient);
    }

    ProducePipeline withValue(ByteString value) {
      return new ProducePipeline(
          keySchema, valueSchema, serializedKey, value, producer, srClient);
    }
  }

  private record RegisteredSchema(
      String subject,
      Integer schemaId,
      Integer schemaVersion,
      ParsedSchema parsedSchema
  ) {

    SchemaManager.SchemaFormat format() {
      return SchemaManager.SchemaFormat.fromSchemaType(parsedSchema.schemaType());
    }
  }
}
