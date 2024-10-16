package io.confluent.idesidecar.restapi.kafkarest;

import static io.confluent.idesidecar.restapi.util.MutinyUtil.uniItem;
import static io.confluent.idesidecar.restapi.util.RequestHeadersConstants.CONNECTION_ID_HEADER;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import io.confluent.idesidecar.restapi.cache.ClusterCache;
import io.confluent.idesidecar.restapi.cache.SchemaRegistryClients;
import io.confluent.idesidecar.restapi.kafkarest.api.RecordsV3Api;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceBatchRequest;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceBatchResponse;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceRequest;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceRequestData;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceResponse;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceResponseData;
import io.confluent.idesidecar.restapi.models.graph.KafkaCluster;
import io.confluent.idesidecar.restapi.models.graph.SchemaRegistry;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.json.JsonSchemaUtils;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaUtils;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer;
import io.quarkus.logging.Log;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.vertx.core.http.HttpServerRequest;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.InternalServerErrorException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;


// TODO: resolve this
@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
@RequestScoped
public class RecordsV3ApiImpl implements RecordsV3Api {
  @Inject
  ClusterCache clusterCache;

  @Inject
  HttpServerRequest request;

  @Inject
  SchemaRegistryClients schemaRegistryClients;

  Supplier<String> connectionId = () -> request.getHeader(CONNECTION_ID_HEADER);

  record ProducePipeline(
      Optional<SchemaDTO> keySchema,
      Optional<SchemaDTO> valueSchema,
      ByteString key,
      ByteString value,
      Producer<byte[], byte[]> producer
  ) {
    ProducePipeline() {
      this(
          Optional.empty(), Optional.empty(), null, null, null);
    }

    ProducePipeline withKeySchema(Optional<SchemaDTO> keySchema) {
      return new ProducePipeline(
          keySchema, valueSchema, key, value, producer);
    }

    ProducePipeline withValueSchema(Optional<SchemaDTO> valueSchema) {
      return new ProducePipeline(
          keySchema, valueSchema, key, value, producer);
    }

    ProducePipeline withKey(ByteString key) {
      return new ProducePipeline(
          keySchema, valueSchema, key, value, producer);
    }

    ProducePipeline withValue(ByteString value) {
      return new ProducePipeline(
          keySchema, valueSchema, key, value, producer);
    }

    ProducePipeline withProducer(Producer<byte[], byte[]> producer) {
      return new ProducePipeline(
          keySchema, valueSchema, key, value, producer);
    }
  }

  @Override
  public Uni<ProduceResponse> produceRecord(
      String clusterId,
      String topicName,
      ProduceRequest produceRequest
  ) {
    return getSchemaRegistryClient(clusterId)
        .onItem()
        .transformToUni(schemaRegistryClient -> Uni
        .combine()
        .all()
        .unis(
            getSchema(schemaRegistryClient, produceRequest.getKey()),
            getSchema(schemaRegistryClient, produceRequest.getValue())
        )
        .asTuple()
        .onItem()
        .transform(tuple -> new ProducePipeline()
            .withKeySchema(tuple.getItem1())
            .withValueSchema(tuple.getItem2())
        )
        .onItem()
        .transformToUni(pipeline -> Uni
            .combine()
            .all()
            .unis(
                serialize(
                    schemaRegistryClient, pipeline.keySchema(), topicName, produceRequest.getKey()),
                serialize(
                    schemaRegistryClient,
                    pipeline.valueSchema(),
                    topicName,
                    produceRequest.getValue()
                )
            )
            .asTuple()
            .onItem().transform(tuple ->
                pipeline.withKey(tuple.getItem1()).withValue(tuple.getItem2())
            )
        )
        .onItem()
        .transformToUni(pipeline -> Uni
            .createFrom()
            .item(() -> createProducer(clusterId))
            .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
            .onItem().transform(pipeline::withProducer)
        )
        .onItem()
        // Convert the Future from producer.send() to a Uni
        .transformToUni(pipeline -> Uni.createFrom()
            .completionStage(() -> {
              CompletableFuture<RecordMetadata> completableFuture = new CompletableFuture<>();
              pipeline.producer().send(
                  new ProducerRecord<>(
                      topicName,
                      produceRequest.getPartitionId(),
                      Optional.ofNullable(produceRequest.getTimestamp())
                          .orElse(Date.from(Instant.now()))
                          .getTime(),
                      pipeline.key().toByteArray(),
                      pipeline.value().toByteArray()
                  ),
                  (metadata, exception) -> {
                    if (exception != null) {
                      completableFuture.completeExceptionally(exception);
                    } else {
                      completableFuture.complete(metadata);
                    }
                  });
              return completableFuture;
            })
            // Run the producer.send() call on the default worker pool since it may block
            .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
            .onItem()
            // Process the metadata and create a ProduceResponse
            .transform(metadata -> ProduceResponse.builder()
                .clusterId(clusterId)
                .topicName(topicName)
                .partitionId(metadata.partition())
                .offset(metadata.offset())
                .timestamp(new Date(metadata.timestamp()))
                .key(
                    ProduceResponseData
                        .builder()
                        .size((long) metadata.serializedKeySize())
                        .schemaId(
                            pipeline.keySchema().map(SchemaDTO::schemaId).orElse(null))
                        .subject(
                            pipeline.keySchema().map(SchemaDTO::subject).orElse(null))
                        .schemaVersion(
                            pipeline.keySchema().map(SchemaDTO::schemaVersion).orElse(null))
                        .type(
                            pipeline.keySchema().map(
                                s -> s.parsedSchema().schemaType()).orElse(null))
                        .build()
                )
                .value(
                    ProduceResponseData
                        .builder()
                        .size((long) metadata.serializedValueSize())
                        .schemaId(pipeline
                            .valueSchema().map(SchemaDTO::schemaId).orElse(null))
                        .subject(pipeline
                            .valueSchema().map(SchemaDTO::subject).orElse(null))
                        .schemaVersion(pipeline
                            .valueSchema().map(SchemaDTO::schemaVersion).orElse(null))
                        .type(pipeline
                            .valueSchema().map(
                                s -> s.parsedSchema().schemaType()).orElse(null))
                        .build()
                )
                .build()
            )));
  }

  private Producer<byte[], byte[]> createProducer(String clusterId) {
    var kafkaCluster = clusterCache.getKafkaCluster(connectionId.get(), clusterId);
    var schemaRegistryCluster = Optional.of(
        clusterCache.getSchemaRegistryForKafkaCluster(connectionId.get(), kafkaCluster));

    var props = new Properties();
    props.put("bootstrap.servers", kafkaCluster.bootstrapServers());
    schemaRegistryCluster.ifPresent(schemaRegistry ->
        props.put("schema.registry.url", schemaRegistry.uri())
    );

    return new KafkaProducer<>(
        props, new ByteArraySerializer(), new ByteArraySerializer()
    );
  }

  private Uni<SchemaRegistryClient> getSchemaRegistryClient(String kafkaClusterId) {
    return uniItem((Supplier<KafkaCluster>)
        () -> clusterCache.getKafkaCluster(connectionId.get(), kafkaClusterId))
        .chain(kafkaClusterSupplier -> uniItem((Supplier<SchemaRegistry>)
            () -> clusterCache.getSchemaRegistryForKafkaCluster(
                connectionId.get(), kafkaClusterSupplier.get()
            )
        ))
        .map(Supplier::get)
        .chain(schemaRegistry -> uniItem(
            schemaRegistryClients.getClient(connectionId.get(), schemaRegistry.id())
        ))
        .runSubscriptionOn(Infrastructure.getDefaultWorkerPool());
  }

  @Override
  public Uni<ProduceBatchResponse> produceRecordsBatch(
      String clusterId,
      String topicName,
      ProduceBatchRequest produceBatchRequest
  ) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  record SchemaDTO(
      String subject,
      Integer schemaId,
      Integer schemaVersion,
      SchemaFormat format,
      ParsedSchema parsedSchema
  ) {

  }

  private Uni<Optional<SchemaDTO>> getSchema(
      SchemaRegistryClient schemaRegistryClient, ProduceRequestData produceRequestData) {
    if (supportsSchemaVersion(produceRequestData)) {
      return getSchemaFromSchemaVersion(
          schemaRegistryClient,
          produceRequestData.getSubject(),
          produceRequestData.getSchemaVersion()
      );
    }
    return Uni.createFrom().item(Optional.empty());
  }

  /**
   * Check if the ProduceRequestData contains a non-null subject and schema version.
   */
  static boolean supportsSchemaVersion(ProduceRequestData produceRequestData) {
    return produceRequestData.getSubject() != null && produceRequestData.getSchemaVersion() != null;
  }

  private Uni<Optional<SchemaDTO>> getSchemaFromSchemaVersion(
      SchemaRegistryClient schemaRegistryClient, String subject, int schemaVersion
  ) {
    return uniItem((Supplier<Schema>) () -> schemaRegistryClient.getByVersion(
        subject,
        schemaVersion,
        // do not lookup deleted schemas
        false
    ))
        // Run the supplier on the default worker pool since it may block
        .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
        .map(Supplier::get)
        .onFailure(RuntimeException.class)
        .transform(e -> {
          Log.errorf(e, "Error fetching schema %s version %d", subject, schemaVersion);
          if (e.getCause() instanceof RestClientException) {
            return new BadRequestException("Schema not found", e);
          } else {
            return new InternalServerErrorException(
                "Error fetching schema %s version %d".formatted(subject, schemaVersion), e);
          }
        })
        .onItem()
        .ifNull()
        .failWith(new InternalServerErrorException(
            "Error fetching schema %s version %d".formatted(subject, schemaVersion)))
        .onItem()
        .ifNotNull()
        .transformToUni(this::parseSchema);
  }

  private Uni<Optional<SchemaDTO>> parseSchema(Schema schema) {
    var schemaFormat = SchemaFormat.fromSchemaType(schema.getSchemaType());
    var schemaProvider = schemaFormat
        .schemaProvider()
        .orElseThrow(() ->
            new IllegalArgumentException("Schema type has no provider: " + schema.getSchemaType()));
    var parsedSchema = schemaProvider
        .parseSchema(schema, false)
        .orElseThrow(() -> new BadRequestException("Failed to parse schema"));
    return uniItem(Optional.of(new SchemaDTO(
        schema.getSubject(),
        schema.getId(),
        schema.getVersion(),
        schemaFormat,
        parsedSchema
    )));
  }

  enum SchemaFormat {
    AVRO {
      private final SchemaProvider schemaProvider = new AvroSchemaProvider();

      @Override
      Optional<SchemaProvider> schemaProvider() {
        return Optional.of(schemaProvider);
      }
    },
    JSON {
      @Override
      Optional<SchemaProvider> schemaProvider() {
        return Optional.empty();
      }
    },
    PROTOBUF {
      private final SchemaProvider schemaProvider = new ProtobufSchemaProvider();

      @Override
      Optional<SchemaProvider> schemaProvider() {
        return Optional.of(schemaProvider);
      }
    },
    JSONSCHEMA {
      private final SchemaProvider schemaProvider = new JsonSchemaProvider();

      @Override
      Optional<SchemaProvider> schemaProvider() {
        return Optional.of(schemaProvider);
      }
    },
    STRING {
      @Override
      Optional<SchemaProvider> schemaProvider() {
        return Optional.empty();
      }
    },
    BINARY {
      @Override
      Optional<SchemaProvider> schemaProvider() {
        return Optional.empty();
      }
    };

    abstract Optional<SchemaProvider> schemaProvider();

    /**
     * Get the SchemaFormat for the given schema type. Only formats with a schema provider are
     * supported.
     * @param schemaType the schema type
     * @return           the SchemaFormat
     */
    static SchemaFormat fromSchemaType(String schemaType) {
      return Arrays.stream(values())
          .filter(value -> {
            if (value.schemaProvider().isPresent()) {
              return value.schemaProvider().get().schemaType().equals(schemaType);
            } else {
              return false;
            }
          })
          .findFirst()
          .orElseThrow(() -> new IllegalArgumentException("Illegal schema type: " + schemaType));
    }
  }

  private static final ObjectMapper objectMapper = new ObjectMapper();

  private Uni<ByteString> serialize(
      SchemaRegistryClient client,
      Optional<SchemaDTO> schemaDTO,
      String topicName,
      ProduceRequestData produceRequestData
  ) {
    if (schemaDTO.isEmpty()) {
      return Uni.createFrom().item(
          ByteString.copyFrom(produceRequestData.getData().toString().getBytes())
      );
    }

    return uniItem(schemaDTO.get())
        .onItem()
        .transformToUni(schema -> switch (schema.format) {
          case AVRO -> uniItem(serializeAvro(
              client,
              schema,
              topicName,
              objectMapper.valueToTree(produceRequestData.getData()))
          );
          case JSONSCHEMA -> uniItem(serializeJsonSchema(
              client,
              schema,
              topicName,
              objectMapper.valueToTree(produceRequestData.getData()))
          );
          case PROTOBUF -> uniItem(serializeProtobuf(
              client,
              schema,
              topicName,
              objectMapper.valueToTree(produceRequestData.getData()))
          );
          default -> Uni.createFrom().failure(new BadRequestException("Unsupported schema format"));
        });
  }

  private ByteString serializeAvro(
      SchemaRegistryClient client, SchemaDTO schemaDTO, String topicName, JsonNode data) {
    final KafkaAvroSerializer avroSerializer = new KafkaAvroSerializer(client);
    AvroSchema schema = (AvroSchema) schemaDTO.parsedSchema;
    Object record;
    try {
      record = AvroSchemaUtils.toObject(data, schema);
    } catch (Exception e) {
      throw new BadRequestException("Failed to parse Avro data", e);
    }

    return ByteString.copyFrom(avroSerializer.serialize(topicName, record));
  }

  private ByteString serializeJsonSchema(
      SchemaRegistryClient client, SchemaDTO schemaDTO, String topicName, JsonNode data
  ) {
    final KafkaJsonSchemaSerializer<Object> jsonschemaSerializer =
        new KafkaJsonSchemaSerializer<>(client);
    JsonSchema schema = (JsonSchema) schemaDTO.parsedSchema;
    Object record;
    try {
      record = JsonSchemaUtils.toObject(data, schema);
    } catch (Exception e) {
      throw new BadRequestException("Failed to parse JSON data", e);
    }

    return ByteString.copyFrom(jsonschemaSerializer.serialize(topicName, record));
  }

  private ByteString serializeProtobuf(
      SchemaRegistryClient client, SchemaDTO schemaDTO, String topicName, JsonNode data) {
    final KafkaProtobufSerializer<Message> protobufSerializer =
        new KafkaProtobufSerializer<>(client);
    ProtobufSchema schema = (ProtobufSchema) schemaDTO.parsedSchema;
    Message record;
    try {
      record = (Message) ProtobufSchemaUtils.toObject(data, schema);
    } catch (Exception e) {
      throw new BadRequestException("Failed to parse Protobuf data", e);
    }

    return ByteString.copyFrom(protobufSerializer.serialize(topicName, record));
  }
}
