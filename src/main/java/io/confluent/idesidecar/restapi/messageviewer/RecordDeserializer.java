package io.confluent.idesidecar.restapi.messageviewer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.dataformat.avro.AvroFactory;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import graphql.VisibleForTesting;
import io.confluent.idesidecar.restapi.clients.SchemaErrors;
import io.confluent.idesidecar.restapi.kafkarest.SchemaFormat;
import io.confluent.idesidecar.restapi.util.ConfigUtil;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.quarkus.logging.Log;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.unchecked.Unchecked;
import io.soabase.recordbuilder.core.RecordBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.errors.SerializationException;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.net.UnknownServiceException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * Utility class for decoding record keys and values.
 */
@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
@ApplicationScoped
public class RecordDeserializer {

  private static final Map<String, String> SERDE_CONFIGS = ConfigUtil
      .asMap("ide-sidecar.serde-configs");

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final ObjectMapper AVRO_OBJECT_MAPPER = new AvroMapper(new AvroFactory());
  public static final byte MAGIC_BYTE = 0x0;
  private static final Duration CACHE_FAILED_SCHEMA_ID_FETCH_DURATION = Duration.ofSeconds(30);

  private final Duration schemaFetchRetryInitialBackoff;
  private final Duration schemaFetchRetryMaxBackoff;
  private final Duration schemaFetchTimeout;
  private final int schemaFetchMaxRetries;

  private final SchemaErrors schemaErrors;

  @Inject
  public RecordDeserializer(
      @ConfigProperty(name = "ide-sidecar.schema-fetch-retry.initial-backoff-ms")
      long schemaFetchRetryInitialBackoffMs,
      @ConfigProperty(name = "ide-sidecar.schema-fetch-retry.max-backoff-ms")
      long schemaFetchRetryMaxBackoffMs,
      @ConfigProperty(name = "ide-sidecar.schema-fetch-retry.timeout-ms")
      long schemaFetchTimeoutMs,
      @ConfigProperty(name = "ide-sidecar.schema-fetch-retry.max-retries")
      int schemaFetchMaxRetries,
      SchemaErrors schemaErrors
  ) {
    this.schemaFetchRetryInitialBackoff = Duration.ofMillis(schemaFetchRetryInitialBackoffMs);
    this.schemaFetchRetryMaxBackoff = Duration.ofMillis(schemaFetchRetryMaxBackoffMs);
    this.schemaFetchTimeout = Duration.ofMillis(schemaFetchTimeoutMs);
    this.schemaFetchMaxRetries = schemaFetchMaxRetries;
    this.schemaErrors = schemaErrors;
  }

  @RecordBuilder
  public record DecodedResult(JsonNode value, String errorMessage)
      implements RecordDeserializerDecodedResultBuilder.With {
  }

  @VisibleForTesting
  static void clearCachedFailures() {
    // This method is no longer needed as the cache is managed by SchemaErrors
  }

  @VisibleForTesting
  static int getSchemaIdFromRawBytes(byte[] rawBytes) {
    if (rawBytes == null || rawBytes.length < 5) {
      throw new IllegalArgumentException("Invalid raw bytes for extracting schema ID.");
    }
    ByteBuffer buffer = ByteBuffer.wrap(rawBytes, 1, 4);
    return buffer.getInt();
  }

  private JsonNode handleAvro(
      byte[] bytes, String topicName, SchemaRegistryClient sr, boolean isKey
  ) {
    try (
        var outputStream = new ByteArrayOutputStream();
        var avroDeserializer = new KafkaAvroDeserializer(sr);
    ) {
      avroDeserializer.configure(SERDE_CONFIGS, isKey);
      var avroRecord = (GenericData.Record) avroDeserializer.deserialize(topicName, bytes);
      var writer = new GenericDatumWriter<>(
          avroRecord.getSchema());
      var encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
      writer.write(avroRecord, encoder);
      encoder.flush();
      var jacksonAvroSchema = new AvroSchema(avroRecord.getSchema());
      return AVRO_OBJECT_MAPPER
          .readerFor(ObjectNode.class)
          .with(jacksonAvroSchema)
          .readValue(outputStream.toByteArray());
    } catch (IOException e) {
      throw new RuntimeException("Failed to deserialize Avro-encoded bytes", e);
    }
  }

  private JsonNode handleProtobuf(
      byte[] bytes,
      String topicName,
      SchemaRegistryClient sr,
      boolean isKey
  ) throws JsonProcessingException, InvalidProtocolBufferException {
    try (var protobufDeserializer = new KafkaProtobufDeserializer<>(sr)) {
      protobufDeserializer.configure(SERDE_CONFIGS, isKey);
      var protobufMessage = (DynamicMessage) protobufDeserializer.deserialize(topicName, bytes);
      var printer = JsonFormat
          .printer()
          .includingDefaultValueFields()
          .preservingProtoFieldNames();
      var jsonString = printer.print(protobufMessage);
      return OBJECT_MAPPER.readTree(jsonString);
    }
  }

  private JsonNode handleJson(
      byte[] bytes,
      String topicName,
      SchemaRegistryClient sr,
      boolean isKey
  ) {
    try (var jsonSchemaDeserializer = new KafkaJsonSchemaDeserializer<>(sr)) {
      jsonSchemaDeserializer.configure(SERDE_CONFIGS, isKey);
      var jsonSchemaResult = jsonSchemaDeserializer.deserialize(topicName, bytes);
      return OBJECT_MAPPER.valueToTree(jsonSchemaResult);
    }
  }

  public DecodedResult deserialize(
      byte[] bytes,
      SchemaRegistryClient schemaRegistryClient,
      String topic,
      boolean isKey,
      Optional<Function<byte[], byte[]>> encoderOnFailure
  ) {
    var result = maybeTrivialCase(bytes, schemaRegistryClient);
    if (result.isPresent()) {
      return result.get();
    }

    final int schemaId = getSchemaIdFromRawBytes(bytes);
    var cachedError = schemaErrors.readBySchemaId(schemaId);
    if (cachedError != null) {
      return new DecodedResult(
          onFailure(encoderOnFailure, bytes),
          cachedError
      );
    }

    try {
      var parsedSchema = Uni
          .createFrom()
          .item(Unchecked.supplier(() -> schemaRegistryClient.getSchemaById(schemaId)))
          .onFailure(this::isRetryableException)
          .retry()
          .withBackOff(schemaFetchRetryInitialBackoff, schemaFetchRetryMaxBackoff)
          .atMost(schemaFetchMaxRetries)
          .runSubscriptionOn(Infrastructure.getDefaultExecutor())
          .await()
          .atMost(schemaFetchTimeout);

      var schemaType = SchemaFormat.fromSchemaType(parsedSchema.schemaType());
      var deserializedData = switch (schemaType) {
        case AVRO -> handleAvro(bytes, topic, schemaRegistryClient, isKey);
        case PROTOBUF -> handleProtobuf(bytes, topic, schemaRegistryClient, isKey);
        case JSON -> handleJson(bytes, topic, schemaRegistryClient, isKey);
      };
      return RecordDeserializerDecodedResultBuilder
          .builder()
          .value(deserializedData)
          .build();
    } catch (Exception e) {
      var exc = e.getCause();
      if (exc instanceof RestClientException || isNetworkRelatedException(exc)) {
        cacheSchemaFetchError(exc, schemaId);
        return new DecodedResult(onFailure(encoderOnFailure, bytes), e.getMessage());
      } else if (
          exc instanceof SerializationException
          || exc instanceof IOException
      ) {
        return new DecodedResult(onFailure(encoderOnFailure, bytes), e.getMessage());
      }
      throw new RuntimeException("Failed to deserialize record", e);
    }
  }

  private boolean isRetryableException(Throwable throwable) {
    var exc = throwable.getCause();
    return (
        isNetworkRelatedException(exc) ||
        (exc instanceof RestClientException
         && isRestClientExceptionRetryable((RestClientException) exc)
        ));
  }

  private boolean isNetworkRelatedException(Throwable throwable) {
    var e = (Exception) throwable;
    return (
        e instanceof ConnectException
        || e instanceof SocketTimeoutException
        || e instanceof UnknownHostException
        || e instanceof UnknownServiceException
    );
  }

  private boolean isRestClientExceptionRetryable(RestClientException e) {
    var status = e.getStatus();
    var isClientErrorToIgnore = (
        status == HttpResponseStatus.REQUEST_TIMEOUT.code()
        || status == HttpResponseStatus.TOO_MANY_REQUESTS.code()
    );
    var isServerErrorToIgnore = (
        status == HttpResponseStatus.BAD_GATEWAY.code()
        || status == HttpResponseStatus.SERVICE_UNAVAILABLE.code()
        || status == HttpResponseStatus.GATEWAY_TIMEOUT.code()
    );
    return isClientErrorToIgnore || isServerErrorToIgnore;
  }

  public DecodedResult deserialize(
      byte[] bytes,
      SchemaRegistryClient schemaRegistryClient,
      String topic,
      boolean isKey
  ) {
    return deserialize(bytes, schemaRegistryClient, topic, isKey, Optional.empty());
  }

  private void cacheSchemaFetchError(Throwable e, int schemaId) {
    var retryTime = Instant.now().plus(CACHE_FAILED_SCHEMA_ID_FETCH_DURATION);
    var timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss")
                                         .withZone(ZoneId.systemDefault());
    Log.errorf(
        "Failed to retrieve schema with ID %d. Will try again in %d seconds at %s. Error: %s",
        schemaId,
        CACHE_FAILED_SCHEMA_ID_FETCH_DURATION.getSeconds(),
        timeFormatter.format(retryTime),
        e.getMessage(),
        e
    );
    var errorMessage = String.format(
        "Failed to retrieve schema with ID %d: %s",
        schemaId,
        e.getMessage()
    );
    schemaErrors.writeBySchemaId(schemaId, errorMessage);
  }

  private static Optional<DecodedResult> maybeTrivialCase(
      byte[] bytes,
      SchemaRegistryClient schemaRegistryClient
  ) {
    if (bytes == null) {
      return Optional.of(new DecodedResult(NullNode.getInstance(), null));
    }
    if (bytes.length == 0) {
      return Optional.of(new DecodedResult(new TextNode(""), null));
    }

    if (bytes[0] != MAGIC_BYTE) {
      return Optional.of(new DecodedResult(safeReadTree(bytes), null));
    }

    if (schemaRegistryClient == null) {
      return Optional.of(new DecodedResult(
          safeReadTree(bytes),
          "The value references a schema but we can't find the schema registry"
      ));
    }

    return Optional.empty();
  }

  private static JsonNode safeReadTree(byte[] bytes) {
    try {
      return OBJECT_MAPPER.readTree(bytes);
    } catch (IOException e) {
      return TextNode.valueOf(new String(bytes, StandardCharsets.UTF_8));
    }
  }

  private static JsonNode onFailure(
      Optional<Function<byte[], byte[]>> encoderOnFailure, byte[] bytes
  ) {
    return safeReadTree(encoderOnFailure.orElse(Function.identity()).apply(bytes));
  }
}