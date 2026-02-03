package io.confluent.idesidecar.restapi.messageviewer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.net.UnknownServiceException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.errors.SerializationException;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

import graphql.VisibleForTesting;
import io.confluent.idesidecar.restapi.clients.SchemaErrors;
import io.confluent.idesidecar.restapi.kafkarest.SchemaFormat;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionRequest;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionResponse;
import io.confluent.idesidecar.restapi.models.DataFormat;
import io.confluent.idesidecar.restapi.models.KeyOrValueMetadata;
import io.confluent.idesidecar.restapi.proxy.KafkaRestProxyContext;
import io.confluent.idesidecar.restapi.util.ByteArrayJsonUtil;
import io.confluent.idesidecar.restapi.util.ConfigUtil;
import static io.confluent.idesidecar.restapi.util.ExceptionUtil.unwrap;
import io.confluent.idesidecar.restapi.util.ObjectMapperFactory;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.quarkus.logging.Log;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.unchecked.Unchecked;
import io.soabase.recordbuilder.core.RecordBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import static io.confluent.kafka.serializers.schema.id.SchemaId.KEY_SCHEMA_ID_HEADER;
import static io.confluent.kafka.serializers.schema.id.SchemaId.VALUE_SCHEMA_ID_HEADER;


/**
 * Utility class for decoding record keys and values.
 */
@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
@ApplicationScoped
public class RecordDeserializer {

  private static final Map<String, String> SERDE_CONFIGS = ConfigUtil
      .asMap("ide-sidecar.serde-configs");
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getObjectMapper();
  public static final byte MAGIC_BYTE = 0x0;
  private static final Duration CACHE_FAILED_SCHEMA_ID_FETCH_DURATION = Duration.ofSeconds(30);

  private final int schemaFetchMaxRetries;
  private final SchemaErrors schemaErrors;
  private final Duration schemaFetchRetryInitialBackoff;
  private final Duration schemaFetchRetryMaxBackoff;

  @Inject
  public RecordDeserializer(
      @ConfigProperty(name = "ide-sidecar.schema-fetch-retry.initial-backoff-ms")
      long schemaFetchRetryInitialBackoffMs,
      @ConfigProperty(name = "ide-sidecar.schema-fetch-retry.max-backoff-ms")
      long schemaFetchRetryMaxBackoffMs,
      @ConfigProperty(name = "ide-sidecar.schema-fetch-retry.max-retries")
      int schemaFetchMaxRetries,
      SchemaErrors schemaErrors
  ) {
    this.schemaFetchRetryInitialBackoff = Duration.ofMillis(schemaFetchRetryInitialBackoffMs);
    this.schemaFetchRetryMaxBackoff = Duration.ofMillis(schemaFetchRetryMaxBackoffMs);
    this.schemaFetchMaxRetries = schemaFetchMaxRetries;
    this.schemaErrors = schemaErrors;
  }

  @RecordBuilder
  public record DecodedResult(
      JsonNode value,
      String errorMessage,
      KeyOrValueMetadata metadata
  ) implements RecordDeserializerDecodedResultBuilder.With {

    /**
     * Creates a new DecodedResult with the given value and error message, for when there are no
     * schema details.
     *
     * @param value        The decoded value
     * @param errorMessage The error message, null if there is no error
     */
    public DecodedResult(JsonNode value, String errorMessage) {
      this(value, errorMessage, null);
    }
  }

  /**
   * Extracts the schema GUID from the Kafka record headers.
   *
   * @param headers The Kafka record headers.
   * @param isKey   Whether the data is a key or value.
   * @return An Optional containing the schema GUID if present, or empty otherwise.
   */
  @VisibleForTesting
  static Optional<String> getSchemaGuidFromHeaders(Headers headers, boolean isKey) {
    if (headers == null) {
      return Optional.empty();
    }
    var headerName = isKey ? KEY_SCHEMA_ID_HEADER : VALUE_SCHEMA_ID_HEADER;
    var header = headers.lastHeader(headerName);
    if (header == null || header.value() == null) {
      return Optional.empty();
    }
    return Optional.of(new String(header.value(), StandardCharsets.UTF_8));
  }

  /**
   * Extracts the schema ID from the raw bytes if they are Schema Registry encoded. We assume the
   * raw bytes are Schema Registry encoded if they start with the magic byte (0x0) and have a length
   * of at least 5 bytes.
   *
   * @param rawBytes The raw bytes potentially containing the schema ID.
   * @return An Optional containing the schema ID if the bytes start with the magic byte,
   *         or empty if the bytes are null, too short, or don't start with the magic byte.
   */
  @VisibleForTesting
  static Optional<Integer> getSchemaIdFromRawBytes(byte[] rawBytes) {
    if (rawBytes == null || rawBytes.length < 5 || rawBytes[0] != MAGIC_BYTE) {
      return Optional.empty();
    }
    var buffer = ByteBuffer.wrap(rawBytes, 1, 4);
    return Optional.of(buffer.getInt());
  }

  /**
   * Handles deserialization of Avro-encoded bytes.
   *
   * @param bytes     The Avro-encoded bytes to deserialize.
   * @param topicName topicName.
   * @param sr        The SchemaRegistryClient used for deserialization.
   * @param isKey     Whether the bytes are a key or value.
   * @param headers   The Kafka record headers (may contain schema ID).
   * @return The deserialized JsonNode.
   */
  private JsonNode handleAvro(
      byte[] bytes, String topicName, SchemaRegistryClient sr, boolean isKey, Headers headers
  ) {
    try (
        var outputStream = new ByteArrayOutputStream();
        var avroDeserializer = new KafkaAvroDeserializer(sr)
    ) {
      avroDeserializer.configure(SERDE_CONFIGS, isKey);
      var genericObject = avroDeserializer.deserialize(topicName, headers, bytes);
      if (genericObject instanceof GenericData.Record avroRecord) {
        // Use AVRO's native JSON encoder which preserves union type information
        // See https://github.com/confluentinc/ide-sidecar/issues/417 for more information
        var writer = new GenericDatumWriter<>(avroRecord.getSchema());
        var jsonEncoder = EncoderFactory.get().jsonEncoder(avroRecord.getSchema(), outputStream);
        writer.write(avroRecord, jsonEncoder);
        jsonEncoder.flush();

        // Parse the JSON string directly using Jackson's ObjectMapper
        var jsonString = outputStream.toString(StandardCharsets.UTF_8);
        return OBJECT_MAPPER.readTree(jsonString);
      } else {
        return OBJECT_MAPPER.valueToTree(genericObject);
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed to deserialize Avro-encoded bytes", e);
    }
  }

  /**
   * Handles deserialization of Protobuf-encoded bytes.
   *
   * @param bytes   The Protobuf-encoded bytes to deserialize.
   * @param sr      The SchemaRegistryClient used for deserialization.
   * @param isKey   Whether the bytes are a key or value.
   * @param headers The Kafka record headers (may contain schema ID).
   * @return The deserialized JsonNode.
   */
  private JsonNode handleProtobuf(
      byte[] bytes,
      String topicName,
      SchemaRegistryClient sr,
      boolean isKey,
      Headers headers
  ) throws JsonProcessingException, InvalidProtocolBufferException {
    try (var protobufDeserializer = new KafkaProtobufDeserializer<>(sr)) {
      protobufDeserializer.configure(SERDE_CONFIGS, isKey);
      var protobufMessage = (DynamicMessage) protobufDeserializer.deserialize(topicName, headers, bytes);

      // Add the message and its nested types to the type registry
      // used by the JsonFormat printer.
      var typeRegistry = JsonFormat.TypeRegistry
          .newBuilder()
          .add(protobufMessage.getDescriptorForType())
          .add(protobufMessage.getDescriptorForType().getNestedTypes())
          .build();
      var printer = JsonFormat
          .printer()
          .usingTypeRegistry(typeRegistry)
          .includingDefaultValueFields()
          .preservingProtoFieldNames();
      var jsonString = printer.print(protobufMessage);
      return OBJECT_MAPPER.readTree(jsonString);
    }
  }

  /**
   * Handles deserialization of JSON Schema-encoded bytes.
   *
   * @param bytes   The JSON Schema-encoded bytes to deserialize.
   * @param sr      The SchemaRegistryClient used for deserialization.
   * @param isKey   Whether the bytes are a key or value.
   * @param headers The Kafka record headers (may contain schema ID).
   * @return The deserialized JsonNode.
   */
  private JsonNode handleJson(
      byte[] bytes,
      String topicName,
      SchemaRegistryClient sr,
      boolean isKey,
      Headers headers
  ) {
    try (var jsonSchemaDeserializer = new KafkaJsonSchemaDeserializer<>(sr)) {
      jsonSchemaDeserializer.configure(SERDE_CONFIGS, isKey);
      var jsonSchemaResult = jsonSchemaDeserializer.deserialize(topicName, headers, bytes);
      return OBJECT_MAPPER.valueToTree(jsonSchemaResult);
    }
  }

  /**
   * Parses a byte array into a JsonNode, handling various data formats.
   *
   * <p>The deserialization follows this priority order:
   * <ol>
   *   <li>If bytes are null or empty, return appropriate trivial result</li>
   *   <li>If headers contain a schema GUID, use it to fetch the schema from the registry</li>
   *   <li>If bytes start with the magic byte and contain a schema ID, use it to fetch the schema</li>
   *   <li>Otherwise, decode the bytes as plain JSON, UTF-8 string, or raw bytes</li>
   * </ol>
   *
   * @param bytes                The byte array to parse
   * @param schemaRegistryClient The SchemaRegistryClient used for deserialization of Schema
   *                             Registry encoded data
   * @param context              The message viewer context.
   * @param isKey                Whether the data is a key or value
   * @param encoderOnFailure     A function to apply to the byte array if deserialization fails.
   * @param headers              The Kafka record headers (may contain schema GUID).
   * @return A DecodedResult containing the decoded data and any error message
   */
  public DecodedResult deserialize(
      byte[] bytes,
      SchemaRegistryClient schemaRegistryClient,
      KafkaRestProxyContext
          <SimpleConsumeMultiPartitionRequest, SimpleConsumeMultiPartitionResponse> context,
      boolean isKey,
      Optional<Function<byte[], byte[]>> encoderOnFailure,
      Headers headers
  ) {
    // Handle null and empty bytes
    if (bytes == null) {
      return new DecodedResult(NullNode.getInstance(), null, null);
    }
    if (bytes.length == 0) {
      return new DecodedResult(new TextNode(""), null, null);
    }

    // Try to deserialize with a schema if available
    // Priority 1: Schema GUID from headers
    // Priority 2: Schema ID from raw bytes
    var schemaGuid = getSchemaGuidFromHeaders(headers, isKey);
    var schemaId = getSchemaIdFromRawBytes(bytes);
    var hasSchemaInfo = schemaGuid.isPresent() || schemaId.isPresent();
    if (schemaRegistryClient != null && hasSchemaInfo) {
      return deserializeWithSchema(
          bytes,
          schemaRegistryClient,
          context,
          isKey,
          encoderOnFailure,
          headers,
          schemaGuid,
          schemaId
      );
    }

    // In the absence of any schema info, try to decode as JSON data
    var wrappedJson = safeRead(bytes);
    return new DecodedResult(
        wrappedJson.data(),
        schemaRegistryClient == null && hasSchemaInfo
            ? "The value references a schema but no schema registry is available"
            : null,
        new KeyOrValueMetadata(null, wrappedJson.dataFormat())
    );
  }

  /**
   * Deserializes bytes using schema information from either GUID or ID.
   */
  private DecodedResult deserializeWithSchema(
      byte[] bytes,
      SchemaRegistryClient schemaRegistryClient,
      KafkaRestProxyContext
          <SimpleConsumeMultiPartitionRequest, SimpleConsumeMultiPartitionResponse> context,
      boolean isKey,
      Optional<Function<byte[], byte[]>> encoderOnFailure,
      Headers headers,
      Optional<String> schemaGuid,
      Optional<Integer> schemaId
  ) {
    String connectionId = context.getConnectionId();

    // Check if schema retrieval has failed recently
    // TODO: Include GUID in cache key
    if (schemaId.isPresent()) {
      var error = schemaErrors.readSchemaIdByConnectionId(
          connectionId,
          context.getClusterId(),
          schemaId.get()
      );
      if (error != null) {
        return new DecodedResult(
            onFailure(encoderOnFailure, bytes).data,
            error.message()
        );
      }
    }

    try {
      // Attempt to get the schema from the schema registry
      // and retry if the operation fails due to a retryable exception.
      var parsedSchema = Uni
          .createFrom()
          .item(
              Unchecked.supplier(
                  () -> schemaGuid.isPresent()
                      ? schemaRegistryClient.getSchemaByGuid(schemaGuid.get(), null)
                      : schemaRegistryClient.getSchemaById(schemaId.orElseThrow())
              )
          )
          .onFailure(this::isRetryableException)
          .retry()
          .withBackOff(schemaFetchRetryInitialBackoff, schemaFetchRetryMaxBackoff)
          .atMost(schemaFetchMaxRetries)
          .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
          .subscribeAsCompletionStage()
          .toCompletableFuture()
          .join();

      var schemaType = SchemaFormat.fromSchemaType(parsedSchema.schemaType());
      var topicName = context.getTopicName();
      var deserializedJsonNode = switch (schemaType) {
        case AVRO -> handleAvro(bytes, topicName, schemaRegistryClient, isKey, headers);
        case PROTOBUF -> handleProtobuf(bytes, topicName, schemaRegistryClient, isKey, headers);
        case JSON -> handleJson(bytes, topicName, schemaRegistryClient, isKey, headers);
      };
      return RecordDeserializerDecodedResultBuilder
          .builder()
          .value(deserializedJsonNode)
          .metadata(new KeyOrValueMetadata(schemaId.orElse(null), DataFormat.fromSchemaFormat(schemaType)))
          .build();
    } catch (Exception e) {
      var exc = unwrap(e);
      if (exc instanceof RestClientException || isNetworkRelatedException(exc)) {
        // IMPORTANT: We must cache RestClientException, and more importantly,
        //            network-related IOExceptions, to prevent the sidecar from
        //            bombarding the Schema Registry servers with requests for every
        //            consumed message when we encounter a schema fetch error.
        schemaId.ifPresent(id -> cacheSchemaFetchError(exc, id, context));
        var wrappedJson = onFailure(encoderOnFailure, bytes);
        return new DecodedResult(
            wrappedJson.data(),
            e.getMessage(),
            new KeyOrValueMetadata(schemaId.orElse(null), wrappedJson.dataFormat())
        );
      } else if (exc instanceof SerializationException || exc instanceof IOException) {
        // We don't cache SerializationException because it's not a schema fetch error
        // but rather a deserialization error, scoped to the specific message.
        Log.errorf(e, "Failed to deserialize record. " +
            "Returning raw encoded base64 data instead.");
        var wrappedJson = onFailure(encoderOnFailure, bytes);
        return new DecodedResult(
            wrappedJson.data(),
            e.getMessage(),
            new KeyOrValueMetadata(schemaId.orElse(null), wrappedJson.dataFormat())
        );
      } else if (e instanceof CompletionException) {
        throw new RuntimeException(exc);
      }
      // If we reach this point, we have an unexpected exception, so we log and rethrow it.
      Log.error(e);
      throw new RuntimeException("Failed to deserialize record", e);
    }
  }

  private boolean isRetryableException(Throwable throwable) {
    var exc = unwrap(throwable);
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
    // Adapted from io.confluent.kafka.schemaregistry.client.rest.RestService#isRetriable
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
      KafkaRestProxyContext
          <SimpleConsumeMultiPartitionRequest, SimpleConsumeMultiPartitionResponse> context,
      boolean isKey
  ) {
    return deserialize(bytes, schemaRegistryClient, context, isKey, Optional.empty(), null);
  }

  public DecodedResult deserialize(
      byte[] bytes,
      SchemaRegistryClient schemaRegistryClient,
      KafkaRestProxyContext
          <SimpleConsumeMultiPartitionRequest, SimpleConsumeMultiPartitionResponse> context,
      boolean isKey,
      Optional<Function<byte[], byte[]>> encoderOnFailure
  ) {
    return deserialize(bytes, schemaRegistryClient, context, isKey, encoderOnFailure, null);
  }

  public DecodedResult deserialize(
      byte[] bytes,
      SchemaRegistryClient schemaRegistryClient,
      KafkaRestProxyContext
          <SimpleConsumeMultiPartitionRequest, SimpleConsumeMultiPartitionResponse> context,
      boolean isKey,
      Headers headers
  ) {
    return deserialize(bytes, schemaRegistryClient, context, isKey, Optional.empty(), headers);
  }

  private void cacheSchemaFetchError(
      Throwable e, int schemaId, KafkaRestProxyContext
          <SimpleConsumeMultiPartitionRequest, SimpleConsumeMultiPartitionResponse> context
  ) {
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
    SchemaErrors.Error errorMessage = new SchemaErrors.Error(String.format(
        "Failed to retrieve schema with ID %d: %s",
        schemaId,
        e.getMessage()
    ));
    schemaErrors.writeSchemaIdByConnectionId(
        context.getConnectionId(),
        schemaId,
        context.getClusterId(),
        errorMessage
    );
  }

  record WrappedJson(
      JsonNode data,
      DataFormat dataFormat
  ) {

  }

  /**
   * Interpret the bytes safely. This is the order in which we try to interpret the bytes: 1. Try to
   * read the bytes as a JSON value (object, array, string, number, boolean, or null). 2. If the
   * bytes are not valid JSON, try to read them as a UTF-8 string. 3. If the bytes are not valid
   * UTF-8, encode them as a base64 string in a JSON object with a single field named "__raw__".
   *
   * @param bytes The byte array to interpret.
   * @return A {@link WrappedJson} object containing the interpreted data and whether it's JSON or
   * raw bytes.
   */
  private static WrappedJson safeRead(byte[] bytes) {
    try {
      return new WrappedJson(
          OBJECT_MAPPER.readTree(bytes),
          DataFormat.JSON
      );
    } catch (IOException e) {
      return handleNonJsonBytes(bytes);
    }
  }

  /**
   * Try to read the byte array as a valid UTF-8 string, or encode it as a base64 string if it's not
   * valid UTF-8.
   *
   * @param bytes The non-JSON byte array to interpret.
   * @return A WrappedJson object containing the interpreted data and whether it's JSON or raw
   * bytes.
   */
  private static WrappedJson handleNonJsonBytes(byte[] bytes) {
    try {
      return new WrappedJson(
          TextNode.valueOf(ByteArrayJsonUtil.asUTF8String(bytes)),
          DataFormat.UTF8_STRING
      );
    } catch (CharacterCodingException ex) {
      // The bytes are not valid JSON or UTF-8, so we encode them as a base64 string.
      return new WrappedJson(ByteArrayJsonUtil.asJsonNode(bytes), DataFormat.RAW_BYTES);
    }
  }

  private static WrappedJson onFailure(
      Optional<Function<byte[], byte[]>> encoderOnFailure, byte[] bytes
  ) {
    return safeRead(encoderOnFailure.orElse(Function.identity()).apply(bytes));
  }
}
