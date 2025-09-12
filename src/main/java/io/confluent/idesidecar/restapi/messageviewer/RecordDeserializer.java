package io.confluent.idesidecar.restapi.messageviewer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.net.UnknownServiceException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.EncoderFactory;
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
import io.netty.handler.codec.http.HttpResponseStatus;
import io.quarkus.logging.Log;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.unchecked.Unchecked;
import io.soabase.recordbuilder.core.RecordBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

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
  private final Duration schemaFetchTimeout;

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
   * Extracts the schema ID from the raw bytes.
   *
   * @param rawBytes The raw bytes containing the schema ID.
   * @return The extracted schema ID.
   * @throws IllegalArgumentException If the raw bytes are invalid for extracting the schema ID.
   */
  @VisibleForTesting
  static int getSchemaIdFromRawBytes(byte[] rawBytes) {
    if (rawBytes == null || rawBytes.length < 5) {
      throw new IllegalArgumentException("Invalid raw bytes for extracting schema ID.");
    }
    // The first byte is the magic byte, so we skip it
    ByteBuffer buffer = ByteBuffer.wrap(rawBytes, 1, 4);
    return buffer.getInt();
  }

  /**
   * Handles deserialization of Avro-encoded bytes.
   *
   * @param bytes     The Avro-encoded bytes to deserialize.
   * @param topicName topicName.
   * @param sr        The SchemaRegistryClient used for deserialization.
   * @param isKey     Whether the bytes are a key or value.
   * @return The deserialized JsonNode.
   */
  private JsonNode handleAvro(
      byte[] bytes, String topicName, SchemaRegistryClient sr, boolean isKey
  ) {
    try (
        var outputStream = new ByteArrayOutputStream();
        var avroDeserializer = new KafkaAvroDeserializer(sr)
    ) {
      avroDeserializer.configure(SERDE_CONFIGS, isKey);
      var genericObject = avroDeserializer.deserialize(topicName, bytes);
      if (genericObject instanceof GenericData.Record avroRecord) {
        // Use AVRO's native JSON encoder which preserves union type information
        var writer = new GenericDatumWriter<>(avroRecord.getSchema());
        var jsonEncoder = EncoderFactory.get().jsonEncoder(avroRecord.getSchema(), outputStream);
        writer.write(avroRecord, jsonEncoder);
        jsonEncoder.flush();

        // Parse the JSON string directly using Jackson's ObjectMapper
        var jsonString = outputStream.toString();
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
   * @param bytes The Protobuf-encoded bytes to deserialize.
   * @param sr    The SchemaRegistryClient used for deserialization.
   * @param isKey Whether the bytes are a key or value.
   * @return The deserialized JsonNode.
   */
  private JsonNode handleProtobuf(
      byte[] bytes,
      String topicName,
      SchemaRegistryClient sr,
      boolean isKey
  ) throws JsonProcessingException, InvalidProtocolBufferException {
    try (var protobufDeserializer = new KafkaProtobufDeserializer<>(sr)) {
      protobufDeserializer.configure(SERDE_CONFIGS, isKey);
      var protobufMessage = (DynamicMessage) protobufDeserializer.deserialize(topicName, bytes);

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
   * @param bytes The JSON Schema-encoded bytes to deserialize.
   * @param sr    The SchemaRegistryClient used for deserialization.
   * @param isKey Whether the bytes are a key or value.
   * @return The deserialized JsonNode.
   */
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

  /**
   * Parses a byte array into a JsonNode, handling various data formats: 1. Schema Registry encoded
   * data, supported formats: protobuf, avro, & JsonSchema. 2. Plain string data If the byte array
   * is Schema Registry encoded (starts with the MAGIC_BYTE), it is decoded and deserialized using
   * the provided SchemaRegistryClient. Otherwise, it is treated as a plain string and wrapped in a
   * TextNode.
   *
   * @param bytes                The byte array to parse
   * @param schemaRegistryClient The SchemaRegistryClient used for deserialization of Schema
   *                             Registry encoded data
   * @param context              The message viewer context.
   * @param isKey                Whether the data is a key or value
   * @param encoderOnFailure     A function to apply to the byte array if deserialization fails.
   * @return A DecodedResult containing either: - A JsonNode representing the decoded and
   * deserialized data (for Schema Registry encoded data) - A TextNode containing the original
   * string representation of the byte array ( for other cases) The DecodedResult also includes any
   * error message encountered during processing
   */
  public DecodedResult deserialize(
      byte[] bytes,
      SchemaRegistryClient schemaRegistryClient,
      KafkaRestProxyContext
          <SimpleConsumeMultiPartitionRequest, SimpleConsumeMultiPartitionResponse> context,
      boolean isKey,
      Optional<Function<byte[], byte[]>> encoderOnFailure
  ) {
    var result = maybeTrivialCase(bytes, schemaRegistryClient);
    if (result.isPresent()) {
      return result.get();
    }
    int schemaId = getSchemaIdFromRawBytes(bytes);
    String connectionId = context.getConnectionId();
    // Check if schema retrieval has failed recently
    var error = schemaErrors.readSchemaIdByConnectionId(
        connectionId,
        context.getClusterId(),
        schemaId
    );
    if (error != null) {
      return new DecodedResult(
          // If the schema fetch failed, we can't decode the data, so we just return the raw bytes.
          // We apply the encoderOnFailure function to the bytes before returning them.
          onFailure(encoderOnFailure, bytes).data,
          error.message()
      );
    }

    try {
      // Attempt to get the schema from the schema registry
      // and retry if the operation fails due to a retryable exception.
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
      var topicName = context.getTopicName();
      var deserializedJsonNode = switch (schemaType) {
        case AVRO -> handleAvro(bytes, topicName, schemaRegistryClient, isKey);
        case PROTOBUF -> handleProtobuf(bytes, topicName, schemaRegistryClient, isKey);
        case JSON -> handleJson(bytes, topicName, schemaRegistryClient, isKey);
      };
      return RecordDeserializerDecodedResultBuilder
          .builder()
          .value(deserializedJsonNode)
          .metadata(new KeyOrValueMetadata(schemaId, DataFormat.fromSchemaFormat(schemaType)))
          .build();
    } catch (Exception e) {
      var exc = unwrap(e);
      if (exc instanceof RestClientException || isNetworkRelatedException(exc)) {
        // IMPORTANT: We must cache RestClientException, and more importantly,
        //            network-related IOExceptions, to prevent the sidecar from
        //            bombarding the Schema Registry servers with requests for every
        //            consumed message when we encounter a schema fetch error.
        cacheSchemaFetchError(exc, schemaId, context);
        var wrappedJson = onFailure(encoderOnFailure, bytes);
        return new DecodedResult(
            wrappedJson.data(),
            e.getMessage(),
            new KeyOrValueMetadata(schemaId, wrappedJson.dataFormat())
        );
      } else if (
          exc instanceof SerializationException
              || exc instanceof IOException
      ) {
        // We don't cache SerializationException because it's not a schema fetch error
        // but rather a deserialization error, scoped to the specific message.
        Log.errorf(e, "Failed to deserialize record. " +
            "Returning raw encoded base64 data instead.");
        var wrappedJson = onFailure(encoderOnFailure, bytes);
        return new DecodedResult(
            wrappedJson.data(),
            e.getMessage(),
            new KeyOrValueMetadata(schemaId, wrappedJson.dataFormat())
        );
      }
      // If we reach this point, we have an unexpected exception, so we rethrow it.
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
    return deserialize(bytes, schemaRegistryClient, context, isKey, Optional.empty());
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

  /**
   * Handles the trivial cases of deserialization: null, empty, or non-Schema Registry encoded
   * data.
   *
   * @param bytes                The byte array to parse
   * @param schemaRegistryClient The SchemaRegistryClient used for deserialization of Schema
   *                             Registry encoded data
   * @return An Optional containing a DecodedResult if the base cases are met, or empty otherwise
   */
  private static Optional<DecodedResult> maybeTrivialCase(
      byte[] bytes,
      SchemaRegistryClient schemaRegistryClient
  ) {
    // If the byte array is null or empty, we return a NullNode or an empty string, respectively.
    if (bytes == null) {
      return Optional.of(new DecodedResult(
          NullNode.getInstance(),
          null,
          null
      ));
    }
    if (bytes.length == 0) {
      return Optional.of(new DecodedResult(
          new TextNode(""),
          null,
          null
      ));
    }

    // If the first byte is not the magic byte, we try to parse the data as a JSON object
    // or fall back to returning the raw bytes if parsing fails. Simple enough.
    if (bytes[0] != MAGIC_BYTE) {
      var wrappedJson = safeRead(bytes);
      return Optional.of(
          new DecodedResult(
              wrappedJson.data(),
              null,
              new KeyOrValueMetadata(null, wrappedJson.dataFormat())
          )
      );
    }

    // If the first byte is the magic byte, but we weren't provided a schema registry client,
    // we can't decode the data, so we just return the raw bytes.
    if (schemaRegistryClient == null) {
      var wrappedJson = safeRead(bytes);
      return Optional.of(new DecodedResult(
          wrappedJson.data(),
          "The value references a schema but we can't find the schema registry",
          new KeyOrValueMetadata(
              null,
              wrappedJson.dataFormat()
          )
      ));
    }

    // Alright, let's move on to more complex cases.
    return Optional.empty();
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
