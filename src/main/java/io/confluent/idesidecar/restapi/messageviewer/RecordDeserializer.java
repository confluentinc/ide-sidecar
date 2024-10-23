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
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import graphql.VisibleForTesting;
import io.confluent.idesidecar.restapi.util.ConfigUtil;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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

/**
 * Utility class for decoding record keys and values.
 */
@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
@ApplicationScoped
public class RecordDeserializer {

  private static final Map<String, String> SERDE_CONFIGS = ConfigUtil
      .asMap("ide-sidecar.serde-configs");

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final ObjectMapper avroObjectMapper = new AvroMapper(new AvroFactory());
  public static final byte MAGIC_BYTE = 0x0;
  private static final Duration CACHE_FAILED_SCHEMA_ID_FETCH_DURATION = Duration.ofSeconds(30);
  private static final Cache<Integer, String> schemaFetchErrorCache = Caffeine.newBuilder()
      .expireAfterWrite(CACHE_FAILED_SCHEMA_ID_FETCH_DURATION)
      .build();

  public record DecodedResult(JsonNode value, String errorMessage) {
  }

  static void clearCachedFailures() {
    schemaFetchErrorCache.invalidateAll();
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
   * Deserializes the given bytes to JSON based on the schema type retrieved from the
   * SchemaRegistryClient.
   *
   * @param bytes                The bytes to deserialize.
   * @param schemaRegistryClient The SchemaRegistryClient used for retrieving the schema.
   * @param topicName            Name of topic.
   * @param isKey                Whether the bytes are a key or value.
   * @return The deserialized JsonNode.
   */
  @SuppressWarnings("checkstyle:NPathComplexity")
  private DecodedResult deserializeToJson(
      byte[] bytes,
      SchemaRegistryClient schemaRegistryClient,
      String topicName,
      boolean isKey,
      Optional<Function<byte[], byte[]>> encoderOnFailure
  ) {
    if (bytes == null || bytes.length == 0) {
      return new DecodedResult(OBJECT_MAPPER.nullNode(), null);
    }

    final int schemaId = getSchemaIdFromRawBytes(bytes);
    // Check if schema retrieval has failed recently
    var cachedError = schemaFetchErrorCache.getIfPresent(schemaId);
    if (cachedError != null) {
      return new DecodedResult(
          // If the schema fetch failed, we can't decode the data, so we just return the raw bytes.
          // We apply the encoderOnFailure function to the bytes before returning them.
          onFailure(encoderOnFailure, bytes),
          cachedError
      );
    }

    try {
      // Attempt to get the schema from the schema registry
      var schemaType = schemaRegistryClient.getSchemaById(schemaId).schemaType();
      var decodedJson = switch (schemaType) {
        case "AVRO" -> handleAvro(bytes, topicName, schemaRegistryClient, isKey);
        case "PROTOBUF" -> handleProtobuf(bytes, topicName, schemaRegistryClient, isKey);
        case "JSON" -> handleJson(bytes, topicName, schemaRegistryClient, isKey);
        default -> OBJECT_MAPPER.readTree(new String(bytes, StandardCharsets.UTF_8));
      };
      return new DecodedResult(decodedJson, null);
    } catch (RestClientException e) {
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
      schemaFetchErrorCache.put(schemaId, errorMessage);
      return new DecodedResult(onFailure(encoderOnFailure, bytes), e.getMessage());
    } catch (IOException | SerializationException e) {
      var data = onFailure(encoderOnFailure, bytes);
      Log.error("Error deserializing: %s", data, e);
      return new DecodedResult(data, e.getMessage());
    }
  }

  private JsonNode onFailure(Optional<Function<byte[], byte[]>> encoderOnFailure, byte[] bytes) {
    return safeReadTree(encoderOnFailure.orElse(Function.identity()).apply(bytes));
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
      return avroObjectMapper
          .readerFor(ObjectNode.class)
          .with(jacksonAvroSchema)
          .readValue(outputStream.toByteArray());
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
      var printer = JsonFormat
          .printer()
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
   * Parses a byte array into a JsonNode, handling various data formats:
   * 1. Schema Registry encoded data, supported formats: protobuf, avro, & JsonSchema.
   * 2. Plain string data
   * If the byte array is Schema Registry encoded (starts with the MAGIC_BYTE),
   * it is decoded and deserialized using the provided SchemaRegistryClient.
   * Otherwise, it is treated as a plain string and wrapped in a TextNode.
   *
   * @param bytes                The byte array to parse
   * @param schemaRegistryClient The SchemaRegistryClient used for deserialization of
   *                             Schema Registry encoded data
   * @param topic                The name of the topic, used for Schema Registry deserialization
   * @param isKey                Whether the data is a key or value
   * @param encoderOnFailure     A function to apply to the byte array if deserialization fails.
   * @return A DecodedResult containing either:
   *         - A JsonNode representing the decoded and deserialized data (for Schema Registry
   *           encoded data)
   *         - A TextNode containing the original string representation of the byte array (
   *           for other cases)
   *         The DecodedResult also includes any error message encountered during processing
   */
  public DecodedResult deserialize(
      byte[] bytes,
      SchemaRegistryClient schemaRegistryClient,
      String topic,
      boolean isKey,
      Optional<Function<byte[], byte[]>> encoderOnFailure
  ) {
    if (bytes == null) {
      return new DecodedResult(NullNode.getInstance(), null);
    }
    if (bytes.length == 0) {
      return new DecodedResult(new TextNode(""), null);
    }
    if (bytes[0] == MAGIC_BYTE) {
      if (schemaRegistryClient != null) {
        return deserializeToJson(bytes, schemaRegistryClient, topic, isKey, encoderOnFailure);
      } else {
        return new DecodedResult(
            safeReadTree(bytes),
            "The value references a schema but we can't find the schema registry"
        );
      }
    }
    return new DecodedResult(safeReadTree(bytes), null);
  }

  public DecodedResult deserialize(
      byte[] bytes,
      SchemaRegistryClient schemaRegistryClient,
      String topic,
      boolean isKey
  ) {
    return deserialize(bytes, schemaRegistryClient, topic, isKey, Optional.empty());
  }

  /**
   * Try to read the byte array as a JSON object, falling back to a string if it fails.
   * @param bytes The byte array to read
   * @return      A JsonNode representing the byte array as a JSON object, or a TextNode
   */
  private static JsonNode safeReadTree(byte[] bytes) {
    try {
      return OBJECT_MAPPER.readTree(bytes);
    } catch (IOException e) {
      return TextNode.valueOf(new String(bytes, StandardCharsets.UTF_8));
    }
  }
}
