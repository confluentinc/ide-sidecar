package io.confluent.idesidecar.restapi.messageviewer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.dataformat.avro.AvroFactory;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer;
import io.quarkus.logging.Log;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;

/**
 * Utility class for decoding record keys and values.
 */
@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public class DecoderUtil {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final ObjectMapper avroObjectMapper = new AvroMapper(new AvroFactory());
  public static final byte MAGIC_BYTE = 0x0;

  private static final Duration CACHE_FAILED_SCHEMA_ID_FETCH_DURATION = Duration.ofSeconds(30);

  private static final Cache<Integer, String> schemaFetchErrorCache = Caffeine.newBuilder()
      .expireAfterWrite(CACHE_FAILED_SCHEMA_ID_FETCH_DURATION)
      .build();

  public static class DecodedResult {
    private final JsonNode value;
    private final String errorMessage;

    public DecodedResult(JsonNode value, String errorMessage) {
      this.value = value;
      this.errorMessage = errorMessage;
    }

    public JsonNode getValue() {
      return value;
    }

    public String getErrorMessage() {
      return errorMessage;
    }
  }

  /**
   * Decodes a Base64-encoded string and deserializes it using the provided SchemaRegistryClient.
   *
   * @param rawBase64           The Base64-encoded string to decode and deserialize.
   * @param schemaRegistryClient The SchemaRegistryClient used for deserialization.
   * @param topicName The name of topic.
   * @return The deserialized JsonNode, or the original rawBase64 string as TextNode if an error
   *        occurs.
   */
  public static DecodedResult decodeAndDeserialize(
      String rawBase64,
      SchemaRegistryClient schemaRegistryClient,
      String topicName
  ) {
    if (rawBase64 == null || rawBase64.isEmpty()) {
      return null;
    }

    try {
      byte[] decodedBytes = Base64.getDecoder().decode(rawBase64);
      return deserializeToJson(decodedBytes, schemaRegistryClient, topicName);
    } catch (IllegalArgumentException e) {
      Log.error("Error decoding Base64 string: " + rawBase64, e);
      return new DecodedResult(TextNode.valueOf(rawBase64), e.getMessage());
    } catch (IOException | RestClientException e) {
      Log.error("Error deserializing: " + rawBase64, e);
      return new DecodedResult(TextNode.valueOf(rawBase64), e.getMessage());
    }
  }

  /**
   * Extracts the schema ID from the raw bytes.
   *
   * @param rawBytes The raw bytes containing the schema ID.
   * @return The extracted schema ID.
   * @throws IllegalArgumentException If the raw bytes are invalid for extracting the schema ID.
   */
  public static int getSchemaIdFromRawBytes(byte[] rawBytes) {
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
   * @param bytes               The bytes to deserialize.
   * @param schemaRegistryClient The SchemaRegistryClient used for retrieving the schema.
   * @param topicName Name of topic.
   * @return The deserialized JsonNode.
   * @throws IOException        If an I/O error occurs during deserialization.
   * @throws RestClientException If an error occurs while communicating with the Schema Registry.
   */
  @SuppressWarnings("checkstyle:NPathComplexity")
  public static DecodedResult deserializeToJson(
      byte[] bytes,
      SchemaRegistryClient schemaRegistryClient,
      String topicName
  ) throws IOException, RestClientException {
    if (bytes == null || bytes.length == 0) {
      return new DecodedResult(OBJECT_MAPPER.nullNode(), null);
    }

    final int schemaId = getSchemaIdFromRawBytes(bytes);

    // Check if schema retrieval has failed recently
    String cachedError = schemaFetchErrorCache.getIfPresent(schemaId);
    if (cachedError != null) {
      return new DecodedResult(
          TextNode.valueOf(new String(bytes, StandardCharsets.UTF_8)),
          cachedError
      );
    }

    try {
      // Attempt to get the schema from the schema registry
      String schemaType = schemaRegistryClient.getSchemaById(schemaId).schemaType();
      JsonNode decodedJson = switch (schemaType) {
        case "AVRO" -> handleAvro(bytes, topicName, schemaRegistryClient);
        case "PROTOBUF" -> handleProtobuf(bytes, topicName, schemaRegistryClient);
        case "JSON" -> handleJson(bytes, topicName, schemaRegistryClient);
        default -> OBJECT_MAPPER.readTree(new String(bytes, StandardCharsets.UTF_8));
      };
      return new DecodedResult(decodedJson, null);
    } catch (RestClientException e) {
      Instant retryTime = Instant.now().plus(CACHE_FAILED_SCHEMA_ID_FETCH_DURATION);
      DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss")
          .withZone(ZoneId.systemDefault());

      Log.errorf(
          "Failed to retrieve schema with ID %d. Will try again in %d seconds at %s. Error: %s",
          schemaId,
          CACHE_FAILED_SCHEMA_ID_FETCH_DURATION.getSeconds(),
          timeFormatter.format(retryTime),
          e.getMessage(),
          e
      );

      String errorMessage = String.format(
          "Failed to retrieve schema with ID %d: %s",
          schemaId,
          e.getMessage()
      );
      schemaFetchErrorCache.put(schemaId, errorMessage);
      throw e;
    }
  }

  /**
   * Handles deserialization of Avro-encoded bytes.
   *
   * @param bytes The Avro-encoded bytes to deserialize.
   * @param topicName topicName.
   * @param sr    The SchemaRegistryClient used for deserialization.
   * @return The deserialized JsonNode.
   */
  public static JsonNode handleAvro(byte[] bytes, String topicName, SchemaRegistryClient sr) {
    KafkaAvroDeserializer avroDeserializer = new KafkaAvroDeserializer(sr);
    GenericData.Record avroRecord = (GenericData.Record) avroDeserializer.deserialize(
        topicName, bytes);

    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
      GenericDatumWriter<GenericData.Record> writer = new GenericDatumWriter<>(
          avroRecord.getSchema());
      Encoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
      writer.write(avroRecord, encoder);
      encoder.flush();
      com.fasterxml.jackson.dataformat.avro.AvroSchema jacksonAvroSchema = new
          com.fasterxml.jackson.dataformat.avro.AvroSchema(avroRecord.getSchema());

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
   * @return The deserialized JsonNode.
   */
  public static JsonNode handleProtobuf(byte[] bytes, String topicName, SchemaRegistryClient sr)
      throws JsonProcessingException, InvalidProtocolBufferException {
    KafkaProtobufDeserializer<?> protobufDeserializer = new KafkaProtobufDeserializer<>(sr);
    DynamicMessage protobufMessage = (DynamicMessage) protobufDeserializer.deserialize(
        topicName, bytes);

    JsonFormat.Printer printer = JsonFormat.printer()
        .includingDefaultValueFields()
        .preservingProtoFieldNames();

    String jsonString = printer.print(protobufMessage);
    return OBJECT_MAPPER.readTree(jsonString);
  }

  /**
   * Handles deserialization of JSON Schema-encoded bytes.
   *
   * @param bytes The JSON Schema-encoded bytes to deserialize.
   * @param sr    The SchemaRegistryClient used for deserialization.
   * @return The deserialized JsonNode.
   */
  public static JsonNode handleJson(byte[] bytes, String topicName, SchemaRegistryClient sr)
      throws JsonProcessingException {
    KafkaJsonSchemaDeserializer<?> jsonSchemaDeserializer = new KafkaJsonSchemaDeserializer<>(sr);
    Object jsonSchemaResult = jsonSchemaDeserializer.deserialize(topicName, bytes);

    // Convert the result to a JsonNode
    return OBJECT_MAPPER.valueToTree(jsonSchemaResult);
  }

  /**
   * Parses a JSON byte array into a JsonNode. If the byte array cannot be parsed as JSON,
   * it is returned as a TextNode containing the original string representation of the byte array.
   *
   * @param bytes the JSON byte array to parse
   * @return the parsed JsonNode, or a TextNode containing the original string if parsing fails
   */
  public static JsonNode parseJsonNode(byte[] bytes) {
    if (bytes == null || bytes.length == 0) {
      return new TextNode("");
    }
    if (bytes[0] == MAGIC_BYTE) {
      // Jackson will automatically encode the map as a simple JSON object and the byte array value
      // into a Base64-encoded string.
      // TODO(Ravi) Deserialize the encoded bytes into JsonNode object.
      try {
        return OBJECT_MAPPER.readTree(bytes);
      } catch (IOException e) {
        Log.errorf("Error while converting bytes to Json. '%s'", e.getMessage());
        return new TextNode(new String(bytes, StandardCharsets.UTF_8));
      }
    }
    return new TextNode(new String(bytes, StandardCharsets.UTF_8));
  }
}