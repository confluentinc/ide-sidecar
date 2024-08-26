package io.confluent.idesidecar.restapi.messageviewer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.dataformat.avro.AvroFactory;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
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
import java.util.Base64;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public class DecoderUtil {
  private static final Logger log = LoggerFactory.getLogger(DecoderUtil.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final ObjectMapper avroObjectMapper = new AvroMapper(new AvroFactory());
  public static final byte MAGIC_BYTE = 0x0;

  /**
   * Decodes a Base64-encoded string and deserializes it using the provided SchemaRegistryClient.
   *
   * @param rawBase64           The Base64-encoded string to decode and deserialize.
   * @param schemaRegistryClient The SchemaRegistryClient used for deserialization.
   * @param topicName The name of topic.
   * @return The deserialized JsonNode, or the original rawBase64 string as TextNode if an error
   *        occurs.
   */
  public static JsonNode decodeAndDeserialize(
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
      log.error("Error decoding Base64 string: " + rawBase64, e);
      return TextNode.valueOf(rawBase64);
    } catch (IOException | RestClientException e) {
      log.error("Error deserializing: " + rawBase64, e);
      return TextNode.valueOf(rawBase64);
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
  public static JsonNode deserializeToJson(
      byte[] bytes,
      SchemaRegistryClient schemaRegistryClient,
      String topicName
  ) throws IOException, RestClientException {
    if (bytes == null || bytes.length == 0) {
      return OBJECT_MAPPER.nullNode();
    }

    final int schemaId = getSchemaIdFromRawBytes(bytes);
    String schemaType = schemaRegistryClient.getSchemaById(schemaId).schemaType();
    return switch (schemaType) {
      case "AVRO" -> handleAvro(bytes, topicName, schemaRegistryClient);
      case "PROTOBUF" -> handleProtobuf(bytes, topicName, schemaRegistryClient);
      case "JSON" -> handleJson(bytes, topicName, schemaRegistryClient);
      default -> OBJECT_MAPPER.readTree(new String(bytes, StandardCharsets.UTF_8));
    };
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