package io.confluent.idesidecar.restapi.messageviewer;

import static io.confluent.idesidecar.restapi.messageviewer.DecoderUtil.getSchemaIdFromRawBytes;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TextNode;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionResponse;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


public class DecoderUtilTest {

  private final ObjectMapper objectMapper = new ObjectMapper();
  private static final String VALID_BASE64 = "valid_base64_string";
  private static final String INVALID_BASE64 = "invalid_base64_string";
  private static final byte[] VALID_BYTES = new byte[]{0, 1, 2, 3, 4};
  private static final byte[] INVALID_BYTES = new byte[]{0, 1, 2};
  private static final int SCHEMA_ID = 1;

  private SchemaRegistryClient schemaRegistryClient;

  @BeforeEach
  public void setup() {
    schemaRegistryClient = new SimpleMockSchemaRegistryClient(
        Arrays.asList(
            new ProtobufSchemaProvider(),
            new AvroSchemaProvider(),
            new JsonSchemaProvider()
        )
    );
  }

  @Test
  public void testDecodeAndDeserialize_NullOrEmptyBase64() {
    assertNull(DecoderUtil.decodeAndDeserialize(null, schemaRegistryClient, ""));
    assertNull(DecoderUtil.decodeAndDeserialize("", schemaRegistryClient, ""));
  }

  @Test
  public void testDecodeAndDeserialize_ValidBase64() throws IOException, RestClientException {
    var schemaStr = new String(Objects.requireNonNull(
        Thread
            .currentThread()
            .getContextClassLoader()
            .getResourceAsStream(
                "message-viewer/schema-avro.json")).readAllBytes());
    ParsedSchema parsedSchema = new AvroSchema(schemaStr);
    SimpleMockSchemaRegistryClient smsrc = (SimpleMockSchemaRegistryClient) schemaRegistryClient;
    // This is raw text of actual record from the stag cluster which is prefixed with 100002 schemaId.
    String raw = "AAABhqKm2oqJtVgGDkl0ZW1fODY0Pkl7kE0hQAxDaXR5XzkOU3RhdGVfOf73Cg==";
    int schemaId = smsrc.register(100002, "test-subject-value", parsedSchema);
    byte[] decodedBytes = Base64.getDecoder().decode(raw);
    int actualSchemaId = DecoderUtil.getSchemaIdFromRawBytes(decodedBytes);
    assertEquals(schemaId, actualSchemaId);
    var record = DecoderUtil.decodeAndDeserialize(
        raw,
        schemaRegistryClient,
        "test-subject");
    assertNotNull(record);
    // Asserts for the top-level fields
    assertEquals(1518951552659L, record.getValue().get("ordertime").asLong(), "ordertime does not match");
    assertNull(record.getErrorMessage());
    assertEquals(3, record.getValue().get("orderid").asInt(), "orderid does not match");
    assertEquals("Item_86", record.getValue().get("itemid").asText(), "itemid does not match");
    assertEquals(8.651492932024759, record.getValue().get("orderunits").asDouble(), "orderunits does not match");

    // Asserts for the nested 'address' object
    JsonNode addressNode = record.getValue().get("address");
    assertNotNull(addressNode, "address is null");
    assertEquals("City_9", addressNode.get("city").asText(), "city does not match");
    assertEquals("State_9", addressNode.get("state").asText(), "state does not match");
    assertEquals(89599, addressNode.get("zipcode").asInt(), "zipcode does not match");
  }

  @Test
  public void testDecodeAndDeserialize_InValidBase64() throws IOException, RestClientException {
    var schemaStr = new String(Objects.requireNonNull(
        Thread
            .currentThread()
            .getContextClassLoader()
            .getResourceAsStream(
                "message-viewer/schema-avro.json")).readAllBytes());
    ParsedSchema parsedSchema = new AvroSchema(schemaStr);
    SimpleMockSchemaRegistryClient smsrc = (SimpleMockSchemaRegistryClient) schemaRegistryClient;
    // This is raw text of actual record from the stag cluster which is prefixed with 100002 schemaId.
    String raw = "AAABhqKm2oqJtVgGDkl0ZW1fODY0Pkl7kE0hQAxDaXR5XzkOU3RhdGVfOf73Cg==";
    int schemaId = smsrc.register(100002, "test-subject-value", parsedSchema);
    byte[] decodedBytes = Base64.getDecoder().decode(raw);
    int actualSchemaId = DecoderUtil.getSchemaIdFromRawBytes(decodedBytes);
    assertEquals(schemaId, actualSchemaId);
    var record = DecoderUtil.decodeAndDeserialize(
        raw+"FOO",
        schemaRegistryClient,
        "test-subject");
    assertNotNull(record);
    // Asserts for the top-level fields
    assertNotNull(record.getErrorMessage());
  }

  @Test
  public void testDecodeAndDeserializeProtobuf_ValidBase64() throws IOException, RestClientException {
    var schemaStr = new String(Objects.requireNonNull(
        Thread
            .currentThread()
            .getContextClassLoader()
            .getResourceAsStream(
                "message-viewer/schema-protobuf.proto")).readAllBytes());
    ParsedSchema parsedSchema = new ProtobufSchema(schemaStr);
    SimpleMockSchemaRegistryClient smsrc = (SimpleMockSchemaRegistryClient) schemaRegistryClient;
    // This is raw text of actual record from the stag cluster which is prefixed with 100003 schemaId.
    String raw = "AAABhqMACJTg0YGSLBD/7x8aBkl0ZW1fMyGiH5dsO2sUQCoXCgdDaXR5XzgzEghTdGF0ZV81NBiO/wQ=";
    int schemaId = smsrc.register(100003, "test-subject-value", parsedSchema);
    byte[] decodedBytes = Base64.getDecoder().decode(raw);
    int actualSchemaId = DecoderUtil.getSchemaIdFromRawBytes(decodedBytes);
    assertEquals(schemaId, actualSchemaId);
    var record = DecoderUtil.decodeAndDeserialize(
        raw,
        schemaRegistryClient,
        "test-subject");
    assertNotNull(record);
    // Asserts for the top-level fields
    assertEquals("1516663762964", record.getValue().get("ordertime").asText(), "ordertime does not match");
    assertEquals(522239, record.getValue().get("orderid").asInt(), "orderid does not match");
    assertEquals("Item_3", record.getValue().get("itemid").asText(), "itemid does not match");
    assertEquals(5.10471887276063, record.getValue().get("orderunits").asDouble(), "orderunits does not match");

    // Asserts for the nested 'address' object
    JsonNode addressNode = record.getValue().get("address");
    assertNotNull(addressNode, "address is null");
    assertEquals("City_83", addressNode.get("city").asText(), "city does not match");
    assertEquals("State_54", addressNode.get("state").asText(), "state does not match");
    assertEquals("81806", addressNode.get("zipcode").asText(), "zipcode does not match");
  }

  @Test
  public void testDecodeAndDeserializeJsonSR_ValidBase64() throws IOException, RestClientException {
    var schemaStr = new String(Objects.requireNonNull(
        Thread
            .currentThread()
            .getContextClassLoader()
            .getResourceAsStream(
                "message-viewer/schema-json.json")).readAllBytes());
    JsonSchema parsedSchema = new JsonSchema(schemaStr);
    SimpleMockSchemaRegistryClient smsrc = (SimpleMockSchemaRegistryClient) schemaRegistryClient;
    // This is raw text of actual record from the stag cluster which is prefixed with 100001 schemaId.
    int schemaId = smsrc.register(100001, "test-subject-value", parsedSchema);
    String raw = "AAABhqF7Im9yZGVydGltZSI6MTUxNzk3MDEyNjg2OSwib3JkZXJpZCI6MTE0LCJpdGVtaWQiOiJJdGVtXzciLCJvcmRlcnVuaXRzIjo4LjcwMTc4NjYyODExMjk2NSwiYWRkcmVzcyI6eyJjaXR5IjoiQ2l0eV8iLCJzdGF0ZSI6IlN0YXRlXzI2IiwiemlwY29kZSI6Njc1ODB9fQ==";
    byte[] decodedBytes = Base64.getDecoder().decode(raw);
    int actualSchemaId = DecoderUtil.getSchemaIdFromRawBytes(decodedBytes);
    assertEquals(schemaId, actualSchemaId);
    var record = DecoderUtil.decodeAndDeserialize(
        raw,
        schemaRegistryClient,
        "test-subject"
    );
    assertNotNull(record);

    // Asserts for the top-level fields
    assertEquals(1517970126869L, record.getValue().get("ordertime").asLong(), "ordertime does not match");
    assertEquals(114, record.getValue().get("orderid").asInt(), "orderid does not match");
    assertEquals("Item_7", record.getValue().get("itemid").asText(), "itemid does not match");
    assertEquals(8.701786628112965, record.getValue().get("orderunits").asDouble(), "orderunits does not match");

    // Asserts for the nested 'address' object
    JsonNode addressNode = record.getValue().get("address");
    assertNotNull(addressNode, "address is null");
    assertEquals("City_", addressNode.get("city").asText(), "city does not match");
    assertEquals("State_26", addressNode.get("state").asText(), "state does not match");
    assertEquals(67580, addressNode.get("zipcode").asInt(), "zipcode does not match");
  }

  @Test
  public void testGetSchemaIdFromRawBytes_ValidBytes() {
    byte[] validBytes = new byte[]{0, 0, 0, 0, SCHEMA_ID};
    assertEquals(SCHEMA_ID, getSchemaIdFromRawBytes(validBytes));
  }

  @Test
  public void testGetSchemaIdFromRawBytes_InvalidBytes() {
    assertThrows(IllegalArgumentException.class, () -> getSchemaIdFromRawBytes(null));
    assertThrows(IllegalArgumentException.class, () -> getSchemaIdFromRawBytes(INVALID_BYTES));
  }

  @Test
  void parseJsonNodeShouldReturnEmptyStringWhenReceivingNullValue() {
    assertEquals(new TextNode(""), DecoderUtil.parseJsonNode(null));
  }

  @Test
  void parseJsonNodeShouldReturnEmptyStringWhenReceivingEmptyByteArray() {
    var emptyArray = new byte[0];
    assertEquals(new TextNode(""), DecoderUtil.parseJsonNode(emptyArray));
  }

  @Test
  void parseJsonNodeShouldReturnStringIfByteArrayDoesNotStartWithMagicByte() {
    var rawString = "Team DTX";
    var byteArray = rawString.getBytes(StandardCharsets.UTF_8);
    assertEquals(new TextNode(rawString), DecoderUtil.parseJsonNode(byteArray));
  }

  @Test
  void parseJsonNodeShouldReturnStringIfParsingByteArrayWithMagicByteFails() {
    // Build byte array with magic byte as prefix
    var rawString = "Team DTX";
    var byteArray = rawString.getBytes(StandardCharsets.UTF_8);
    var byteArrayWithMagicByte = new byte[1 + byteArray.length];
    byteArrayWithMagicByte[0] = DecoderUtil.MAGIC_BYTE;
    System.arraycopy(byteArray, 0, byteArrayWithMagicByte, 1, byteArray.length);

    // Expect parsing to fail, should return byte array as string
    var magicByteAsString = new String(new byte[]{DecoderUtil.MAGIC_BYTE}, StandardCharsets.UTF_8);
    assertEquals(
        new TextNode(magicByteAsString + rawString),
        DecoderUtil.parseJsonNode(byteArrayWithMagicByte));
  }

  @Test
  public void testKeyDecodingErrorAndValueDecodingErrorAreNotSerializedWhenNull() throws JsonProcessingException {
    // Given
    JsonNode keyNode = objectMapper.nullNode();
    JsonNode valueNode = objectMapper.nullNode();

    // Create a PartitionConsumeRecord with null keyDecodingError and valueDecodingError
    SimpleConsumeMultiPartitionResponse.PartitionConsumeRecord record = new SimpleConsumeMultiPartitionResponse.PartitionConsumeRecord(
        0, 100L, System.currentTimeMillis(),
        SimpleConsumeMultiPartitionResponse.TimestampType.CREATE_TIME,
        Collections.emptyList(),
        keyNode, valueNode, null // No decoding errors
    );

    // Create PartitionConsumeData with the record
    SimpleConsumeMultiPartitionResponse.PartitionConsumeData partitionConsumeData = new SimpleConsumeMultiPartitionResponse.PartitionConsumeData(
        0, 101L, List.of(record)
    );

    // Construct the full SimpleConsumeMultiPartitionResponse object
    SimpleConsumeMultiPartitionResponse response = new SimpleConsumeMultiPartitionResponse(
        "test-cluster",
        "test-topic",
        List.of(partitionConsumeData)
    );

    // When
    String serializedResponse = objectMapper.writeValueAsString(response);

    // Then
    assertFalse(serializedResponse.contains("key_decoding_error"), "keyDecodingError should not be present in the serialized JSON");
    assertFalse(serializedResponse.contains("value_decoding_error"), "valueDecodingError should not be present in the serialized JSON");
  }

  @Test
  public void testKeyDecodingErrorAndValueDecodingErrorAreSerializedWhenNotNull() throws JsonProcessingException {
    // Given
    JsonNode keyNode = objectMapper.nullNode();
    JsonNode valueNode = objectMapper.nullNode();
    SimpleConsumeMultiPartitionResponse.PartitionConsumeRecord record = new SimpleConsumeMultiPartitionResponse.PartitionConsumeRecord(
        0, 100L, System.currentTimeMillis(),
        SimpleConsumeMultiPartitionResponse.TimestampType.CREATE_TIME,
        Collections.emptyList(),
        keyNode, valueNode, "Key decoding failed", "Value decoding failed",
        new SimpleConsumeMultiPartitionResponse.ExceededFields(false, false)
    );

    // When
    String serializedRecord = objectMapper.writeValueAsString(record);

    // Then
    assertTrue(serializedRecord.contains("key_decoding_error"), "keyDecodingError should be present in the serialized JSON");
    assertTrue(serializedRecord.contains("value_decoding_error"), "valueDecodingError should be present in the serialized JSON");
  }
}