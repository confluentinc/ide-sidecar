package io.confluent.idesidecar.restapi.messageviewer;

import static io.confluent.idesidecar.restapi.messageviewer.RecordDeserializer.getSchemaIdFromRawBytes;
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
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.quarkus.test.junit.QuarkusTest;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.apache.avro.Schema.Parser;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@QuarkusTest
public class RecordDeserializerTest {

  private final ObjectMapper objectMapper = new ObjectMapper();

  private SchemaRegistryClient schemaRegistryClient;

  @Inject
  RecordDeserializer recordDeserializer;

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

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testDecodeAndDeserialize_NullOrEmptyBase64(boolean isKey) {
    assertNullResult(
        recordDeserializer.deserialize(null, schemaRegistryClient, "", isKey));
    assertEmptyResult(
        recordDeserializer.deserialize(new byte[]{}, schemaRegistryClient, "", isKey));
    assertEmptyResult(
        recordDeserializer.deserialize("".getBytes(), schemaRegistryClient, "", isKey));
  }

  private void assertNullResult(RecordDeserializer.DecodedResult result) {
    assertTrue(result.value().isNull());
    assertNull(result.errorMessage());
  }

  private void assertEmptyResult(RecordDeserializer.DecodedResult result) {
    assertTrue(result.value().isEmpty());
    assertNull(result.errorMessage());
  }

  @Test
  public void testDecodeAndDeserialize_ValidBase64() throws IOException, RestClientException {
    var schemaStr = new String(Objects.requireNonNull(
        Thread
            .currentThread()
            .getContextClassLoader()
            .getResourceAsStream(
                "message-viewer/schema-avro.json")).readAllBytes());
    var parsedSchema = new AvroSchema(schemaStr);
    var smsrc = (SimpleMockSchemaRegistryClient) schemaRegistryClient;
    // This is raw text of actual record from the stag cluster which is prefixed with 100002 schemaId.
    var raw = "AAABhqKm2oqJtVgGDkl0ZW1fODY0Pkl7kE0hQAxDaXR5XzkOU3RhdGVfOf73Cg==";
    var schemaId = smsrc.register(100002, "test-subject-value", parsedSchema);
    var decodedBytes = Base64.getDecoder().decode(raw);
    var actualSchemaId = RecordDeserializer.getSchemaIdFromRawBytes(decodedBytes);
    assertEquals(schemaId, actualSchemaId);
    var record = recordDeserializer.deserialize(
        decodedBytes,
        schemaRegistryClient,
        "test-subject",
        false
    );
    assertNotNull(record);
    // Asserts for the top-level fields
    assertEquals(1518951552659L, record.value().get("ordertime").asLong(), "ordertime does not match");
    assertNull(record.errorMessage());
    assertEquals(3, record.value().get("orderid").asInt(), "orderid does not match");
    assertEquals("Item_86", record.value().get("itemid").asText(), "itemid does not match");
    assertEquals(8.651492932024759, record.value().get("orderunits").asDouble(), "orderunits does not match");

    // Asserts for the nested 'address' object
    var addressNode = record.value().get("address");
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
    var parsedSchema = new AvroSchema(schemaStr);
    var smsrc = (SimpleMockSchemaRegistryClient) schemaRegistryClient;
    // This is raw text of actual record from the stag cluster which is prefixed with 100002 schemaId.
    var raw = "AAABhqKm2oqJtVgGDkl0ZW1fODY0Pkl7kE0hQAxDaXR5XzkOU3RhdGVfOf73Cg==";
    var schemaId = smsrc.register(100002, "test-subject-value", parsedSchema);
    var decodedBytes = Base64.getDecoder().decode(raw);
    var actualSchemaId = RecordDeserializer.getSchemaIdFromRawBytes(decodedBytes);
    assertEquals(schemaId, actualSchemaId);
    var record = recordDeserializer.deserialize(
        // Remove the last byte to make the base64 string invalid
        Arrays.copyOfRange(decodedBytes, 0, decodedBytes.length - 1),
        schemaRegistryClient,
        "test-subject",
        false
    );
    assertNotNull(record);
    // Asserts for the top-level fields
    assertNotNull(record.errorMessage());
  }

  @Test
  public void testDecodeAndDeserializeProtobuf_ValidBase64() throws IOException, RestClientException {
    var schemaStr = new String(Objects.requireNonNull(
        Thread
            .currentThread()
            .getContextClassLoader()
            .getResourceAsStream(
                "message-viewer/schema-protobuf.proto")).readAllBytes());
    var parsedSchema = new ProtobufSchema(schemaStr);
    var smsrc = (SimpleMockSchemaRegistryClient) schemaRegistryClient;
    // This is raw text of actual record from the stag cluster which is prefixed with 100003 schemaId.
    var raw = "AAABhqMACJTg0YGSLBD/7x8aBkl0ZW1fMyGiH5dsO2sUQCoXCgdDaXR5XzgzEghTdGF0ZV81NBiO/wQ=";
    var schemaId = smsrc.register(100003, "test-subject-value", parsedSchema);
    var decodedBytes = Base64.getDecoder().decode(raw);
    var actualSchemaId = RecordDeserializer.getSchemaIdFromRawBytes(decodedBytes);
    assertEquals(schemaId, actualSchemaId);
    var record = recordDeserializer.deserialize(
        decodedBytes,
        schemaRegistryClient,
        "test-subject",
        false
    );
    assertNotNull(record);
    // Asserts for the top-level fields
    assertEquals("1516663762964", record.value().get("ordertime").asText(), "ordertime does not match");
    assertEquals(522239, record.value().get("orderid").asInt(), "orderid does not match");
    assertEquals("Item_3", record.value().get("itemid").asText(), "itemid does not match");
    assertEquals(5.10471887276063, record.value().get("orderunits").asDouble(), "orderunits does not match");

    // Asserts for the nested 'address' object
    var addressNode = record.value().get("address");
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
    var parsedSchema = new JsonSchema(schemaStr);
    var smsrc = (SimpleMockSchemaRegistryClient) schemaRegistryClient;
    // This is raw text of actual record from the stag cluster which is prefixed with 100001 schemaId.
    var schemaId = smsrc.register(100001, "test-subject-value", parsedSchema);
    var rawValue = "AAABhqF7Im9yZGVydGltZSI6MTUxNzk3MDEyNjg2OSwib3JkZXJpZCI6MTE0LCJpdGVtaWQiOiJJdGVtXzciLCJvcmRlcnVuaXRzIjo4LjcwMTc4NjYyODExMjk2NSwiYWRkcmVzcyI6eyJjaXR5IjoiQ2l0eV8iLCJzdGF0ZSI6IlN0YXRlXzI2IiwiemlwY29kZSI6Njc1ODB9fQ==";
    var decodedBytes = Base64.getDecoder().decode(rawValue);
    var actualSchemaId = RecordDeserializer.getSchemaIdFromRawBytes(decodedBytes);
    assertEquals(schemaId, actualSchemaId);
    var record = recordDeserializer.deserialize(
        decodedBytes,
        schemaRegistryClient,
        "test-subject",
        false
    );
    assertNotNull(record);

    // Asserts for the top-level fields
    assertEquals(1517970126869L, record.value().get("ordertime").asLong(), "ordertime does not match");
    assertEquals(114, record.value().get("orderid").asInt(), "orderid does not match");
    assertEquals("Item_7", record.value().get("itemid").asText(), "itemid does not match");
    assertEquals(8.701786628112965, record.value().get("orderunits").asDouble(), "orderunits does not match");

    // Asserts for the nested 'address' object
    var addressNode = record.value().get("address");
    assertNotNull(addressNode, "address is null");
    assertEquals("City_", addressNode.get("city").asText(), "city does not match");
    assertEquals("State_26", addressNode.get("state").asText(), "state does not match");
    assertEquals(67580, addressNode.get("zipcode").asInt(), "zipcode does not match");
  }

  @Test
  public void testGetSchemaIdFromRawBytes_ValidBytes() {
    byte[] validBytes = new byte[]{0, 0, 0, 0, 1};
    assertEquals(1, getSchemaIdFromRawBytes(validBytes));
  }

  @Test
  public void testGetSchemaIdFromRawBytes_InvalidBytes() {
    assertThrows(IllegalArgumentException.class, () -> getSchemaIdFromRawBytes(null));
    assertThrows(IllegalArgumentException.class, () -> getSchemaIdFromRawBytes(new byte[]{0, 1, 2}));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void parseJsonNodeShouldReturnNullNodeWhenReceivingNullValue(boolean isKey) {
    var resp = recordDeserializer.deserialize(null, null, "foo", isKey);
    assertTrue(resp.value().isNull());
    assertNull(resp.errorMessage());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void parseJsonNodeShouldReturnEmptyStringWhenReceivingEmptyByteArray(boolean isKey) {
    var emptyArray = new byte[0];
    var resp = recordDeserializer.deserialize(emptyArray, null, "foo", isKey);
    assertEquals(new TextNode(""), resp.value());
    assertNull(resp.errorMessage());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void parseJsonNodeShouldReturnStringIfByteArrayDoesNotStartWithMagicByte(boolean isKey) {
    var rawString = "Team DTX";
    var byteArray = rawString.getBytes(StandardCharsets.UTF_8);
    var resp = recordDeserializer.deserialize(byteArray, null, "foo", isKey);
    assertEquals(new TextNode(rawString), resp.value());
    assertNull(resp.errorMessage());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void parseJsonNodeShouldReturnStringIfParsingByteArrayWithMagicByteFails(boolean isKey) {
    // Build byte array with magic byte as prefix
    var rawString = "{\"Team\" : \"DTX\"}";
    var byteArray = rawString.getBytes(StandardCharsets.UTF_8);
    var byteArrayWithMagicByte = new byte[1 + byteArray.length];
    byteArrayWithMagicByte[0] = RecordDeserializer.MAGIC_BYTE;
    System.arraycopy(byteArray, 0, byteArrayWithMagicByte, 1, byteArray.length);

    // Expect parsing to fail, should return byte array as string
    var magicByteAsString = new String(new byte[]{RecordDeserializer.MAGIC_BYTE}, StandardCharsets.UTF_8);
    var resp = recordDeserializer.deserialize(byteArrayWithMagicByte, null, "foo", isKey);
    assertEquals(new TextNode(magicByteAsString + rawString), resp.value());
    assertEquals("The value references a schema but we can't find the schema registry", resp.errorMessage());
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
        keyNode, valueNode, null // NULL exceeding fields
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

  @Test
  public void testDeserializeToJson_SchemaFetchFailure_403Unauthorized_ThenCacheFailure() throws IOException, RestClientException {
    // Register an unauthenticated schema ID (403 Unauthorized)
    var smc = new SimpleMockSchemaRegistryClient();
    var avroSchema = new Parser().parse("{\"type\": \"record\", \"name\": \"TestRecord\", \"fields\": [{\"name\": \"testField\", \"type\": \"string\"}]}");
    var topicName = "test-topic";
    var schemaId = smc.register(100003, topicName + "-value", new AvroSchema(avroSchema));
    smc.registerUnAuthenticated(schemaId);
    var rawBytes = createRawBytes(schemaId); // Mock the bytes with schema ID

    // First attempt - will return error message due to unauthorized schema
    var firstResult = recordDeserializer
        .deserialize(rawBytes, smc, topicName, false);
    assertNotNull(firstResult.errorMessage());
    assertTrue(firstResult.errorMessage().contains("error code: 40301"));

    // Second attempt - should skip schema fetching and return the same error message
    var secondResult = recordDeserializer
        .deserialize(rawBytes, smc, topicName, false);
    assertNotNull(secondResult.errorMessage());
    assertTrue(secondResult.errorMessage().contains("Failed to retrieve schema"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testDecodeAndDeserializeProtobuf_UnauthorizedSchemaRegistry(boolean isKey) {
    var smc = new SimpleMockSchemaRegistryClient();
    var raw = "AAABhqMACJTg0YGSLBD/7x8aBkl0ZW1fMyGiH5dsO2sUQCoXCgdDaXR5XzgzEghTdGF0ZV81NBiO/wQ=";
    var decodedBytes = Base64.getDecoder().decode(raw);
    var actualSchemaId = RecordDeserializer.getSchemaIdFromRawBytes(decodedBytes);
    smc.registerUnAuthenticated(actualSchemaId);

    // An error to fetch schema, should result in response containing the original base64 string.
    var resp = recordDeserializer.deserialize(
        decodedBytes,
        smc,
        "test-subject",
        isKey,
        Optional.of(Base64.getEncoder()::encode)
    );
    assertNotNull(resp.errorMessage());
    assertTrue(resp.errorMessage().contains("error code: 40301"));
    assertEquals(raw, resp.value().asText());

    // The second fetch from cache should also return original rawBase64 String.
    var resp2 = recordDeserializer.deserialize(
        decodedBytes,
        smc,
        "test-subject",
        isKey,
        Optional.of(Base64.getEncoder()::encode)
    );
    assertNotNull(resp2.errorMessage());
    assertTrue(resp2.errorMessage().contains("Failed to retrieve schema"));
    assertEquals(raw, resp2.value().asText());
  }

  /**
   * Helper method to create raw bytes with schema ID.
   */
  private byte[] createRawBytes(int schemaId) {
    var schemaIdBytes = ByteBuffer.allocate(5).put(RecordDeserializer.MAGIC_BYTE).putInt(schemaId).array();
    var sampleData = "{\"testField\":\"testValue\"}";
    var dataBytes = sampleData.getBytes(StandardCharsets.UTF_8);

    // Combine schema ID and data bytes
    var combinedBytes = new byte[schemaIdBytes.length + dataBytes.length];
    System.arraycopy(schemaIdBytes, 0, combinedBytes, 0, schemaIdBytes.length);
    System.arraycopy(dataBytes, 0, combinedBytes, schemaIdBytes.length, dataBytes.length);
    return combinedBytes;
  }
}