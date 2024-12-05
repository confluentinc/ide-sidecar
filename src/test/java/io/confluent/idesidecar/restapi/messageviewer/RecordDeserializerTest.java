package io.confluent.idesidecar.restapi.messageviewer;

import static io.confluent.idesidecar.restapi.messageviewer.RecordDeserializer.getSchemaIdFromRawBytes;
import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.loadResource;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TextNode;
import io.confluent.idesidecar.restapi.clients.SchemaErrors;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionResponse;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.net.UnknownServiceException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Stream;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

@QuarkusTest
public class RecordDeserializerTest {
  @Inject
  RecordDeserializer recordDeserializer;
  @Inject
  SchemaErrors schemaErrors;
  private final ObjectMapper objectMapper = new ObjectMapper();

  private static final SchemaErrors.SchemaId VALID_SCHEMA_ID = new SchemaErrors.SchemaId("fake_cluster_id", 10008);

  String CONNECTION_1_ID = "c1";

  /**
   * Data containing valid schema ID of 10008, and nothing else.
   * 00000000 00000000 00100111 00011000
   */
  private static final byte[] VALID_SCHEMA_ID_BYTES = new byte[]{0, 0, 0, 39, 24};
  private static final String SAMPLE_TOPIC_NAME = "test-subject";

  private SchemaRegistryClient schemaRegistryClient;

  MessageViewerContext context = new MessageViewerContext(
      null,
      null,
      null,
      null,
      null,
      new SchemaErrors.ConnectionId(CONNECTION_1_ID),
      "testClusterId",
      SAMPLE_TOPIC_NAME);

  @BeforeEach
  public void setup() throws RestClientException, IOException {

    schemaRegistryClient = new SimpleMockSchemaRegistryClient(
        Arrays.asList(
            new ProtobufSchemaProvider(),
            new AvroSchemaProvider(),
            new JsonSchemaProvider()
        )
    );
  }

  @AfterEach
  public void tearDown() {
    schemaErrors.clearByConnectionId(CONNECTION_1_ID);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testDecodeAndDeserialize_NullOrEmptyBase64(boolean isKey) {
    assertNullResult(
        recordDeserializer.deserialize(null, schemaRegistryClient, context, isKey));
    assertEmptyResult(
        recordDeserializer.deserialize(new byte[]{}, schemaRegistryClient, context, isKey));
    assertEmptyResult(
        recordDeserializer.deserialize("".getBytes(), schemaRegistryClient, context, isKey));
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
    var schemaStr = loadResource("message-viewer/schema-avro.json");
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
        context,
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
    var schemaStr = loadResource("message-viewer/schema-avro.json");
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
        context,
        false
    );
    assertNotNull(record);
    // Asserts for the top-level fields
    assertNotNull(record.errorMessage());
  }

  @Test
  public void testDecodeAndDeserializeProtobuf_ValidBase64() throws IOException, RestClientException {
    var schemaStr = loadResource("message-viewer/schema-protobuf.proto");
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
        context,
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
    var schemaStr = loadResource("message-viewer/schema-json.json");
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
        context,
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
    var resp = recordDeserializer.deserialize(null, null, context, isKey);
    assertTrue(resp.value().isNull());
    assertNull(resp.errorMessage());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void parseJsonNodeShouldReturnEmptyStringWhenReceivingEmptyByteArray(boolean isKey) {
    var emptyArray = new byte[0];
    var resp = recordDeserializer.deserialize(emptyArray, null, context, isKey);
    assertEquals(new TextNode(""), resp.value());
    assertNull(resp.errorMessage());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void parseJsonNodeShouldReturnStringIfByteArrayDoesNotStartWithMagicByte(boolean isKey) {
    var rawString = "Team DTX";
    var byteArray = rawString.getBytes(StandardCharsets.UTF_8);
    var resp = recordDeserializer.deserialize(byteArray, null, context, isKey);
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
    var resp = recordDeserializer.deserialize(byteArrayWithMagicByte, null, context, isKey);
    assertEquals(new TextNode(magicByteAsString + rawString), resp.value());
    assertEquals("The value references a schema but we can't find the schema registry", resp.errorMessage());
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

  private static Stream<Arguments> testSchemaFetchFailuresAreCached() {
    var clientWithAuthError = new SimpleMockSchemaRegistryClient()
        .registerWithStatusCode(VALID_SCHEMA_ID.schemaId(), 403);
    var clientWithNetworkError = new SimpleMockSchemaRegistryClient()
        .registerAsNetworkErrored(VALID_SCHEMA_ID.schemaId());

    return Stream.of(
        Arguments.of(clientWithAuthError, true, 0),
        Arguments.of(clientWithAuthError, false, 0),
        Arguments.of(clientWithNetworkError, true, 3),
        Arguments.of(clientWithNetworkError, false, 3)
    );
  }

  @ParameterizedTest
  @MethodSource
  public void testSchemaFetchFailuresAreCached(
      SimpleMockSchemaRegistryClient smc,
      boolean isKey,
      int expectedRetries
  ) throws RestClientException, IOException {
    smc = spy(smc);
    // Assume we have 5 records with the same schema ID
    // We should only fetch the schema once, and then cache the failure for the rest
    RecordDeserializer.DecodedResult resp = null;
    for (int i = 0; i < 10; i++) {
      try {
        resp = recordDeserializer.deserialize(
            // Has a schema ID, so we'll actually try to fetch the schema
            VALID_SCHEMA_ID_BYTES,
            smc,
            context,
            isKey,
            Optional.of(Base64.getEncoder()::encode)
        );
      } catch (Exception e) {
        fail(("Should not have thrown exception %s. Consider caching the error by the schema ID"
            + " to avoid bombarding the schema registry API with requests.")
            .formatted(e.getClass().getSimpleName() + ": " + e.getMessage()));
      }
      assertNotNull(resp.errorMessage());
    }

    // Assert that the schema was tried to be fetched only once
    var initialHit = 1;
    verify(smc, times(initialHit + expectedRetries)).getSchemaById(VALID_SCHEMA_ID.schemaId());
  }


  @TestFactory
  public Stream<DynamicTest>
  testDeserializeRetriesAndCachesSchemaFetchFailuresUponUnhappyStatusCodes() {
    record TestCase(
        int statusCode,
        int maxRetries,
        int expectedTries,
        Boolean isKey
    ) {
      static TestCase nonRetryable(int statusCode) {
        // Expect only 1 try when the status code is non-retryable
        return new TestCase(statusCode, 3, 1, null);
      }

      static TestCase retryable(int statusCode) {
        // Expect 4 tries - 1 initial try + 3 retries
        return new TestCase(statusCode, 3, 4, null);
      }

      TestCase withIsKey(Boolean isKey) {
        return new TestCase(statusCode, maxRetries, expectedTries, isKey);
      }

      @Override
      public String toString() {
        var prefix = TestCase.this.expectedTries == 1 ? "Non" : "";
        return prefix + "RetryableTestCase{" +
            "statusCode=" + statusCode +
            ", maxRetries=" + maxRetries +
            ", expectedTries=" + expectedTries +
            ", isKey=" + isKey +
            '}';
      }

    }

    return Stream.of(
        // Test cases for non-retryable status codes
        TestCase.nonRetryable(400),
        TestCase.nonRetryable(401),
        TestCase.nonRetryable(404),
        TestCase.nonRetryable(405),
        TestCase.nonRetryable(500),
        TestCase.nonRetryable(501),
        // Test cases for retryable status codes
        TestCase.retryable(408),
        TestCase.retryable(429),
        TestCase.retryable(502),
        TestCase.retryable(503),
        TestCase.retryable(504)
    )
        .flatMap(tc -> Stream.of(tc.withIsKey(true), tc.withIsKey(false)))
        .map(input -> DynamicTest.dynamicTest(String.valueOf(input), () -> {
          var mockedSRClient = mock(CachedSchemaRegistryClient.class);
          when(mockedSRClient.getSchemaById(anyInt()))
              .thenThrow(new RestClientException("Mock", input.statusCode, input.statusCode));

          assertSchemaFetchFailuresCached(
              input.maxRetries,
              input.expectedTries,
              input.isKey,
              mockedSRClient
          );
        }));
  }

  @TestFactory
  public Stream<DynamicTest>
  testDeserializeRetriesAndCachesSchemaFetchFailuresUponNetworkErrors() {
    return Stream.of(
        // Test a bunch of exceptions that are considered network errors (all extend IOException)
        ConnectException.class,
        SocketTimeoutException.class,
        UnknownHostException.class,
        UnknownServiceException.class
    )
        .map(input -> DynamicTest.dynamicTest(String.valueOf(input), () -> {
          var mockedSRClient = mock(CachedSchemaRegistryClient.class);
          when(mockedSRClient.getSchemaById(anyInt()))
              .thenThrow(input.getDeclaredConstructor().newInstance());

          assertSchemaFetchFailuresCached(
              3,
              4,
              // We don't care about isKey
              false,
              mockedSRClient
          );
        }));
  }

  private void assertSchemaFetchFailuresCached(
      int maxRetries,
      int expectedTries,
      Boolean isKey,
      CachedSchemaRegistryClient mockedSRClient
  ) throws IOException, RestClientException {
    schemaErrors.clearByConnectionId(CONNECTION_1_ID);

    // Create a new record with a schema ID
    RecordDeserializer.DecodedResult resp;
    resp = recordDeserializer.deserialize(
        // Has a schema ID, so we'll actually try to fetch the schema
        VALID_SCHEMA_ID_BYTES,
        mockedSRClient,
        context,
        isKey
    );

    // Assert before trying to deserialize with the same schema ID
    verify(mockedSRClient, times(expectedTries)).getSchemaById(VALID_SCHEMA_ID.schemaId());

    // Now simulate being called with the same schema ID again
    for (int i = 0; i < 5; i++) {
      try {
        resp = recordDeserializer.deserialize(
            VALID_SCHEMA_ID_BYTES,
            mockedSRClient,
            context,
            isKey
        );
      } catch (Exception e) {
        fail(("Should not have thrown exception %s. Consider caching the error by the schema ID"
            + " to avoid bombarding the schema registry API with requests.")
            .formatted(e.getClass().getSimpleName() + ": " + e.getMessage()));
      }
      assertNotNull(resp.errorMessage());
    }

    // Assert after trying to deserialize with the same schema ID
    verify(mockedSRClient, times(expectedTries)).getSchemaById(anyInt());

    schemaErrors.clearByConnectionId(CONNECTION_1_ID);

  }

  @TestFactory
  public Stream<DynamicTest> testDeserializeWithUnexpectedExceptionsIsActuallyThrown() {
    return Stream.of(
        IllegalArgumentException.class,
        IllegalStateException.class,
        NullPointerException.class,
        ArrayIndexOutOfBoundsException.class
    )
        .map(input -> DynamicTest.dynamicTest(String.valueOf(input), () -> {
          var mockedSRClient = mock(CachedSchemaRegistryClient.class);
          when(mockedSRClient.getSchemaById(anyInt()))
              .thenThrow(input.getDeclaredConstructor().newInstance());

          // Simulate a persistent failure that we don't expect
          for (int i = 0; i < 10; i++) {
//            var recordDeserializer = getDeserializer(3);
            try {
              recordDeserializer.deserialize(
                  VALID_SCHEMA_ID_BYTES,
                  mockedSRClient,
                  context,
                  // We don't care about isKey
                  false
              );
            } catch (RuntimeException e) {
              assertInstanceOf(input, e.getCause());
            } catch (Exception e) {
              fail("Should have thrown a RuntimeException, not %s".formatted(e.getClass().getSimpleName()));
            }
          }

          // Check that we tried to fetch the schema 10 times
          // but not any more than that because we're not retrying
          verify(mockedSRClient, times(10)).getSchemaById(anyInt());
        }));
  }
}