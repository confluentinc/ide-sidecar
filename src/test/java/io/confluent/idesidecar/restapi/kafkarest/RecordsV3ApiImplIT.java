package io.confluent.idesidecar.restapi.kafkarest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.idesidecar.restapi.messageviewer.data.ConsumeRequest;
import io.confluent.idesidecar.restapi.messageviewer.data.ConsumeRequestBuilder;
import io.confluent.idesidecar.restapi.testutil.NoAccessFilterProfile;
import io.confluent.idesidecar.restapi.util.ConfluentLocalTestBed;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junitpioneer.jupiter.cartesian.ArgumentSets;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.loadResource;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

@QuarkusIntegrationTest
@Tag("io.confluent.common.utils.IntegrationTest")
@TestProfile(NoAccessFilterProfile.class)
class RecordsV3ApiImplIT extends ConfluentLocalTestBed {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  record RecordData(
      SchemaManager.SchemaFormat schemaFormat,
      String rawSchema,
      Object data
  ) {

    @Override
    public String toString() {
      return "(data = %s, schemaFormat = %s, schema = %s)"
          .formatted(data, schemaFormat, rawSchema);
    }
  }

  static RecordData schemaData(SchemaManager.SchemaFormat format, String jsonData) {
    var schema = getProductSchema(format);

    try {
      return new RecordData(format, schema, OBJECT_MAPPER.readTree(jsonData));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static String getProductSchema(SchemaManager.SchemaFormat format) {
    return switch (format) {
      case JSON: {
        yield loadResource("schemas/product.schema.json").translateEscapes();
      }
      case AVRO: {
        yield loadResource("schemas/product.avsc").translateEscapes();
      }
      case PROTOBUF: {
        yield loadResource("schemas/product.proto").translateEscapes();
      }
    };
  }

  @Nested
  // Do we really need to specify this TestProfile again? Yes. Why? I don't know.
  @TestProfile(NoAccessFilterProfile.class)
  class SadPath {
    @Test
    void shouldThrowNotFoundWhenClusterDoesNotExist() {
      givenConnectionId()
          .header("Content-Type", "application/json")
          .body(createProduceRequest(
              null, "key", null, "value", null
          ))
          .post("/kafka/v3/clusters/non-existent-cluster/topics/foo/records")
          .then()
          .statusCode(404)
          .body("message", equalTo("Kafka cluster 'non-existent-cluster' not found."));
    }

    @Test
    void shouldThrowNotFoundWhenTopicDoesNotExist() {
      produceRecordThen(null, "non-existent-topic", null, null)
          .statusCode(404)
          .body("message", equalTo("This server does not host this topic-partition."));
    }

    @Test
    void shouldThrowNotFoundWhenPartitionDoesNotExist() {
      var topic = randomTopicName();
      createTopic(topic, 3, 1);
      produceRecordThen(10, topic, "key", "value")
          .statusCode(404)
          .body(
              "message", equalTo("This server does not host this topic-partition."));
    }

    /**
     * Inputs for test cases for when the key and value schema versions are not found.
     * @return the Arguments for the test cases
     */
    static Stream<Arguments> keyAndValueSchemaVersions() {
      return Stream.of(
          Arguments.of(40, null),
          Arguments.of(null, 40),
          Arguments.of(40, 40)
      );
    }

    @ParameterizedTest
    @MethodSource("keyAndValueSchemaVersions")
    void shouldThrowNotFoundWhenSubjectDoesNotExist(
        Integer keySchemaVersion, Integer valueSchemaVersion
    ) {
      var topic = randomTopicName();
      createTopic(topic);
      // Schema Registry should fail to find the subject before it even gets to the schema
      // version check
      produceRecordThen(
          null, topic, "key", keySchemaVersion, "value", valueSchemaVersion)
          .statusCode(404)
          .body("message",
              matchesRegex("^Subject '%s-(key|value)' not found\\.; error code: 40401$"
                  .formatted(topic))
          );
    }

    @ParameterizedTest
    @MethodSource("keyAndValueSchemaVersions")
    void shouldThrowNotFoundWhenSchemaVersionDoesNotExist(
        Integer keySchemaVersion, Integer valueSchemaVersion) {
      var topic = randomTopicName();
      createTopic(topic);
      createSchema(
          "%s-key".formatted(topic),
          "JSON",
          loadResource("schemas/product.schema.json")
      );
      createSchema(
          "%s-value".formatted(topic),
          "PROTOBUF",
          loadResource("schemas/product.proto")
      );
      // Schema version 1 would be created by the above calls,
      // but the following call should fail to find version 40
      // (I mean, who even has 40 versions of a schema?)
      produceRecordThen(
          null, topic, "key", keySchemaVersion, "value", valueSchemaVersion)
          .statusCode(404)
          .body("message", matchesRegex("^Version \\d+ not found.; error code: 40402$"));
    }

    @Test
    void shouldThrowBadRequestIfKeyAndValueDataAreNull() {
      var topic = randomTopicName();
      createTopic(topic);
      produceRecordThen(null, topic, null, null)
          .statusCode(400)
          .body("message", equalTo("Key and value data cannot both be null"));
    }

    /**
     * Inputs for test cases for when the schema is not compatible with the data.
     * @return the ArgumentSets with the combinations of schema formats and bad data
     */
    static ArgumentSets badData() {
      return ArgumentSets
          .argumentsForFirstParameter(
              SchemaManager.SchemaFormat.AVRO,
              SchemaManager.SchemaFormat.JSON
          )
          .argumentsForNextParameter(
              Stream.of(
                  Map.of(),
                  Map.of(
                      "id", 123
                  ),
                  Map.of(
                      "id", 123,
                      "name", 50
                  ),
                  Map.of(
                      "id", "invalid",
                      "name", "hello",
                      "price", 123.45
                  ),
                  Map.of(
                      "id", 10,
                      "name", List.of("hello", "world"),
                      "price", 123.45
                  )
              )
          );
    }

    @CartesianTest
    @CartesianTest.MethodFactory("badData")
    void shouldThrowBadRequestIfSchemaIsNotCompatibleWithData(
        SchemaManager.SchemaFormat keyFormat, Object badData
    ) {
      var topic = randomTopicName();
      createTopic(topic);
      var keySchema = createSchema(
          "%s-key".formatted(topic),
          keyFormat.schemaProvider().schemaType(),
          getProductSchema(keyFormat)
      );

      produceRecordThen(
          null, topic, badData, keySchema.getVersion(), null, null)
          .statusCode(400)
          .body("message", containsString("Failed to parse data"));

      var valueSchema = createSchema(
          "%s-value".formatted(topic),
          keyFormat.schemaProvider().schemaType(),
          getProductSchema(keyFormat)
      );

      produceRecordThen(
          null, topic, null, null, badData, valueSchema.getVersion())
          .statusCode(400)
          .body("message", containsString("Failed to parse data"));
    }

    /**
     * Inputs for test cases for when the (Protobuf) schema is not compatible with the data.
     * @return the arguments for the test cases
     */
    static Stream<Arguments> invalidProtobufData() {
      return Stream.of(
          Arguments.of(
              Map.of("id", "invalid", "name", "hello", "price", 123.45)),
          Arguments.of(
              Map.of("id", 10, "name", List.of("hello", "world"), "price", 123.45))
      );
    }

    // Special treatment for protobuf data since it's more liberal in schema validation
    // and there is no way to declare required fields in protobuf.
    @ParameterizedTest
    @MethodSource("invalidProtobufData")
    void shouldThrowBadRequestForInvalidProtobufData(Object badData) {
      shouldThrowBadRequestIfSchemaIsNotCompatibleWithData(
          SchemaManager.SchemaFormat.PROTOBUF, badData
      );
    }
  }

  // Happy path
  @Nested
  @TestProfile(NoAccessFilterProfile.class)
  class HappyPath {
    private static RecordData jsonData(Object data) {
      return new RecordData(null, null, data);
    }

    private final static String PRODUCT_DATA = """
      {
        "id": 123,
        "name": "hello",
        "price": 123.45,
        "tags": ["hello", "world"]
      }
      """;

    private final static List<RecordData> RECORD_DATA_VALUES = List.of(
        jsonData(null),
        jsonData("hello"),
        jsonData(123),
        jsonData(123.45),
        jsonData(true),
        jsonData(List.of("hello", "world")),
        jsonData(Collections.singletonMap("hello", "world")),
        schemaData(SchemaManager.SchemaFormat.JSON, PRODUCT_DATA),
        schemaData(SchemaManager.SchemaFormat.PROTOBUF, PRODUCT_DATA),
        schemaData(SchemaManager.SchemaFormat.AVRO, PRODUCT_DATA)
    );

    /**
     * Valid keys and values inputs used for producing and consuming data.
     * @return the sets of keys and sets of values
     */
    static ArgumentSets validKeysAndValues() {
      return ArgumentSets
          .argumentsForFirstParameter(RECORD_DATA_VALUES)
          .argumentsForNextParameter(RECORD_DATA_VALUES);
    }

    private static void assertSame(JsonNode actual, Object expected) {
      if (expected != null) {
        var parsedKey = OBJECT_MAPPER.convertValue(
            expected,
            expected.getClass()
        );
        assertEquals(expected, parsedKey);
      } else {
        assertTrue(actual.isNull());
      }
    }

    @CartesianTest
    @CartesianTest.MethodFactory("validKeysAndValues")
    void testProduceAndConsumeData(RecordData key, RecordData value) {
      var topicName = randomTopicName();
      // Create topic with a single partition
      createTopic(topicName);

      Schema keySchema = null, valueSchema = null;

      // Use TopicNameStrategy by default for subject names
      // Create key schema if not null
      if (key.rawSchema() != null) {
        keySchema = createSchema(
            topicName + "-key",
            key.schemaFormat().name(),
            key.rawSchema()
        );
      }

      // Create value schema if not null
      if (value.rawSchema() != null) {
        valueSchema = createSchema(
            topicName + "-value",
            value.schemaFormat().name(),
            value.rawSchema()
        );
      }

      // Produce record to topic
      var resp = produceRecordThen(
          null,
          topicName,
          key.data(),
          Optional.ofNullable(keySchema).map(Schema::getVersion).orElse(null),
          value.data(),
          Optional.ofNullable(valueSchema).map(Schema::getVersion).orElse(null)
      );

      if (key.data() != null || value.data() != null) {
        resp.statusCode(200);
        assertTopicHasRecord(key, value, topicName);
      } else {
        // A "SadPath" test in a "HappyPath" test?! Blasphemy!
        // Easier to catch and assert this here than create a separate test case for
        // passing nulls.
        resp.statusCode(400)
            .body("message", equalTo("Key and value data cannot both be null"));
      }
    }

    private static void assertTopicHasRecord(RecordData key, RecordData value, String topicName) {
      var consumeResponse = consume(
          topicName,
          ConsumeRequestBuilder
              .builder()
              .fromBeginning(true)
              .maxPollRecords(1)
              .build()
      );

      var records = consumeResponse
          .partitionDataList()
          // Assuming single partition
          .getFirst()
          .records();

      assertEquals(records.size(), 1);

      assertSame(records.getFirst().key(), key.data());
      assertSame(records.getFirst().value(), value.data());
    }

    @Test
    void shouldProduceRecordToPartition() {
      var topicName = randomTopicName();
      createTopic(topicName, 5, 1);

      produceRecord(0, topicName, null, "value0");
      produceRecord(0, topicName, null, "value1");
      produceRecord(1, topicName, null, "value2");
      produceRecord(1, topicName, null, "value3");
      produceRecord(2, topicName, null, "value4");
      produceRecord(2, topicName, null, "value5");
      produceRecord(3, topicName, null, "value6");
      produceRecord(3, topicName, null, "value7");
      produceRecord(4, topicName, null, "value8");
      produceRecord(4, topicName, null, "value9");

      var resp = consume(topicName, ConsumeRequestBuilder
          .builder()
          .fromBeginning(true)
          .partitionOffsets(
              List.of(
                  new ConsumeRequest.PartitionOffset(0, 0),
                  new ConsumeRequest.PartitionOffset(1, 0),
                  new ConsumeRequest.PartitionOffset(2, 0),
                  new ConsumeRequest.PartitionOffset(3, 0),
                  new ConsumeRequest.PartitionOffset(4, 0)
              ))
          .build()
      );

      for (var partition : resp.partitionDataList()) {
        var records = partition.records();

        // Assert greater than or equal to 2 since consume API
        // may return more records than asked for, but there should be at least 2 records
        // in each partition
        assertTrue(records.size() >= 2);
      }
    }
  }
}