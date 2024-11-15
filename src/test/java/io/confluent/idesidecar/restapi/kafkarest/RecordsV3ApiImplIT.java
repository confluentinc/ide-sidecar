package io.confluent.idesidecar.restapi.kafkarest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceRequest;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceRequestData;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionRequest;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionRequestBuilder;
import io.confluent.idesidecar.restapi.testutil.NoAccessFilterProfile;
import io.confluent.idesidecar.restapi.util.AbstractSidecarIT;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junitpioneer.jupiter.cartesian.ArgumentSets;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import java.util.Arrays;
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
class RecordsV3ApiImplIT {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  record RecordData(
      SchemaFormat schemaFormat,
      SubjectNameStrategyEnum subjectNameStrategy,
      String rawSchema,
      Object data
  ) {

    public RecordData(SchemaFormat schemaFormat, String rawSchema, Object data) {
      this(schemaFormat, null, rawSchema, data);
    }

    RecordData withSubjectNameStrategy(SubjectNameStrategyEnum subjectNameStrategy) {
      return new RecordData(schemaFormat, subjectNameStrategy, rawSchema, data);
    }

    @Override
    public String toString() {
      return "(data = %s, schemaFormat = %s, subjectNameStrategy = %s)"
          .formatted(data, schemaFormat, subjectNameStrategy);
    }

    public boolean hasSchema() {
      return schemaFormat != null && rawSchema != null && subjectNameStrategy != null;
    }
  }

  private static String getSubjectName(
      String topicName,
      SubjectNameStrategyEnum strategy,
      boolean isKey
  ) {
    /*
    https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#how-the-naming-strategies-work
    */
    return switch (strategy) {
      case TOPIC_NAME -> (isKey ? "%s-key" : "%s-value").formatted(topicName);
      case RECORD_NAME -> (isKey ? "ProductKey" : "ProductValue");
      case TOPIC_RECORD_NAME -> (isKey ? "%s-ProductKey" : "%s-ProductValue").formatted(topicName);
    };
  }

  static RecordData schemaData(SchemaFormat format, Boolean isKey) {
    var schema = getProductSchema(format, isKey);

    try {
      return new RecordData(
          format,
          schema,
          OBJECT_MAPPER.readTree(HappyPath.PRODUCT_DATA)
      );
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static String getProductSchema(SchemaFormat format, boolean isKey) {
    var suffix = isKey ? "key" : "value";
    return switch (format) {
      case JSON: {
        yield loadResource("schemas/product-%s.schema.json".formatted(suffix)).translateEscapes();
      }
      case AVRO: {
        yield loadResource("schemas/product-%s.avsc".formatted(suffix)).translateEscapes();
      }
      case PROTOBUF: {
        yield loadResource("schemas/product-%s.proto".formatted(suffix)).translateEscapes();
      }
    };
  }

  @Nested
  @Tag("io.confluent.common.utils.IntegrationTest")
  @TestProfile(NoAccessFilterProfile.class)
  class SadPathWithLocal extends SadPath {

    @BeforeEach
    public void beforeEach() {
      setupLocalConnection(this);
    }
  }

  @Nested
  @Tag("io.confluent.common.utils.IntegrationTest")
  @TestProfile(NoAccessFilterProfile.class)
  class SadPathWithDirect extends SadPath {

    @BeforeEach
    public void beforeEach() {
      setupDirectConnection(this);
    }
  }

  abstract class SadPath extends AbstractSidecarIT {

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
          loadResource("schemas/product-key.schema.json")
      );
      createSchema(
          "%s-value".formatted(topic),
          "PROTOBUF",
          loadResource("schemas/product-value.proto")
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
              SchemaFormat.AVRO,
              SchemaFormat.JSON
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
        SchemaFormat keyFormat, Object badData
    ) {
      var topic = randomTopicName();
      createTopic(topic);
      var keySchema = createSchema(
          "%s-key".formatted(topic),
          keyFormat.schemaProvider().schemaType(),
          getProductSchema(keyFormat, true)
      );

      produceRecordThen(
          null, topic, badData, keySchema.getVersion(), null, null)
          .statusCode(400)
          .body("message", containsString("Failed to parse data"));

      var valueSchema = createSchema(
          "%s-value".formatted(topic),
          keyFormat.schemaProvider().schemaType(),
          getProductSchema(keyFormat, false)
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
          SchemaFormat.PROTOBUF, badData
      );
    }

    static Stream<Arguments> unsupportedSchemaDetails() {
      return Stream.of(
          Arguments.of(
              ProduceRequestData
                  .builder()
                  .data(Map.of())
                  // Schema ID is not supported
                  .schemaId(1)
                  .build()
          ),
          Arguments.of(
              ProduceRequestData
                  .builder()
                  .data(Map.of())
                  // Passing raw schema is not supported
                  .schema("invalid")
                  // Passing schema type is not supported
                  .type("AVRO")
                  .build()
          ),
          Arguments.of(
              ProduceRequestData
                  .builder()
                  .data(Map.of())
                  // Passing schema version is supported
                  .schemaVersion(1)
                  // But type is not supported
                  .type("PROTOBUF")
                  .build()
          ),
          Arguments.of(
              ProduceRequestData
                  .builder()
                  .data(Map.of())
                  .schemaVersion(1)
                  // Passing only subject is not supported
                  .subject("standalone")
                  .build()
          ),
          Arguments.of(
              ProduceRequestData
                  .builder()
                  .data(Map.of())
                  .schemaVersion(1)
                  // Passing only subject name strategy is not supported
                  .subjectNameStrategy("record_name")
                  .build()
          )
      );
    }

    @ParameterizedTest
    @MethodSource("unsupportedSchemaDetails")
    void shouldThrowNotImplementedForUnsupportedSchemaDetails(ProduceRequestData data) {
      var topic = randomTopicName();
      createTopic(topic);
      produceRecordThen(
          topic,
          ProduceRequest
              .builder()
              .partitionId(null)
              // Doesn't matter if key or value, the schema details within
              // should trigger the 501 response
              .key(data)
              .value(data)
              .build()
      )
          .statusCode(400)
          .body("message", equalTo(
              "This endpoint does not support specifying schema ID, type, schema, standalone subject or subject name strategy."
          ));
    }

    @Test
    void shouldHandleWrongTopicNameStrategy() {
      var topic = randomTopicName();
      createTopic(topic);

      // Create a schema called "foo-key" with a JSON schema (uses TopicNameStrategy)
      var keySchema = createSchema(
          "foo-key",
          "JSON",
          loadResource("schemas/product-key.schema.json")
      );

      // Try to produce a record with the wrong subject name strategy
      produceRecordThen(
          topic,
          ProduceRequest
              .builder()
              .partitionId(null)
              .key(
                  ProduceRequestData
                      .builder()
                      .schemaVersion(keySchema.getVersion())
                      // Pass valid data
                      .data(Map.of(
                          "id", 123,
                          "name", "test",
                          "price", 123.45
                      ))
                      // But wrong subject name strategy
                      .subjectNameStrategy("record_name")
                      .subject("foo-key")
                      .build()
              )
              .value(
                  ProduceRequestData
                      .builder()
                      .data(Map.of())
                      .build()
              )
              .build()
      )
          .statusCode(404)
          .body("message", equalTo(
              // The KafkaJsonSchemaSerializer tries to look up the subject
              // by the record name but fails to find "ProductKey" which is the
              // "title" of the JSON schema. Nothing gets past the serializer!
              "Subject 'ProductKey' not found.; error code: 40401")
          )
          .body("error_code", equalTo(40401));
    }
  }


  @Nested
  @Tag("io.confluent.common.utils.IntegrationTest")
  @TestProfile(NoAccessFilterProfile.class)
  class HappyPathWithLocal extends HappyPath {

    @BeforeEach
    public void beforeEach() {
      setupLocalConnection(this);
    }
  }

  @Nested
  @Tag("io.confluent.common.utils.IntegrationTest")
  @TestProfile(NoAccessFilterProfile.class)
  class HappyPathWithDirect extends HappyPath {

    @BeforeEach
    public void beforeEach() {
      setupDirectConnection(this);
    }
  }

  abstract class HappyPath extends AbstractSidecarIT {

    private static RecordData schemalessData(Object data) {
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

    /**
     * Generate cartesian product of all schema formats and subject name strategies.
     * @param isKey whether the schema is for a key or value. This changes the schema name.
     * @return the list of all possible schema data
     */
    private static List<RecordData> getSchemaData(boolean isKey) {
      return Lists.cartesianProduct(
              Arrays.stream(SchemaFormat.values()).toList(),
              Arrays.stream(SubjectNameStrategyEnum.values()).toList())
          .stream()
          .map(t ->
              schemaData(
                  (SchemaFormat) t.getFirst(), isKey
              ).withSubjectNameStrategy((SubjectNameStrategyEnum) t.getLast())
          )
          .toList();
    }

    private final static List<RecordData> SCHEMALESS_RECORD_DATA_VALUES = List.of(
        schemalessData(null),
        schemalessData("hello"),
        schemalessData(123),
        schemalessData(123.45),
        schemalessData(true),
        schemalessData(List.of("hello", "world")),
        schemalessData(Collections.singletonMap("hello", "world"))
    );

    /**
     * Valid keys and values inputs used for producing and consuming data.
     * @return the sets of keys and sets of values
     */
    static ArgumentSets validKeysAndValues() {
      return ArgumentSets
          // Key
          .argumentsForFirstParameter(
              Stream.concat(
                  SCHEMALESS_RECORD_DATA_VALUES.stream(),
                  getSchemaData(true).stream()
              )
          )
          // Value
          .argumentsForNextParameter(
              Stream.concat(
                  SCHEMALESS_RECORD_DATA_VALUES.stream(),
                  getSchemaData(false).stream()
              )
          );
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
      String keySubject = null, valueSubject = null;

      // Create key schema if not null
      if (key.hasSchema()) {
        keySubject = getSubjectName(topicName, key.subjectNameStrategy(), true);
        keySchema = createSchema(
            keySubject,
            key.schemaFormat().name(),
            key.rawSchema()
        );
      }

      // Create value schema if not null
      if (value.hasSchema()) {
        valueSubject = getSubjectName(topicName, value.subjectNameStrategy(), false);
        valueSchema = createSchema(
            valueSubject,
            value.schemaFormat().name(),
            value.rawSchema()
        );
      }

      // Produce record to topic
      var resp = produceRecordThen(
          topicName,
          ProduceRequest
              .builder()
              .partitionId(null)
              .key(
                  ProduceRequestData
                      .builder()
                      .schemaVersion(Optional.ofNullable(keySchema).map(Schema::getVersion).orElse(null))
                      .data(key.data())
                      .subject(keySubject)
                      .subjectNameStrategy(
                          Optional.ofNullable(key.subjectNameStrategy).map(Enum::toString).orElse(null)
                      )
                      .build()
              )
              .value(
                  ProduceRequestData
                      .builder()
                      .schemaVersion(Optional.ofNullable(valueSchema).map(Schema::getVersion).orElse(null))
                      .data(value.data())
                      .subject(valueSubject)
                      .subjectNameStrategy(
                          Optional.ofNullable(value.subjectNameStrategy).map(Enum::toString).orElse(null)
                      )
                      .build()
              )
              .build()
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

    private void assertTopicHasRecord(RecordData key, RecordData value, String topicName) {
      var consumeResponse = consume(
          topicName,
          SimpleConsumeMultiPartitionRequestBuilder
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

      var resp = consume(topicName, SimpleConsumeMultiPartitionRequestBuilder
          .builder()
          .fromBeginning(true)
          .partitionOffsets(
              List.of(
                  new SimpleConsumeMultiPartitionRequest.PartitionOffset(0, 0),
                  new SimpleConsumeMultiPartitionRequest.PartitionOffset(1, 0),
                  new SimpleConsumeMultiPartitionRequest.PartitionOffset(2, 0),
                  new SimpleConsumeMultiPartitionRequest.PartitionOffset(3, 0),
                  new SimpleConsumeMultiPartitionRequest.PartitionOffset(4, 0)
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