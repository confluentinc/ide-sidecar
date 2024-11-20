package io.confluent.idesidecar.restapi.kafkarest;

import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.loadResource;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import io.confluent.idesidecar.restapi.integration.ITSuite;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceRequest;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceRequestData;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionRequest;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionRequestBuilder;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.cartesian.ArgumentSets;
import org.junitpioneer.jupiter.cartesian.CartesianTest;

public interface RecordsV3ApiSuite extends ITSuite {

  ObjectMapper OBJECT_MAPPER = new ObjectMapper();

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

  static String getSubjectName(
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
          OBJECT_MAPPER.readTree(PRODUCT_DATA)
      );
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static String getProductSchema(SchemaFormat format, boolean isKey) {
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

  static RecordData schemalessData(Object data) {
    return new RecordData(null, null, data);
  }

  String PRODUCT_DATA = """
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
  static List<RecordData> getSchemaData(boolean isKey) {
    return Lists
        .cartesianProduct(
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

  List<RecordData> SCHEMALESS_RECORD_DATA_VALUES = List.of(
      schemalessData(null),
      schemalessData("hello"),
      schemalessData(123),
      schemalessData(123.45),
      schemalessData(true),
      schemalessData(List.of("hello", "world")),
      schemalessData(Collections.singletonMap("hello", "world"))
  );

  static RecordData schemaData(SchemaFormat format, String jsonData) {
    var schema = getProductSchema(format);

    try {
      return new RecordData(format, schema, OBJECT_MAPPER.readTree(jsonData));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  static String getProductSchema(SchemaFormat format) {
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

  static void assertSame(JsonNode actual, Object expected) {
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
  @CartesianTest
  @CartesianTest.MethodFactory("validKeysAndValues")
  default void testProduceAndConsumeData(RecordData key, RecordData value) {
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
  default void shouldProduceRecordToPartition() {
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
