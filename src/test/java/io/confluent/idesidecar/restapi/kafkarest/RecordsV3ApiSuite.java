package io.confluent.idesidecar.restapi.kafkarest;

import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.loadResource;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.idesidecar.restapi.integration.ITSuite;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionRequest;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionRequestBuilder;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.cartesian.ArgumentSets;
import org.junitpioneer.jupiter.cartesian.CartesianTest;

public interface RecordsV3ApiSuite extends ITSuite {

  ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  record RecordData(
      SchemaFormat schemaFormat,
      String rawSchema,
      Object data
  ) {

    @Override
    public String toString() {
      return "(data = %s, schemaFormat = %s, schema = %s)"
          .formatted(data, schemaFormat, rawSchema);
    }
  }

  static RecordData jsonData(Object data) {
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

  List<RecordData> RECORD_DATA_VALUES = List.of(
      jsonData(null),
      jsonData("hello"),
      jsonData(123),
      jsonData(123.45),
      jsonData(true),
      jsonData(List.of("hello", "world")),
      jsonData(Collections.singletonMap("hello", "world")),
      schemaData(SchemaFormat.JSON, PRODUCT_DATA),
      schemaData(SchemaFormat.PROTOBUF, PRODUCT_DATA),
      schemaData(SchemaFormat.AVRO, PRODUCT_DATA)
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
        .argumentsForFirstParameter(RECORD_DATA_VALUES)
        .argumentsForNextParameter(RECORD_DATA_VALUES);
  }

  @CartesianTest
  @CartesianTest.MethodFactory("validKeysAndValues")
  default void testProduceAndConsumeData(RecordData key, RecordData value) {
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

  default void assertTopicHasRecord(RecordData key, RecordData value, String topicName) {
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
