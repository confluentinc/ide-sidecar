package io.confluent.idesidecar.restapi.kafkarest;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionRequest;
import io.confluent.idesidecar.restapi.testutil.NoAccessFilterProfile;
import io.confluent.idesidecar.restapi.util.ConfluentLocalTestBed;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.cartesian.ArgumentSets;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.loadResource;
import static org.junit.jupiter.api.Assertions.*;

@QuarkusIntegrationTest
@Tag("io.confluent.common.utils.IntegrationTest")
@TestProfile(NoAccessFilterProfile.class)
class RecordsV3ApiImplIT extends ConfluentLocalTestBed {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  @Test
  void shouldReturnNotFoundWhenClusterDoesNotExist() {
    // Given
    // When
    // Then
  }

  @Test
  void shouldReturnNotFoundWhenTopicDoesNotExist() {
    // Given
    // When
    // Then
  }

  @Test
  void shouldReturnNotFoundWhenPartitionDoesNotExist() {

  }

  @Test
  void shouldReturnNotFoundWhenSubjectDoesNotExist() {
    // Given
    // When
    // Then
  }

  @Test
  void shouldReturnNotFoundWhenSchemaVersionDoesNotExist() {
    // Given
    // When
    // Then
  }

  @Test
  void shouldProduceRecordWithProtobufSchemaForValue() {
    // Given
    // When
    // Then
  }

  @Test
  void shouldProduceRecordWithProtobufSchemaForKeyAndValue() {
    // Given
    // When
    // Then
  }

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

  private static RecordData schemaData(SchemaManager.SchemaFormat format, String jsonData) {
    var schema = switch (format) {
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

    try {
      return new RecordData(format, schema, OBJECT_MAPPER.readTree(jsonData));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

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

  private static ArgumentSets testProduceAndConsumeData() {
    return ArgumentSets
        .argumentsForFirstParameter(RECORD_DATA_VALUES)
        .argumentsForNextParameter(RECORD_DATA_VALUES);
  }

  @CartesianTest
  @CartesianTest.MethodFactory("testProduceAndConsumeData")
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
    produceRecord(
        topicName,
        key.data(),
        Optional.ofNullable(keySchema).map(Schema::getVersion).orElse(null),
        value.data(),
        Optional.ofNullable(valueSchema).map(Schema::getVersion).orElse(null)
    );

    // Consume topic
    var consumeResponse = consume(
        topicName,
        new SimpleConsumeMultiPartitionRequest()
            .withFromBeginning(true)
            .withMaxPollRecords(1)
    );

    var records = consumeResponse
        .partitionDataList()
        // Topic has only one partition
        .getFirst()
        .records();

    assertEquals(records.size(), 1);

    assertSame(records.getFirst().key(), key.data());
    assertSame(records.getFirst().value(), value.data());
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
}