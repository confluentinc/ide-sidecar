package io.confluent.idesidecar.restapi.kafkarest;

import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.loadResource;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import io.confluent.idesidecar.restapi.integration.ITSuite;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceRequest;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceRequestData;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceRequestHeader;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionRequestBuilder;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionResponse;
import io.confluent.idesidecar.restapi.models.DataFormat;
import io.confluent.idesidecar.restapi.models.KeyOrValueMetadata;
import io.confluent.idesidecar.restapi.util.ByteArrayJsonUtil;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.Set;

/**
 * Base suite interface containing common methods for testing the
 * Records V3 API.
 */
public interface RecordsV3BaseSuite extends ITSuite {

  ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  record RecordData(
      SchemaFormat schemaFormat,
      SubjectNameStrategyEnum subjectNameStrategy,
      String rawSchema,
      String subject,
      Integer schemaId,
      Object data
  ) {

    public RecordData(SchemaFormat schemaFormat, String rawSchema, Object data) {
      this(schemaFormat, null, rawSchema, null, null, data);
    }

    RecordData withSubjectNameStrategy(SubjectNameStrategyEnum subjectNameStrategy) {
      return new RecordData(schemaFormat, subjectNameStrategy, rawSchema, null, null, data);
    }

    @Override
    public String toString() {
      return "(data = %s, schemaFormat = %s, subjectNameStrategy = %s)"
          .formatted(data, schemaFormat, subjectNameStrategy);
    }

    public boolean hasSchema() {
      return schemaFormat != null && rawSchema != null && subjectNameStrategy != null;
    }

    public RecordData withSubject(String subject) {
      return new RecordData(schemaFormat, subjectNameStrategy, rawSchema, subject, schemaId, data);
    }

    public RecordData withSchemaId(Integer schemaId) {
      return new RecordData(schemaFormat, subjectNameStrategy, rawSchema, subject, schemaId, data);
    }
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

  static RecordData schemalessData(Object data) {
    return new RecordData(null, null, data);
  }

  List<RecordData> SCHEMALESS_RECORD_DATA_VALUES = List.of(
      schemalessData(null),
      schemalessData("hello"),
      schemalessData(123),
      schemalessData(123.45),
      schemalessData(true),
      schemalessData(List.of("hello", "world")),
      schemalessData(Collections.singletonMap("hello", "world")),
      schemalessData(randomBytes())
  );

  private static byte[] randomBytes() {
    byte[] bytes = new byte[100];
    new SecureRandom().nextBytes(bytes);
    return bytes;
  }

  default String getSubjectName(
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

  default void produceAndConsume(RecordData key, RecordData value) {
    var topicName = randomTopicName();
    // Create topic with a single partition
    createTopic(topicName);

    Schema keySchema, valueSchema;

    // Create key schema if not null
    if (key.hasSchema()) {
      key = key.withSubject(getSubjectName(topicName, key.subjectNameStrategy(), true));
      keySchema = createSchema(
          key.subject(),
          key.schemaFormat().name(),
          key.rawSchema()
      );
      key = key.withSchemaId(keySchema.getId());
    } else {
      keySchema = null;
    }

    // Create value schema if not null
    if (value.hasSchema()) {
      value = value.withSubject(getSubjectName(topicName, value.subjectNameStrategy(), false));
      valueSchema = createSchema(
          value.subject(),
          value.schemaFormat().name(),
          value.rawSchema()
      );
      value = value.withSchemaId(valueSchema.getId());
    } else {
      valueSchema = null;
    }

    // Produce record to topic
    var produceRequest = ProduceRequest
        .builder()
        .partitionId(null)
        .key(
            ProduceRequestData
                .builder()
                .schemaVersion(Optional.ofNullable(keySchema).map(Schema::getVersion).orElse(null))
                .data(key.data())
                .subject(key.subject())
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
                .subject(value.subject())
                .subjectNameStrategy(
                    Optional.ofNullable(value.subjectNameStrategy).map(Enum::toString).orElse(null)
                )
                .build()
        )
        .build();

    // Send produce request
    var resp = produceRecordThen(topicName, produceRequest);

    if (key.data() != null || value.data() != null) {
      // Retry if 404
      if (resp.extract().statusCode() == 404) {
        // Keep trying until 200
        await()
            .atMost(Duration.ofSeconds(10))
            .pollInterval(Duration.ofMillis(500))
            .untilAsserted(() -> {
              var retry = produceRecordThen(topicName, produceRequest);
              assertEquals(200, retry.extract().statusCode());
            });
      }

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
    assertTopicHasRecord(key, value, topicName, Collections.emptySet());
  }

  default void assertTopicHasRecord(
      RecordData key, RecordData value, String topicName, Set<ProduceRequestHeader> headers
  ) {
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

    assertEquals(1, records.size());

    assertKeyOrValue(
        key,
        records.getFirst().key(),
        records.getFirst().metadata().keyMetadata());
    assertKeyOrValue(
        value,
        records.getFirst().value(),
        records.getFirst().metadata().valueMetadata()
    );

    // Assert headers are the same
    assertEquals(headers, convertResponseHeaders(records.getFirst().headers()));
  }

  private void assertKeyOrValue(
      RecordData expectedKeyOrValue,
      JsonNode actualKeyOrValue,
      KeyOrValueMetadata metadata
  ) {
    if (expectedKeyOrValue.hasSchema()) {
      assertEquals(expectedKeyOrValue.schemaId(), metadata.schemaId());
      assertSame(expectedKeyOrValue.data(), actualKeyOrValue);
    } else if (expectedKeyOrValue.data() == null) {
      assertNull(metadata);
    } else if (expectedKeyOrValue.data() instanceof byte[]) {
      assertNull(metadata.schemaId());
      assertEquals(DataFormat.RAW_BYTES, metadata.dataFormat());

      assertArrayEquals(
          (byte[]) expectedKeyOrValue.data(),
          ByteArrayJsonUtil.asBytes(actualKeyOrValue)
      );
    } else {
      assertNull(metadata.schemaId());
      assertEquals(DataFormat.JSON, metadata.dataFormat());
      assertSame(expectedKeyOrValue.data(), actualKeyOrValue);
    }
  }

  private static Set<ProduceRequestHeader> convertResponseHeaders(
      List<SimpleConsumeMultiPartitionResponse.PartitionConsumeRecordHeader> headers
  ) {
    return headers
        .stream()
        .map(h ->
            ProduceRequestHeader
                .builder()
                .name(h.key())
                .value(h.value().getBytes(StandardCharsets.UTF_8))
                .build()
        )
        .collect(Collectors.toSet());
  }

  default void assertSame(Object expected, JsonNode actual) {
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
