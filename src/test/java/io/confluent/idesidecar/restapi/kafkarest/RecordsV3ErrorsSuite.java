package io.confluent.idesidecar.restapi.kafkarest;

import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.loadResource;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.matchesRegex;

import io.confluent.idesidecar.restapi.kafkarest.model.ProduceRequest;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceRequestData;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junitpioneer.jupiter.cartesian.ArgumentSets;
import org.junitpioneer.jupiter.cartesian.CartesianTest;

public interface RecordsV3ErrorsSuite extends RecordsV3BaseSuite {

  @Test
  default void shouldThrowNotFoundWhenClusterDoesNotExist() {
    givenConnectionId()
        .header("Content-Type", "application/json")
        .body(createProduceRequest(
            null,
            "key",
            null,
            "value",
            null,
            Set.of()))
        .post("/kafka/v3/clusters/non-existent-cluster/topics/foo/records")
        .then()
        .statusCode(404)
        .body("message", equalTo("Kafka cluster 'non-existent-cluster' not found."));
  }

  @Test
  default void shouldThrowNotFoundWhenTopicDoesNotExist() {
    produceRecordThen(null, "non-existent-topic", null, null)
        .statusCode(404)
        .body("message", equalTo("This server does not host this topic-partition."));
  }

  @Test
  default void shouldThrowNotFoundWhenPartitionDoesNotExist() {
    var topic = randomTopicName();
    createTopic(topic, 3, 1);
    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() ->
            produceRecordThen(10, topic, "key", "value")
                .statusCode(404)
                .body(
                    "message", equalTo("This server does not host this topic-partition."))
        );
  }

  /**
   * Inputs for test cases for when the key and value schema versions are not found.
   *
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
  default void shouldThrowNotFoundWhenSubjectDoesNotExist(
      Integer keySchemaVersion, Integer valueSchemaVersion
  ) {
    var topic = randomTopicName();
    createTopic(topic);
    // Schema Registry should fail to find the subject before it even gets to the schema
    // version check
    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() ->
            produceRecordThen(
                null, topic, "key", keySchemaVersion, "value", valueSchemaVersion, Set.of())
                .statusCode(404)
                .body("message",
                    matchesRegex("^Subject '%s-(key|value)' not found\\.; error code: 40401$"
                        .formatted(topic))
                )
        );
  }

  @ParameterizedTest
  @MethodSource("keyAndValueSchemaVersions")
  default void shouldThrowNotFoundWhenSchemaVersionDoesNotExist(
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
    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() ->
            produceRecordThen(
                null, topic, "key", keySchemaVersion, "value", valueSchemaVersion, Set.of())
                .statusCode(404)
                .body("message", matchesRegex("^Version \\d+ not found.; error code: 40402$"))
        );
  }

  @Test
  default void shouldThrowBadRequestIfKeyAndValueDataAreNull() {
    var topic = randomTopicName();
    createTopic(topic);
    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() ->
            produceRecordThen(null, topic, null, null)
                .statusCode(400)
                .body("message", equalTo("Key and value data cannot both be null"))
        );
  }

  /**
   * Inputs for test cases for when the schema is not compatible with the data.
   *
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
  default void shouldThrowBadRequestIfSchemaIsNotCompatibleWithData(
      SchemaFormat keyFormat, Object badData
  ) {
    var topic = randomTopicName();
    createTopic(topic);
    var keySchema = createSchema(
        "%s-key".formatted(topic),
        keyFormat.schemaProvider().schemaType(),
        RecordsV3BaseSuite.getProductSchema(keyFormat, true)
    );

    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() ->
            produceRecordThen(
                null, topic, badData, keySchema.getVersion(), null, null, Set.of())
                .statusCode(400)
                .body("message", containsString("Failed to parse data"))
        );

    var valueSchema = createSchema(
        "%s-value".formatted(topic),
        keyFormat.schemaProvider().schemaType(),
        RecordsV3BaseSuite.getProductSchema(keyFormat, false)
    );

    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() ->
            produceRecordThen(
                null, topic, null, null, badData, valueSchema.getVersion(), Set.of())
                .statusCode(400)
                .body("message", containsString("Failed to parse data"))
        );
  }

  /**
   * Inputs for test cases for when the (Protobuf) schema is not compatible with the data.
   *
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
  default void shouldThrowBadRequestForInvalidProtobufData(Object badData) {
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
  default void shouldThrowNotImplementedForUnsupportedSchemaDetails(ProduceRequestData data) {
    var topic = randomTopicName();
    createTopic(topic);

    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() ->
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
                )));
  }

  @Test
  // TODO: Figure out why this test fails for cp-demo
  @DisabledIfSystemProperty(named = "running-in-cp-test-environment", matches = "true")
  default void shouldHandleWrongTopicNameStrategy() {
    var topic = randomTopicName();
    createTopic(topic);

    // Create a schema called "foo-key" with a JSON schema (uses TopicNameStrategy)
    var keySchema = createSchema(
        "foo-key",
        "JSON",
        loadResource("schemas/product-key.schema.json")
    );

    // Try to produce a record with the wrong subject name strategy
    await()
        .atMost(Duration.ofSeconds(10))
        .untilAsserted(() ->
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
                .body("error_code", equalTo(40401))
        );
  }
}
