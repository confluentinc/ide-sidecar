package io.confluent.idesidecar.restapi.kafkarest;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionRequest;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionRequestBuilder;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.cartesian.ArgumentSets;
import org.junitpioneer.jupiter.cartesian.CartesianTest;

public interface RecordsV3Suite extends RecordsV3BaseSuite {

  /**
   * Valid keys and values inputs used for producing and consuming data.
   *
   * @return the sets of keys and sets of values
   */
  static ArgumentSets validKeysAndValues() {
    return ArgumentSets
        // Key
        .argumentsForFirstParameter(
            Stream.concat(
                SCHEMALESS_RECORD_DATA_VALUES.stream(),
                RecordsV3BaseSuite.getSchemaData(true).stream()
            )
        )
        // Value
        .argumentsForNextParameter(
            Stream.concat(
                SCHEMALESS_RECORD_DATA_VALUES.stream(),
                RecordsV3BaseSuite.getSchemaData(false).stream()
            )
        );
  }

  @CartesianTest
  @CartesianTest.MethodFactory("validKeysAndValues")
  default void testProduceAndConsumeData(RecordData key, RecordData value) {
    produceAndConsume(key, value);
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

  @Test
  default void shouldDeserializeProtobufSchemaHavingAnyField() {
    var topicName = randomTopicName();
    createTopic(topicName);

    var rawSchema = """
        syntax = "proto3";
        package com.example;

        import "google/protobuf/any.proto";
        message Product {
          int32 id = 1;
          google.protobuf.Any payload = 2;
        }
        message Other {
          string foo = 1;
        }
        """;
    var valueSchema = createSchema(
        getSubjectName(topicName, SubjectNameStrategyEnum.TOPIC_NAME, false),
        SchemaFormat.PROTOBUF.name(),
        rawSchema
    );

    var valueData = Map.of(
        "id", 1,
        "payload", Map.of(
            "@type", "type.googleapis.com/com.example.Other",
            "foo", "hello"
        ));

    produceRecord(topicName, "foo", null, valueData, valueSchema.getVersion());
    assertTopicHasRecord(
        RecordsV3BaseSuite.schemalessData("foo"),
        new RecordData(
            SchemaFormat.PROTOBUF, SubjectNameStrategyEnum.TOPIC_NAME, rawSchema, valueData
        ),
        topicName
    );
  }

  @Test
  default void shouldDeserializeProtobufSchemasHavingAnyField_WithSchemaReferences() {
    var topicName = randomTopicName();
    createTopic(topicName);

    var otherSchema = createSchema(
        "com.example.Other",
        SchemaFormat.PROTOBUF.name(),
        """
            syntax = "proto3";
            package com.example;
            message Other {
              string foo = 1;
            }
            """
    );

    var rawSchema = """
        syntax = "proto3";
        package com.example;

        import "com.example.Other";
        import "google/protobuf/any.proto";
        message Product {
          int32 id = 1;
          google.protobuf.Any payload = 2;
        }
        """;

    var valueSchema = createSchema(
        getSubjectName(topicName, SubjectNameStrategyEnum.TOPIC_NAME, false),
        SchemaFormat.PROTOBUF.name(),
        rawSchema,
        List.of(
            new SchemaReference(
                // Note: The name of the schema reference MUST match the import
                // statement in the schema
                "com.example.Other",
                otherSchema.getSubject(),
                otherSchema.getVersion()
            )
        )
    );

    var valueData = Map.of(
        "id", 1,
        "payload", Map.of(
            "@type", "type.googleapis.com/com.example.Other",
            "foo", "hello"
        ));

    produceRecord(topicName, "foo", null, valueData, valueSchema.getVersion());
    assertTopicHasRecord(
        RecordsV3BaseSuite.schemalessData("foo"),
        new RecordData(
            SchemaFormat.PROTOBUF, SubjectNameStrategyEnum.TOPIC_NAME, rawSchema, valueData
        ),
        topicName
    );
  }
}
