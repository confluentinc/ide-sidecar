package io.confluent.idesidecar.restapi.kafkarest;

import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.loadResource;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.idesidecar.restapi.integration.ITSuite;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceRequest;
import io.confluent.idesidecar.restapi.kafkarest.model.ProduceRequestData;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionRequestBuilder;
import java.util.Map;
import org.junit.jupiter.api.Test;

public interface RecordsV3DryRunSuite extends ITSuite {

  @Test
  default void shouldDryRunProduceRecord() {
    var topicName = randomTopicName();
    createTopic(topicName);

    var keySchema = createSchema(
        "%s-key".formatted(topicName),
        "JSON",
        loadResource("schemas/product-key.schema.json")
    );

    var valueSchema = createSchema(
        "%s-value".formatted(topicName),
        "PROTOBUF",
        loadResource("schemas/product-value.proto")
    );

    produceRecordThen(
        topicName,
        ProduceRequest
            .builder()
            .partitionId(null)
            .key(
                ProduceRequestData
                    .builder()
                    .schemaVersion(keySchema.getVersion())
                    .data(Map.of(
                        "id", 123,
                        "name", "test",
                        "price", 123.45
                    ))
                    .build()
            )
            .value(
                ProduceRequestData
                    .builder()
                    .schemaVersion(valueSchema.getVersion())
                    .data(Map.of())
                    .build()
            )
            .build(),
        true // Dry run
    )
        // Dry run should not produce any records
        .statusCode(200)
        .body("cluster_id", notNullValue())
        .body("topic_name", equalTo(topicName));

    assertTopicHasNoRecords(topicName);
  }

  @Test
  default void shouldDryRunWithInvalidData() {
    var topicName = randomTopicName();
    createTopic(topicName);

    var keySchema = createSchema(
        "%s-key".formatted(topicName),
        "JSON",
        loadResource("schemas/product-key.schema.json")
    );

    var valueSchema = createSchema(
        "%s-value".formatted(topicName),
        "PROTOBUF",
        loadResource("schemas/product-value.proto")
    );

    produceRecordThen(
        topicName,
        ProduceRequest
            .builder()
            .partitionId(null)
            .key(
                ProduceRequestData
                    .builder()
                    .schemaVersion(keySchema.getVersion())
                    .data(Map.of(
                        "id", "invalid",
                        "name", "test",
                        "price", 123.45
                    ))
                    .build()
            )
            .value(
                ProduceRequestData
                    .builder()
                    .schemaVersion(valueSchema.getVersion())
                    .data(Map.of())
                    .build()
            )
            .build(),
        true // Dry run
    )
        // Dry run should not produce any records
        .statusCode(400)
        .body(
            "message",
            equalTo("Failed to parse data: #/id: expected type: Integer, found: String")
        );

    assertTopicHasNoRecords(topicName);
  }

  private void assertTopicHasNoRecords(String topicName) {
    var resp = consume(
        topicName,
        SimpleConsumeMultiPartitionRequestBuilder
            .builder()
            .fromBeginning(true)
            .maxPollRecords(1)
            .build()
    );

    for (var partition : resp.partitionDataList()) {
      var records = partition.records();
      assertTrue(records.isEmpty());
    }
  }

}
