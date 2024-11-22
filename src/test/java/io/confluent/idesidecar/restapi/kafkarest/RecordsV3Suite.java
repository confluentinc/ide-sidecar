package io.confluent.idesidecar.restapi.kafkarest;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionRequest;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionRequestBuilder;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.cartesian.ArgumentSets;
import org.junitpioneer.jupiter.cartesian.CartesianTest;

public interface RecordsV3Suite extends RecordsV3BaseSuite {

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
}
