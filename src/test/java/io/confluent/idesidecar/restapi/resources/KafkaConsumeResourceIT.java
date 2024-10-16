package io.confluent.idesidecar.restapi.resources;

import static io.confluent.idesidecar.restapi.util.ConfluentLocalKafkaWithRestProxyContainer.CLUSTER_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.util.JsonFormat;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionRequest;
import io.confluent.idesidecar.restapi.proto.Message.MyMessage;
import io.confluent.idesidecar.restapi.testutil.NoAccessFilterProfile;
import io.confluent.idesidecar.restapi.util.ConfluentLocalTestBed;
import io.confluent.idesidecar.restapi.util.ResourceIOUtil;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.*;

@QuarkusIntegrationTest
@Tag("io.confluent.common.utils.IntegrationTest")
@TestProfile(NoAccessFilterProfile.class)
public class KafkaConsumeResourceIT extends ConfluentLocalTestBed {
  private static void setupTestEnvironment(
      String topicName,
      int partitions,
      String[][] sampleRecords
  ) {
    createTopic(topicName, partitions, (short) 1);
    if (sampleRecords != null) {
      produceStringRecords(topicName, sampleRecords);
    }
  }

  private void assertConsumerRecords(String responseJson, String[][] expectedRecords) {
    var partitionDataList = ResourceIOUtil.asJson(responseJson).get("partition_data_list");
    assertNotNull(partitionDataList);
    assertFalse(partitionDataList.isEmpty(), "partition_data_list should not be empty");

    var records = partitionDataList.get(0).get("records");
    assertNotNull(records);

    var expectedRecordCount = expectedRecords.length;
    assertEquals(expectedRecordCount, records.size(), "Expected number of records is " + expectedRecordCount);

    for (var i = 0; i < expectedRecordCount; i++) {
      assertEquals(expectedRecords[i][0], records.get(i).get("key").asText());
      assertEquals(expectedRecords[i][1], records.get(i).get("value").asText());
    }
  }

  @Test
  void testConfluentLocalContainer() {
    var topicName = "test_topic";
    var sampleRecords = new String[][]{
        {"key-record0", "value-record0"},
        {"key-record1", "value-record1"},
        {"key-record2", "value-record2"}
    };
    setupTestEnvironment(topicName, 1, sampleRecords);

    var url = "gateway/v1/clusters/%s/topics/%s/partitions/-/consume".formatted(
        CLUSTER_ID, topicName);
    var rows = givenDefault()
        .body("{\"from_beginning\" : true, \"max_poll_records\" : 3}")
        .post(url)
        .then()
        .statusCode(200)
        .extract()
        .body().asString();

    assertConsumerRecords(rows, sampleRecords);
  }

  @Test
  void testConsumeFromMultiplePartitions_withMaxPollRecordsLimit() {
    // Test that the Kafka consumer respects the "max_poll_records" limit
    // and returns only the specified number of records when consuming
    // from multiple partitions in a Kafka topic. The max limit is 2000 (MAX_POLL_RECORDS_LIMIT)
    var topicName = "test_topic_max_poll";
    var sampleRecords = new String[][]{
        {"key-record0", "value-record0"},
        {"key-record1", "value-record1"},
        {"key-record2", "value-record2"},
        {"key-record3", "value-record3"}
    };
    setupTestEnvironment(topicName, 1, sampleRecords);

    var url = "gateway/v1/clusters/%s/topics/%s/partitions/-/consume".formatted(
        CLUSTER_ID, topicName);
    var rows = givenDefault()
        .body("{\"from_beginning\" : true, \"max_poll_records\" : 2}")
        .post(url)
        .then()
        .statusCode(200)
        .extract()
        .body().asString();

    // Only the first 2 records should be returned due to the max_poll_records limit
    var expectedRecords = new String[][]{
        {"key-record0", "value-record0"},
        {"key-record1", "value-record1"}
    };
    assertConsumerRecords(rows, expectedRecords);
  }

  @Test
  void testConsumeFromMultiplePartitions_enforcesMaxPollRecordsAcrossPartitions() {
    // Test that the Kafka consumer enforces the "max_poll_records" limit
    // across multiple partitions, ensuring that the total number of records
    // returned does not exceed the specified limit, regardless of how
    // many partitions the records are consumed from.
    var topicName = "test_topic_max_poll1";
    var sampleRecords = new String[][]{
        {"key-record0", "value-record0"},
        {"key-record1", "value-record1"},
        {"key-record2", "value-record2"},
        {"key-record3", "value-record3"}
    };
    setupTestEnvironment(topicName, 2, sampleRecords);

    var url = "gateway/v1/clusters/%s/topics/%s/partitions/-/consume".formatted(
        CLUSTER_ID, topicName);
    var rows = givenDefault()
        .body("{\"from_beginning\" : true, \"max_poll_records\" : 3}")
        .post(url)
        .then()
        .statusCode(200)
        .extract()
        .body().asString();

    var partitionDataList = ResourceIOUtil.asJson(rows).get("partition_data_list");
    assertNotNull(partitionDataList);
    assertFalse(partitionDataList.isEmpty(), "partition_data_list should not be empty");

    int totalRecordsSize = 0;
    for (int i = 0; i < partitionDataList.size(); i++) {
      var records = partitionDataList.get(i).get("records");
      if (records != null) {
        totalRecordsSize += records.size();
      }
    }
    assertEquals(3, totalRecordsSize, "Only three rows are expected as 3 rows are requested.");
  }

  @Test
  void testConsumeFromMultiplePartitions_withNullMaxPollRecords() {
    // Test that the Kafka consumer, when provided with a null value for
    // "max_poll_records", retrieves all available records  from the
    // topic (with default size limits). Here, it should retrieve four records.
    var topicName = "test_topic_null_max_poll";
    var sampleRecords = new String[][]{
        {"key-record0", "value-record0"},
        {"key-record1", "value-record1"},
        {"key-record2", "value-record2"},
        {"key-record3", "value-record3"}
    };
    setupTestEnvironment(topicName, 1, sampleRecords);

    var url = "gateway/v1/clusters/%s/topics/%s/partitions/-/consume".formatted(
        CLUSTER_ID, topicName);
    var rows = givenDefault()
        .body("{\"from_beginning\" : true, \"max_poll_records\" : null}")
        .post(url)
        .then()
        .statusCode(200)
        .extract()
        .body().asString();

    assertConsumerRecords(rows, sampleRecords);
  }

  @Test
  void testConsumeFromSpecificPartitionAndOffset() throws JsonProcessingException {
    // Test that the Kafka consumer can retrieve records starting from a specific partition
    // and offset. First, consume all records from the topic and retrieve the next offset
    // for a specific partition. Then, issue a new consume request starting from a specified
    // offset (in this case, offset 2) and verify that the correct records are retrieved
    // from that offset onwards.
    // Set up Kafka Cluster
    var topicName = "test_topic_next_offset_query";
    // Produce records
    var sampleRecords = new String[][]{
        {"key-record0", "value-record0"},
        {"key-record1", "value-record1"},
        {"key-record2", "value-record2"},
        {"key-record3", "value-record3"}
    };

    setupTestEnvironment(topicName, 1, sampleRecords);

    // Initial consume from beginning
    var url = "gateway/v1/clusters/%s/topics/%s/partitions/-/consume".formatted(
        CLUSTER_ID, topicName);
    var initialResponse = givenDefault()
        .body("{\"from_beginning\" : true, \"max_poll_records\" : 4}")
        .post(url)
        .then()
        .statusCode(200)
        .extract()
        .body().asString();
    assertConsumerRecords(initialResponse, sampleRecords);

    // Parse the response to get next_offset for partition 0
    var partitionDataList = ResourceIOUtil.asJson(initialResponse).get("partition_data_list");
    assertNotNull(partitionDataList);
    var partitionData = partitionDataList.get(0);
    var nextOffset = partitionData.get("next_offset").asLong();
    assertEquals(4, nextOffset);

    // Now consume again with partition 0 and offset = 2
    var requestBody = new SimpleConsumeMultiPartitionRequest(
        List.of(new SimpleConsumeMultiPartitionRequest.PartitionOffset(0, 2)), // query partition 0 from offset 2
        null,  // max_poll_records
        null, // timestamp
        null, // fetch_max_bytes
        null, // message_max_bytes
        null // from_beginning = false
    );
    ObjectMapper objectMapper = new ObjectMapper();
    var requestBodyJson = objectMapper.writeValueAsString(requestBody);

    // Send the new request
    var newConsumeResponse = givenDefault()
        .body(requestBodyJson)
        .post(url)
        .then()
        .statusCode(200)
        .extract()
        .body().asString();

    // Validate that records are consumed from offset 2 onward
    var newPartitionDataList = ResourceIOUtil.asJson(newConsumeResponse).get("partition_data_list");
    assertNotNull(newPartitionDataList);
    var newRecords = newPartitionDataList.get(0).get("records");
    assertNotNull(newRecords);
    assertEquals(2, newRecords.size(), "Expected number of records is 2");

    for (var i = 0; i < 2; i++) {
      var actualRecord = newRecords.get(i);
      assertEquals(
          "key-record%d".formatted(i + 2),
          actualRecord.get("key").asText());
      assertEquals(
          "value-record%d".formatted(i + 2),
          actualRecord.get("value").asText());
      assertEquals(0, actualRecord.get("partition_id").asInt());
    }
  }

  @Test
  void testConsumeFromMultiplePartitions_fromBeginningTrue() {
    var topicName = "test_topic_from_beginning_true";
    var sampleRecords = new String[][]{
        {"key-record0", "value-record0"},
        {"key-record1", "value-record1"},
        {"key-record2", "value-record2"},
        {"key-record3", "value-record3"}
    };
    setupTestEnvironment(topicName, 1, sampleRecords);

    var url = "gateway/v1/clusters/%s/topics/%s/partitions/-/consume".formatted(
        CLUSTER_ID, topicName);
    var rows = givenDefault()
        .body("{\"from_beginning\" : true, \"max_poll_records\" : null}")
        .post(url)
        .then()
        .statusCode(200)
        .extract()
        .body().asString();

    assertConsumerRecords(rows, sampleRecords);
  }

  @Test
  void testConsumeFromMultiplePartitions_withNullFetchMaxBytes() {
    var topicName = "test_topic_null_fetch_max_bytes";
    var sampleRecords = new String[][]{
        {"key-record0", "value-record0"},
        {"key-record1", "value-record1"},
        {"key-record2", "value-record2"}
    };
    setupTestEnvironment(topicName, 1, sampleRecords);

    var url = "gateway/v1/clusters/%s/topics/%s/partitions/-/consume".formatted(
        CLUSTER_ID, topicName);
    var rows = givenDefault()
        .body("{\"from_beginning\" : true, \"max_poll_records\" : 3, \"fetch_max_bytes\" : null}")
        .post(url)
        .then()
        .statusCode(200)
        .extract()
        .body().asString();

    assertConsumerRecords(rows, sampleRecords);
  }

  @Test
  void testConsumeFromMultiplePartitions_withFetchMaxBytesLimit() {
    var topicName = "test_topic_fetch_max_bytes";
    var sampleRecords = new String[][]{
        {"key-record0", "value-record0"},
        {"key-record1", "value-record1"},
        {"key-record2", "value-record2"}
    };
    setupTestEnvironment(topicName, 1, sampleRecords);

    var url = "gateway/v1/clusters/%s/topics/%s/partitions/-/consume".formatted(
        CLUSTER_ID, topicName);
    var rows = givenDefault()
        .body("{\"from_beginning\" : true, \"max_poll_records\" : 3, \"fetch_max_bytes\" : 1024}")
        .post(url)
        .then()
        .statusCode(200)
        .extract()
        .body().asString();

    assertConsumerRecords(rows, sampleRecords);
  }

  @Test
  void testConsumeWithMessageSizeLimitAcrossPartitions() {
    // Test that the Kafka consumer respects the "message_max_bytes" limit when consuming records
    // from multiple partitions. The test ensures that records exceeding the specified message size
    // limit are partially consumed, with their values being omitted and marked as
    // exceeding the limit. It validates that smaller messages are fully retrieved, while larger
    // ones are marked as exceeding the allowed size.
    var topicName = "test_topic_fetch_max_bytes2";
    var sampleRecords = new String[][]{
        {"key0", "foo"},
        {"key1", "value-record1"},
        {"key2", "value-record2"}
    };
    setupTestEnvironment(topicName, 1, sampleRecords);

    var url = "gateway/v1/clusters/%s/topics/%s/partitions/-/consume".formatted(
        CLUSTER_ID, topicName);
    var rows = givenDefault()
        .body("{\"from_beginning\" : true, \"message_max_bytes\" : 8}")
        .post(url)
        .then()
        .statusCode(200)
        .extract()
        .body().asString();

    // Validate that records are consumed from offset 2 onward
    var newPartitionDataList = ResourceIOUtil.asJson(rows).get("partition_data_list");
    assertNotNull(newPartitionDataList);
    var newRecords = newPartitionDataList.get(0).get("records");
    assertNotNull(newRecords);
    assertEquals(3, newRecords.size(), "Expected number of records is 2");

    assertEquals("key%d".formatted(0), newRecords.get(0).get("key").asText());
    assertEquals("foo", newRecords.get(0).get("value").asText());
    assertFalse(newRecords.get(0).get("exceeded_fields").get("value").asBoolean());

    assertEquals("key%d".formatted(1), newRecords.get(1).get("key").asText());
    assertTrue(newRecords.get(1).get("value") == null);
    assertTrue(newRecords.get(1).get("exceeded_fields").get("value").asBoolean());
    assertEquals("key%d".formatted(2), newRecords.get(2).get("key").asText());
    assertTrue(newRecords.get(2).get("value") == null);
    assertTrue(newRecords.get(2).get("exceeded_fields").get("value").asBoolean());
  }

  /**
   * This test validates the consumption of protobuf messages from kafka topic and decoded by
   * schema-registry.
   * The test performs the following steps:
   * 1. Produces three Protobuf messages to the specified Kafka topic with unique keys. These
   *    messages are encoded into base64 strings with a MAGIC byte prefix containing schema-id.
   * 2. Consumes all messages from the Kafka topic starting from the beginning using message-viewer
   *    API.
   * 3. Verifies that the expected number of messages (3) is consumed.
   * 4. The messages should be decoded by the message-viewer and return as JSON records.
   * 4. Converts the consumed messages from JSON back into Protobuf format.
   * 5. Compares the consumed Protobuf messages with the original messages to ensure correctness.
   * 6. Checks that no "exceeded fields" flag is set during consumption.
   */
  @Test
  public void testShouldDecodeProfobufMessagesUsingSRInMessageViewer() throws Exception {
    var topic = "myProtobufTopic";
    setupTestEnvironment(topic, 1, null);

    // Create the schema version for the Protobuf message
    createSchema(
        "myProtobufTopic-value",
        "PROTOBUF",
        "syntax = \"proto3\"; package io.confluent.idesidecar.restapi; message MyMessage { string name = 1; int32 age = 2; bool is_active = 3; }"
    );

    MyMessage message1 = MyMessage.newBuilder()
        .setName("Some One")
        .setAge(30)
        .setIsActive(true)
        .build();

    MyMessage message2 = MyMessage.newBuilder()
        .setName("John Doe")
        .setAge(25)
        .setIsActive(false)
        .build();

    MyMessage message3 = MyMessage.newBuilder()
        .setName("Jane Smith")
        .setAge(40)
        .setIsActive(true)
        .build();

    List<MyMessage> messages = List.of(message1, message2, message3);
    List<String> keys = List.of("key0", "key1", "key2");

    for (int i = 0; i < messages.size(); i++) {
      produceRecord(topic, keys.get(i), null, Map.of(
          "name", messages.get(i).getName(),
          "age", messages.get(i).getAge(),
          "is_active", messages.get(i).getIsActive()
      ),1);
    }

    var url = "gateway/v1/clusters/%s/topics/%s/partitions/-/consume".formatted(
        CLUSTER_ID, topic);
    var rows = givenDefault()
        .body("{\"from_beginning\" : true}")
        .post(url)
        .then()
        .statusCode(200)
        .extract()
        .body().asString();

    var newPartitionDataList = ResourceIOUtil.asJson(rows).get("partition_data_list");
    assertNotNull(newPartitionDataList);
    var newRecords = newPartitionDataList.get(0).get("records");
    assertNotNull(newRecords);
    assertEquals(3, newRecords.size(), "Expected number of records is 3");
    // Use JsonFormat to parse the JSON into a Protobuf object
    for (int i = 0; i < 3; i++) {
      assertEquals(keys.get(i), newRecords.get(i).get("key").asText(), "Mismatched key");

      // Parse JSON string into MyMessage Protobuf object
      String jsonValue = newRecords.get(i).get("value").toString();
      MyMessage.Builder messageBuilder = MyMessage.newBuilder();
      JsonFormat.parser().merge(jsonValue, messageBuilder); // Parse JSON to MyMessage
      MyMessage parsedMessage = messageBuilder.build();

      // Compare the parsed message with the original message
      assertEquals(messages.get(i), parsedMessage, "Mismatched Protobuf message");

      assertFalse(newRecords.get(i).get("exceeded_fields").get("value").asBoolean(), "Exceeded fields should be false");
    }
  }
}
