package io.confluent.idesidecar.restapi.resources;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.util.JsonFormat;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionRequest;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionResponse.PartitionConsumeData;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionResponse.PartitionConsumeRecord;
import io.confluent.idesidecar.restapi.proto.Message.MyMessage;
import io.confluent.idesidecar.restapi.testutil.NoAccessFilterProfile;
import io.confluent.idesidecar.restapi.util.ConfluentLocalKafkaWithRestProxyContainer;
import io.confluent.idesidecar.restapi.util.ConfluentLocalTestBed;
import io.confluent.idesidecar.restapi.util.ResourceIOUtil;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.*;

@QuarkusIntegrationTest
@Tag("io.confluent.common.utils.IntegrationTest")
@TestProfile(NoAccessFilterProfile.class)
public class KafkaConsumeResourceIT {
  public record KafkaClusterDetails(String id, String name, String bootstrapServers, String uri) {}

  private static ConfluentLocalTestBed confluentLocal;

  @BeforeAll
  public static void setUp() {
    confluentLocal = new ConfluentLocalTestBed();
    confluentLocal.start();
  }

  @AfterAll
  public static void tearDown() {
    if (confluentLocal != null) {
      confluentLocal.stop();
    }
  }

  private KafkaClusterDetails setupTestEnvironment(
      ConfluentLocalTestBed confluentLocal, String connectionId, String topicName, int partitions, String[][] sampleRecords) {
    confluentLocal.start();
    createLocalConnection(connectionId, connectionId);
    var localKafkaClusterDetails = getLocalKafkaClusterId();
    assertFalse(localKafkaClusterDetails.id().isEmpty());
    createLocalKafkaTopic(connectionId, localKafkaClusterDetails.id(), topicName, partitions);
    if (sampleRecords != null) {
      produceRecords(localKafkaClusterDetails.bootstrapServers(), topicName, sampleRecords);
    }
    return localKafkaClusterDetails;
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
      var connectionId = "local-connection";
      var topicName = "test_topic";
      var sampleRecords = new String[][]{
          {"key-record0", "value-record0"},
          {"key-record1", "value-record1"},
          {"key-record2", "value-record2"}
      };
      var localKafkaClusterDetails = setupTestEnvironment(confluentLocal, connectionId, topicName, 1, sampleRecords);

      var url = "gateway/v1/clusters/%s/topics/%s/partitions/-/consume".formatted(
          localKafkaClusterDetails.id(), topicName);
      var rows = given()
          .when()
          .header("Content-Type", "application/json")
          .header("x-connection-id", connectionId)
          .body("{\"from_beginning\" : true, \"max_poll_records\" : 3}")
          .post(url)
          .then()
          .statusCode(200)
          .extract()
          .body().asString();

      assertConsumerRecords(rows, sampleRecords);
      deleteLocalConnection(connectionId);
  }

  @Test
  void testConsumeFromMultiplePartitions_withMaxPollRecordsLimit() {
    // Test that the Kafka consumer respects the "max_poll_records" limit
    // and returns only the specified number of records when consuming
    // from multiple partitions in a Kafka topic. The max limit is 2000 (MAX_POLL_RECORDS_LIMIT)
      var connectionId = "local-connection3";
      var topicName = "test_topic_max_poll";
      var sampleRecords = new String[][]{
          {"key-record0", "value-record0"},
          {"key-record1", "value-record1"},
          {"key-record2", "value-record2"},
          {"key-record3", "value-record3"}
      };
      var localKafkaClusterDetails = setupTestEnvironment(confluentLocal, connectionId, topicName, 1, sampleRecords);

      var url = "gateway/v1/clusters/%s/topics/%s/partitions/-/consume".formatted(
          localKafkaClusterDetails.id(), topicName);
      var rows = given()
          .when()
          .header("Content-Type", "application/json")
          .header("x-connection-id", connectionId)
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
    deleteLocalConnection(connectionId);
  }

  @Test
  void testConsumeFromMultiplePartitions_enforcesMaxPollRecordsAcrossPartitions() {
    // Test that the Kafka consumer enforces the "max_poll_records" limit
    // across multiple partitions, ensuring that the total number of records
    // returned does not exceed the specified limit, regardless of how
    // many partitions the records are consumed from.
      var connectionId = "local-connection9";
      var topicName = "test_topic_max_poll";
      var sampleRecords = new String[][]{
          {"key-record0", "value-record0"},
          {"key-record1", "value-record1"},
          {"key-record2", "value-record2"},
          {"key-record3", "value-record3"}
      };
      var localKafkaClusterDetails = setupTestEnvironment(confluentLocal, connectionId, topicName, 2, sampleRecords);

      var url = "gateway/v1/clusters/%s/topics/%s/partitions/-/consume".formatted(
          localKafkaClusterDetails.id(), topicName);
      var rows = given()
          .when()
          .header("Content-Type", "application/json")
          .header("x-connection-id", connectionId)
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
    deleteLocalConnection(connectionId);
  }

  @Test
  void testConsumeFromMultiplePartitions_withNullMaxPollRecords() {
    // Test that the Kafka consumer, when provided with a null value for
    // "max_poll_records", retrieves all available records  from the
    // topic (with default size limits). Here, it should retrieve four records.
      var connectionId = "local-connection4";
      var topicName = "test_topic_null_max_poll";
      var sampleRecords = new String[][]{
          {"key-record0", "value-record0"},
          {"key-record1", "value-record1"},
          {"key-record2", "value-record2"},
          {"key-record3", "value-record3"}
      };
      var localKafkaClusterDetails = setupTestEnvironment(confluentLocal, connectionId, topicName, 1, sampleRecords);

      var url = "gateway/v1/clusters/%s/topics/%s/partitions/-/consume".formatted(
          localKafkaClusterDetails.id(), topicName);
      var rows = given()
          .when()
          .header("Content-Type", "application/json")
          .header("x-connection-id", connectionId)
          .body("{\"from_beginning\" : true, \"max_poll_records\" : null}")
          .post(url)
          .then()
          .statusCode(200)
          .extract()
          .body().asString();

      assertConsumerRecords(rows, sampleRecords);
    deleteLocalConnection(connectionId);
  }

  @Test
  void testConsumeFromSpecificPartitionAndOffset() throws JsonProcessingException {
    // Test that the Kafka consumer can retrieve records starting from a specific partition
    // and offset. First, consume all records from the topic and retrieve the next offset
    // for a specific partition. Then, issue a new consume request starting from a specified
    // offset (in this case, offset 2) and verify that the correct records are retrieved
    // from that offset onwards.
      // Set up Kafka Cluster
      var connectionId = "local-connection6";
      var topicName = "test_topic_next_offset_query";
      // Produce records
      var sampleRecords = new String[][]{
          {"key-record0", "value-record0"},
          {"key-record1", "value-record1"},
          {"key-record2", "value-record2"},
          {"key-record3", "value-record3"}
      };

      var localKafkaClusterDetails = setupTestEnvironment(confluentLocal, connectionId, topicName, 1, sampleRecords);

      // Initial consume from beginning
      var url = "gateway/v1/clusters/%s/topics/%s/partitions/-/consume".formatted(
          localKafkaClusterDetails.id(), topicName);
      var initialResponse = given()
          .when()
          .header("Content-Type", "application/json")
          .header("x-connection-id", connectionId)
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
      var newConsumeResponse = given()
          .when()
          .header("Content-Type", "application/json")
          .header("x-connection-id", connectionId)
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
    deleteLocalConnection(connectionId);
  }

  @Test
  void testConsumeFromMultiplePartitions_fromBeginningTrue() {
      var connectionId = "local-connection5";
      var topicName = "test_topic_from_beginning_true";
      var sampleRecords = new String[][]{
          {"key-record0", "value-record0"},
          {"key-record1", "value-record1"},
          {"key-record2", "value-record2"},
          {"key-record3", "value-record3"}
      };
      var localKafkaClusterDetails = setupTestEnvironment(confluentLocal, connectionId, topicName, 1, sampleRecords);

      var url = "gateway/v1/clusters/%s/topics/%s/partitions/-/consume".formatted(
          localKafkaClusterDetails.id(), topicName);
      var rows = given()
          .when()
          .header("Content-Type", "application/json")
          .header("x-connection-id", connectionId)
          .body("{\"from_beginning\" : true, \"max_poll_records\" : null}")
          .post(url)
          .then()
          .statusCode(200)
          .extract()
          .body().asString();

      assertConsumerRecords(rows, sampleRecords);
    deleteLocalConnection(connectionId);
  }

  @Test
  void testConsumeFromMultiplePartitions_withNullFetchMaxBytes() {
      var connectionId = "local-connection1";
      var topicName = "test_topic_null_fetch_max_bytes";
      var sampleRecords = new String[][]{
          {"key-record0", "value-record0"},
          {"key-record1", "value-record1"},
          {"key-record2", "value-record2"}
      };
      var localKafkaClusterDetails = setupTestEnvironment(confluentLocal, connectionId, topicName, 1, sampleRecords);

      var url = "gateway/v1/clusters/%s/topics/%s/partitions/-/consume".formatted(
          localKafkaClusterDetails.id(), topicName);
      var rows = given()
          .when()
          .header("Content-Type", "application/json")
          .header("x-connection-id", connectionId)
          .body("{\"from_beginning\" : true, \"max_poll_records\" : 3, \"fetch_max_bytes\" : null}")
          .post(url)
          .then()
          .statusCode(200)
          .extract()
          .body().asString();

      assertConsumerRecords(rows, sampleRecords);
    deleteLocalConnection(connectionId);
  }

  @Test
  void testConsumeFromMultiplePartitions_withFetchMaxBytesLimit() {
      var connectionId = "local-connection8";
      var topicName = "test_topic_fetch_max_bytes";
      var sampleRecords = new String[][]{
          {"key-record0", "value-record0"},
          {"key-record1", "value-record1"},
          {"key-record2", "value-record2"}
      };
      var localKafkaClusterDetails = setupTestEnvironment(confluentLocal, connectionId, topicName, 1, sampleRecords);

      var url = "gateway/v1/clusters/%s/topics/%s/partitions/-/consume".formatted(
          localKafkaClusterDetails.id(), topicName);
      var rows = given()
          .when()
          .header("Content-Type", "application/json")
          .header("x-connection-id", connectionId)
          .body("{\"from_beginning\" : true, \"max_poll_records\" : 3, \"fetch_max_bytes\" : 1024}")
          .post(url)
          .then()
          .statusCode(200)
          .extract()
          .body().asString();

      assertConsumerRecords(rows, sampleRecords);
    deleteLocalConnection(connectionId);
  }

  @Test
  void testConsumeWithMessageSizeLimitAcrossPartitions() {
    // Test that the Kafka consumer respects the "message_max_bytes" limit when consuming records
    // from multiple partitions. The test ensures that records exceeding the specified message size
    // limit are partially consumed, with their values being omitted and marked as
    // exceeding the limit. It validates that smaller messages are fully retrieved, while larger
    // ones are marked as exceeding the allowed size.
      var connectionId = "local-connection2";
      var topicName = "test_topic_fetch_max_bytes2";
      var sampleRecords = new String[][]{
          {"key0", "foo"},
          {"key1", "value-record1"},
          {"key2", "value-record2"}
      };
      var localKafkaClusterDetails = setupTestEnvironment(confluentLocal, connectionId, topicName, 1, sampleRecords);

      var url = "gateway/v1/clusters/%s/topics/%s/partitions/-/consume".formatted(
          localKafkaClusterDetails.id(), topicName);
      var rows = given()
          .when()
          .header("Content-Type", "application/json")
          .header("x-connection-id", connectionId)
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
    deleteLocalConnection(connectionId);
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
      String topic = "myProtobufTopic";
      var connectionId = "local-connection10";
      var localKafkaClusterDetails = setupTestEnvironment(confluentLocal, connectionId, topic, 1, null);
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

      KafkaProducer<String, MyMessage> producer = confluentLocal.createProtobufProducer();

      for (int i = 0; i < messages.size(); i++) {
        ProducerRecord<String, MyMessage> producerRecord = new ProducerRecord<>(topic, keys.get(i),
            messages.get(i));
        producer.send(producerRecord);
      }
      producer.close();

      var url = "gateway/v1/clusters/%s/topics/%s/partitions/-/consume".formatted(
          localKafkaClusterDetails.id(), topic);
      var rows = given()
          .when()
          .header("Content-Type", "application/json")
          .header("x-connection-id", connectionId)
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
    deleteLocalConnection(connectionId);
  }

  void deleteLocalConnection(String id) {
    given()
        .when()
        .header("Content-Type", "application/json")
        .delete("/gateway/v1/connections/" + id)
        .then()
        .statusCode(204);
  }

  void createLocalConnection(String id, String name) {
    given()
        .when()
        .header("Content-Type", "application/json")
        .body("{\"id\": \"%s\", \"name\": \"%s\", \"type\": \"LOCAL\" }".formatted(id, name))
        .post("/gateway/v1/connections")
        .then()
        .statusCode(200);
  }

  KafkaClusterDetails getLocalKafkaClusterId() {
    var queryLocalConnections = """
        { "query": "query localConnections {
            localConnections{
              id
              name
              type
              kafkaCluster {
                id
                name
                bootstrapServers
                uri
              }
              schemaRegistry {
                id
                uri
              }
            }
          }"
        }
        """;
    var graphQlResponse = given()
        .when()
        .header("Content-Type", "application/json")
        .body(queryLocalConnections)
        .post("/gateway/v1/graphql")
        .then()
        .statusCode(200)
        .extract()
        .body()
        .asString();

    var localConnections = ResourceIOUtil
        .asJson(graphQlResponse)
        .get("data")
        .get("localConnections")
        .elements();
    assertTrue(localConnections.hasNext(), "Could not find local connections");
    var kafkaCluster = localConnections.next().get("kafkaCluster");

    return new KafkaClusterDetails(
        kafkaCluster.get("id").asText(),
        kafkaCluster.get("name").asText(),
        kafkaCluster.get("bootstrapServers").asText(),
        kafkaCluster.get("uri").asText()
    );
  }

  void createLocalKafkaTopic(String connectionId,
                             String localKafkaClusterId,
                             String topicName,
                             Integer partitionsCount) {
    var response = given()
        .when()
        .header("X-Connection-ID", connectionId)
        .header("Content-Type", "application/json")
        .body(
            String.format(
                "{\"topic_name\": \"%s\", \"partitions_count\": %d}",
                topicName,
                partitionsCount
            )
        )
        .post(String.format("/kafka/v3/clusters/%s/topics", localKafkaClusterId));
    if (response.statusCode() != 201) {
      System.err.println("Failed to create Kafka topic. Status code: " + response.statusCode());
      System.err.println("Response body: " + response.getBody().asString());
    } else {
      response.then().statusCode(201); // Only assert status if successful
    }
  }

  void produceRecords(String bootstrapServers, String topicName, String[][] records) {
    // Configure the Producer
    var properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // Create the Producer
    try (var producer = new KafkaProducer<String, String>(properties)) {
      for (var recordData : records) {
        var record = new ProducerRecord<>(
            topicName,
            recordData[0],
            recordData[1]);
        producer.send(record);
      }
    }
  }
}
