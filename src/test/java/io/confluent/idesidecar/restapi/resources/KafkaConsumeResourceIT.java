package io.confluent.idesidecar.restapi.resources;

import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.idesidecar.restapi.testutil.NoAccessFilterProfile;
import io.confluent.idesidecar.restapi.util.ConfluentLocalContainer;
import io.confluent.idesidecar.restapi.util.ResourceIOUtil;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@QuarkusIntegrationTest
@Tag("io.confluent.common.utils.IntegrationTest")
@TestProfile(NoAccessFilterProfile.class)
public class KafkaConsumeResourceIT {
  public record KafkaClusterDetails(String id, String name, String bootstrapServers, String uri) {}

  @Test
  void testConfluentLocalContainer() {
    try (var confluentLocal = new ConfluentLocalContainer()) {
      confluentLocal.start();

      // Create local connection and get cluster ID
      var connectionId = "local-connection";
      createLocalConnection(connectionId, connectionId);
      var localKafkaClusterDetails = getLocalKafkaClusterId();
      assertFalse(localKafkaClusterDetails.id().isEmpty());

      // Create topic on local cluster
      var topicName = "test_topic";
      createLocalKafkaTopic(connectionId, localKafkaClusterDetails.id(), topicName, 1);

      var sampleRecords = new String[][]{
          {"key-record0", "value-record0"},
          {"key-record1", "value-record1"},
          {"key-record2", "value-record2"}
      };
      produceRecords(localKafkaClusterDetails.bootstrapServers(), topicName, sampleRecords);

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

      // Convert the response to JSON
      var partitionDataList = ResourceIOUtil
          .asJson(rows)
          .get("partition_data_list");
      assertNotNull(partitionDataList);
      assertFalse(partitionDataList.isEmpty(), "partition_data_list should not be empty");

      var records = partitionDataList.get(0).get("records");
      assertNotNull(records);

      // Assertions on the consumed records
      assertEquals(3, records.size(), "Expected number of records is 3");

      for (var i = 0; i < 3; i++) {
        assertEquals("key-record%d".formatted(i), records.get(i).get("key").asText());
        assertEquals("value-record%d".formatted(i), records.get(i).get("value").asText());
      }
    }
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
    given()
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
        .post(String.format("/kafka/v3/clusters/%s/topics", localKafkaClusterId))
        .then()
        .statusCode(201);
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
