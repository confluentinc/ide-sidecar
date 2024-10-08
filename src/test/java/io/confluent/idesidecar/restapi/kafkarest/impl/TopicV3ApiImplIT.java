package io.confluent.idesidecar.restapi.kafkarest.impl;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;

import io.confluent.idesidecar.restapi.testutil.NoAccessFilterProfile;
import io.confluent.idesidecar.restapi.util.ConfluentLocalKafkaWithRestProxyContainer;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.specification.RequestSpecification;
import java.util.Map;
import org.junit.jupiter.api.Test;

@QuarkusIntegrationTest
@TestProfile(NoAccessFilterProfile.class)
class TopicV3ApiImplIT extends KafkaRestTestBed {

  private static RequestSpecification spec() {
    var clusterId = ConfluentLocalKafkaWithRestProxyContainer.CLUSTER_ID;
    return given()
        .header("X-connection-id", CONNECTION_ID)
        .when()
        .pathParams(Map.of("cluster_id", clusterId));
  }

  @Test
  void shouldCreateKafkaTopic() throws Exception {
    createTopic("test-topic-1");

    // Get topic should contain the topic name
    spec()
        .get("/internal/kafka/v3/clusters/{cluster_id}/topics/test-topic-1")
        .then()
        .statusCode(200)
        .body("topic_name", equalTo("test-topic-1"));

    // List topics should contain the topic name
    spec()
        .get("/internal/kafka/v3/clusters/{cluster_id}/topics")
        .then()
        .statusCode(200)
        // Could be at any index
        .body("data.find { it.topic_name == 'test-topic-1' }.topic_name", equalTo("test-topic-1"));

    deleteTopic("test-topic-1");
  }

  @Test
  void shouldDeleteKafkaTopic() throws Exception {
    createTopic("test-topic-delete-me");

    // Delete topic should return 204
    spec()
        .delete("/internal/kafka/v3/clusters/{cluster_id}/topics/test-topic-delete-me")
        .then()
        .statusCode(204);

    // List topics should not contain the topic name
    spec()
        .get("/internal/kafka/v3/clusters/{cluster_id}/topics")
        .then()
        .statusCode(200)
        .body("data.find { it.topic_name == 'test-topic-delete-me' }", equalTo(null));
  }

  @Test
  void shouldRaise404WhenGettingNonExistentTopic() {
    spec()
        .get("/internal/kafka/v3/clusters/{cluster_id}/topics/non-existent-topic")
        .then()
        .statusCode(404)
        .body("error_code", equalTo(404))
        .body("message", equalTo("This server does not host this topic-partition."));
  }

  @Test
  void shouldRaise404WhenDeletingNonExistentTopic() {
    spec()
        .delete("/internal/kafka/v3/clusters/{cluster_id}/topics/non-existent-topic")
        .then()
        .statusCode(404)
        .body("error_code", equalTo(404))
        .body("message", equalTo("This server does not host this topic-partition."));
  }

  @Test
  void shouldRaise409WhenCreatingExistingTopic() throws Exception {
    createTopic("test-topic-2");

    spec()
        .body("{\"topic_name\":\"test-topic-2\"}")
        .header("Content-Type", "application/json")
        .post("/internal/kafka/v3/clusters/{cluster_id}/topics")
        .then()
        .statusCode(409)
        .body("error_code", equalTo(409))
        .body("message", equalTo("Topic 'test-topic-2' already exists."));

    deleteTopic("test-topic-2");
  }

  @Test
  void shouldRaise404OnNonExistentCluster() {
    given()
        .header("X-connection-id", CONNECTION_ID)
        .when()
        .get("/internal/kafka/v3/clusters/non-existent-cluster/topics")
        .then()
        .statusCode(404)
        .body("error_code", equalTo(404))
        .body("message", equalTo("Kafka cluster 'non-existent-cluster' not found."));
  }

  @Test
  void shouldRaise404OnNonExistentConnection() {
    given()
        .header("X-connection-id", "non-existent-connection")
        .when()
        .get("/internal/kafka/v3/clusters/{cluster_id}/topics",
            Map.of("cluster_id", ConfluentLocalKafkaWithRestProxyContainer.CLUSTER_ID))
        .then()
        .statusCode(404)
        .body("error_code", equalTo(404))
        .body("message", equalTo("Connection not found: non-existent-connection"));
  }

  @Test
  void shouldRaise400OnAbsentConnectionIdHeader() {
    given()
        // No connection ID header
        .when()
        .get("/internal/kafka/v3/clusters/{cluster_id}/topics",
            Map.of("cluster_id", ConfluentLocalKafkaWithRestProxyContainer.CLUSTER_ID))
        .then()
        .statusCode(400)
        .body("error_code", equalTo(400))
        .body("message", equalTo("Missing required header: x-connection-id"));
  }
}