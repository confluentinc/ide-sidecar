package io.confluent.idesidecar.restapi.kafkarest.api;

import static io.confluent.idesidecar.restapi.util.ConfluentLocalKafkaWithRestProxyContainer.CLUSTER_ID;
import static org.hamcrest.Matchers.equalTo;

import io.confluent.idesidecar.restapi.testutil.NoAccessFilterProfile;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@QuarkusIntegrationTest
@Tag("io.confluent.common.utils.IntegrationTest")
@TestProfile(NoAccessFilterProfile.class)
class TopicV3ApiImplIT extends KafkaRestTestBed {

  @Test
  void shouldCreateKafkaTopic() throws Exception {
    createTopic("test-topic-1");

    // Get topic should contain the topic name
    givenDefault()
        .get("/internal/kafka/v3/clusters/{cluster_id}/topics/test-topic-1")
        .then()
        .statusCode(200)
        .body("topic_name", equalTo("test-topic-1"));

    // List topics should contain the topic name
    givenDefault()
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
    givenDefault()
        .delete("/internal/kafka/v3/clusters/{cluster_id}/topics/test-topic-delete-me")
        .then()
        .statusCode(204);

    // List topics should not contain the topic name
    givenDefault()
        .get("/internal/kafka/v3/clusters/{cluster_id}/topics")
        .then()
        .statusCode(200)
        .body("data.find { it.topic_name == 'test-topic-delete-me' }", equalTo(null));
  }

  @Test
  void shouldRaise404WhenGettingNonExistentTopic() {
    givenDefault()
        .get("/internal/kafka/v3/clusters/{cluster_id}/topics/non-existent-topic")
        .then()
        .statusCode(404)
        .body("error_code", equalTo(404))
        .body("message", equalTo("This server does not host this topic-partition."));
  }

  @Test
  void shouldRaise404WhenDeletingNonExistentTopic() {
    givenDefault()
        .delete("/internal/kafka/v3/clusters/{cluster_id}/topics/non-existent-topic")
        .then()
        .statusCode(404)
        .body("error_code", equalTo(404))
        .body("message", equalTo("This server does not host this topic-partition."));
  }

  @Test
  void shouldRaise409WhenCreatingExistingTopic() throws Exception {
    createTopic("test-topic-2");

    givenDefault()
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
    givenConnectionId()
        .when()
        .get("/internal/kafka/v3/clusters/non-existent-cluster/topics")
        .then()
        .statusCode(404)
        .body("error_code", equalTo(404))
        .body("message", equalTo("Kafka cluster 'non-existent-cluster' not found."));
  }

  @Test
  void shouldRaiseErrorWhenConnectionNotFound() {
    shouldRaiseErrorWhenConnectionNotFound(
        "/internal/kafka/v3/clusters/%s/topics".formatted(CLUSTER_ID)
    );
  }

  @Test
  void shouldRaiseErrorWhenConnectionIdIsMissing() {
    shouldRaiseErrorWhenConnectionIdIsMissing(
        "/internal/kafka/v3/clusters/%s/topics".formatted(CLUSTER_ID)
    );
  }
}