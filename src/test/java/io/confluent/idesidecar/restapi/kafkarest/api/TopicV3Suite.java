package io.confluent.idesidecar.restapi.kafkarest.api;

import static io.confluent.idesidecar.restapi.util.ConfluentLocalKafkaWithRestProxyContainer.CLUSTER_ID;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.equalTo;

import io.confluent.idesidecar.restapi.integration.ITSuite;
import java.time.Duration;
import org.junit.jupiter.api.Test;

public interface TopicV3Suite extends ITSuite {

  @Test
  default void shouldCreateKafkaTopic() {
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
  }

  @Test
  default void shouldDeleteKafkaTopic() {
    createTopic("test-topic-delete-me");

    // Delete topic should return 204
    givenDefault()
        .delete("/internal/kafka/v3/clusters/{cluster_id}/topics/test-topic-delete-me")
        .then()
        .statusCode(204);

    // List topics should not contain the topic name
    await().atMost(Duration.ofSeconds(10)).until(
        () -> !givenDefault()
            .get("/internal/kafka/v3/clusters/{cluster_id}/topics")
            .then()
            .extract()
            .body()
            .jsonPath()
            .getList("data.topic_name")
            .contains("test-topic-delete-me")
    );
  }

  @Test
  default void shouldRaise404WhenGettingNonExistentTopic() {
    givenDefault()
        .get("/internal/kafka/v3/clusters/{cluster_id}/topics/non-existent-topic")
        .then()
        .statusCode(404)
        .body("error_code", equalTo(404))
        .body("message", equalTo("This server does not host this topic-partition."));
  }

  @Test
  default void shouldRaise404WhenDeletingNonExistentTopic() {
    givenDefault()
        .delete("/internal/kafka/v3/clusters/{cluster_id}/topics/non-existent-topic")
        .then()
        .statusCode(404)
        .body("error_code", equalTo(404))
        .body("message", equalTo("This server does not host this topic-partition."));
  }

  @Test
  default void shouldRaise409WhenCreatingExistingTopic() throws Exception {
    createTopic("test-topic-2");

    givenDefault()
        .body("{\"topic_name\":\"test-topic-2\"}")
        .header("Content-Type", "application/json")
        .post("/internal/kafka/v3/clusters/{cluster_id}/topics")
        .then()
        .statusCode(409)
        .body("error_code", equalTo(409))
        .body("message", equalTo("Topic 'test-topic-2' already exists."));
  }

  @Test
  default void shouldRaise404OnNonExistentCluster() {
    givenConnectionId()
        .when()
        .get("/internal/kafka/v3/clusters/non-existent-cluster/topics")
        .then()
        .statusCode(404)
        .body("error_code", equalTo(404))
        .body("message", equalTo("Kafka cluster 'non-existent-cluster' not found."));
  }

  @Test
  default void shouldRaiseErrorWhenGettingTopicsAndConnectionNotFound() {
    shouldRaiseErrorWhenConnectionNotFound(
        "/internal/kafka/v3/clusters/%s/topics".formatted(CLUSTER_ID)
    );
  }

  @Test
  default void shouldRaiseErrorWhenGettingTopicsAndConnectionIdIsMissing() {
    shouldRaiseErrorWhenConnectionIdIsMissing(
        "/internal/kafka/v3/clusters/%s/topics".formatted(CLUSTER_ID)
    );
  }
}