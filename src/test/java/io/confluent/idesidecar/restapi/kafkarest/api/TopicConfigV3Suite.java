package io.confluent.idesidecar.restapi.kafkarest.api;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

import io.confluent.idesidecar.restapi.integration.ITSuite;
import org.junit.jupiter.api.Test;
import java.util.List;

public interface TopicConfigV3Suite extends ITSuite {

  @Test
  default void shouldListTopicConfigs() {
    createTopic("test-topic-1");

    // List topic configs with default values
    assertTopicHasDefaultConfigs("test-topic-1");
  }

  @Test
  default void shouldBatchAlterTopicConfigs() {
    // Create topic
    createTopic("test-topic-2");

    // List topic configs before
    assertTopicHasDefaultConfigs("test-topic-2");

    // Alter topic configs
    givenDefault()
        // So that "configs:alter" doesn't get encoded as "configs%3Aalter"
        // Quarkus does not decode this automatically
        .urlEncodingEnabled(false)
        .body("""
            {
              "data": [
                {
                  "name": "cleanup.policy",
                  "value": "compact",
                  "operation": "set"
                },
                {
                  "name": "retention.ms",
                  "value": "1000000",
                  "operation": "set"
                }
              ]
            }
            """)
        .post("/internal/kafka/v3/clusters/{cluster_id}/topics/test-topic-2/configs:alter")
        .then()
        .statusCode(204);

    // List topic configs
    givenDefault()
        .get("/internal/kafka/v3/clusters/{cluster_id}/topics/{topic_name}/configs", "test-topic-2")
        .then()
        .statusCode(200)
        .body("data.size()", greaterThan(0))
        .body("data.find { it.name == 'cleanup.policy' }.value", equalTo("compact"))
        .body("data.find { it.name == 'retention.ms' }.value", equalTo("1000000"));

    // Now remove the override
    givenDefault()
        .urlEncodingEnabled(false)
        .body("""
            {
              "data": [
                {
                  "name": "cleanup.policy",
                  "operation": "delete"
                },
                {
                  "name": "retention.ms",
                  "operation": "delete"
                }
              ]
            }
            """)
        .post("/internal/kafka/v3/clusters/{cluster_id}/topics/test-topic-2/configs:alter")
        .then()
        .statusCode(204);

    // List topic configs should be back to default
    assertTopicHasDefaultConfigs("test-topic-2");
  }

  private void assertTopicHasDefaultConfigs(String topicName) {
    givenDefault()
        .get("/internal/kafka/v3/clusters/{cluster_id}/topics/{topic_name}/configs", topicName)
        .then()
        .statusCode(200)
        .body("data.size()", greaterThan(0))
        .body("data.find { it.name == 'cleanup.policy' }.value", equalTo("delete"))
        .body("data.find { it.name == 'retention.ms' }.value", equalTo("604800000"));
  }

  @Test
  default void shouldValidateOnlyBatchAlterTopicConfigs() {
    // Create topic
    createTopic("test-topic-3");

    // List topic configs before
    assertTopicHasDefaultConfigs("test-topic-3");

    givenDefault()
        .urlEncodingEnabled(false)
        .body("""
            {
              "data": [
                {
                  "name": "cleanup.policy",
                  "value": "compact",
                  "operation": "set"
                },
                {
                  "name": "retention.ms",
                  "value": "1000000",
                  "operation": "set"
                }
              ],
              "validate_only": true
            }
            """)
        .post("/internal/kafka/v3/clusters/{cluster_id}/topics/test-topic-3/configs:alter")
        .then()
        .statusCode(204);

    // List topic configs should be unchanged
    assertTopicHasDefaultConfigs("test-topic-3");
  }

  @Test
  default void shouldThrowBadRequestOnInvalidConfiguration() {
    // Create topic
    createTopic("test-topic-4");

    // List topic configs before
    assertTopicHasDefaultConfigs("test-topic-4");

    // Try configs:alter with an invalid value
    givenDefault()
        .urlEncodingEnabled(false)
        .body("""
            {
              "data": [
                {
                  "name": "cleanup.policy",
                  "value": "invalid",
                  "operation": "set"
                }
              ]
            }
            """)
        .post("/internal/kafka/v3/clusters/{cluster_id}/topics/test-topic-4/configs:alter")
        .then()
        .statusCode(400)
        .body("error_code", equalTo(400))
        .body("message", equalTo("Invalid value invalid for configuration cleanup.policy: String must be one of: compact, delete"));

    // Try again with validate_only set to true, we should still get the same error
    givenDefault()
        .urlEncodingEnabled(false)
        .body("""
            {
              "data": [
                {
                  "name": "cleanup.policy",
                  "value": "invalid",
                  "operation": "set"
                }
              ],
              "validate_only": true
            }
            """)
        .post("/internal/kafka/v3/clusters/{cluster_id}/topics/test-topic-4/configs:alter")
        .then()
        .statusCode(400)
        .body("error_code", equalTo(400))
        .body("message", equalTo("Invalid value invalid for configuration cleanup.policy: String must be one of: compact, delete"));

    // List topic configs should still be unchanged
    assertTopicHasDefaultConfigs("test-topic-4");
  }

  @Test
  default void shouldThrowBadRequestOnDuplicateConfiguration() {
    // Create topic
    createTopic("test-topic-5");

    // Try configs:alter with a duplicate configuration
    givenDefault()
        .urlEncodingEnabled(false)
        .body("""
            {
              "data": [
                {
                  "name": "cleanup.policy",
                  "value": "compact",
                  "operation": "set"
                },
                {
                  "name": "cleanup.policy",
                  "value": "delete",
                  "operation": "set"
                }
              ]
            }
            """)
        .post("/internal/kafka/v3/clusters/{cluster_id}/topics/test-topic-5/configs:alter")
        .then()
        .statusCode(400)
        .body("error_code", equalTo(400))
        .body("message", equalTo("Error due to duplicate config keys"));

    // List topic configs should still be unchanged
    assertTopicHasDefaultConfigs("test-topic-5");
  }

  @Test
  default void shouldThrowBadRequestOnUnknownOperation() {
    // Create topic
    createTopic("test-topic-5");

    // Try configs:alter with an unknown operation
    givenDefault()
        .urlEncodingEnabled(false)
        .body("""
            {
              "data": [
                {
                  "name": "cleanup.policy",
                  "value": "compact",
                  "operation": "unknown"
                }
              ]
            }
            """)
        .post("/internal/kafka/v3/clusters/{cluster_id}/topics/test-topic-5/configs:alter")
        .then()
        .statusCode(400)
        .body("error_code", equalTo(400))
        .body("message", equalTo("Invalid operation unknown for configuration cleanup.policy: must be one of: [SET, DELETE]"));
  }

  @Test
  default void shouldThrowBadRequestOnEmptyConfiguration() {
    // Create topic
    createTopic("test-topic-5");

    // Try configs:alter with an empty request
    for (var body: List.of("{}", "{\"data\": []}")) {
      givenDefault()
          .urlEncodingEnabled(false)
          .body(body)
          .post("/internal/kafka/v3/clusters/{cluster_id}/topics/test-topic-5/configs:alter")
          .then()
          .statusCode(400)
          .body("error_code", equalTo(400))
          .body("message", equalTo("No configurations provided"));
    }
  }

  @Test
  default void shouldRaise404ForNonExistentTopic() {
    givenDefault()
        .get("/internal/kafka/v3/clusters/{cluster_id}/topics/non-existent-topic/configs")
        .then()
        .statusCode(404)
        .body("error_code", equalTo(404))
        .body("message", equalTo("This server does not host this topic-partition."));

    givenDefault()
        .urlEncodingEnabled(false)
        .post("/internal/kafka/v3/clusters/{cluster_id}/topics/non-existent-topic/configs:alter")
        .then()
        .statusCode(404)
        .body("error_code", equalTo(404))
        .body("message", equalTo("This server does not host this topic-partition."));
  }

  @Test
  default void shouldRaise404OnNonExistentCluster() {
    givenConnectionId()
        .when()
        .get("/internal/kafka/v3/clusters/non-existent-cluster/topics/foo/configs")
        .then()
        .statusCode(404)
        .body("error_code", equalTo(404))
        .body("message", equalTo("Kafka cluster 'non-existent-cluster' not found."));

    givenConnectionId()
        .urlEncodingEnabled(false)
        .header("Content-Type", "application/json")
        .when()
        .post("/internal/kafka/v3/clusters/non-existent-cluster/topics/foo/configs:alter")
        .then()
        .statusCode(404)
        .body("error_code", equalTo(404))
        .body("message", equalTo("Kafka cluster 'non-existent-cluster' not found."));
  }
}
