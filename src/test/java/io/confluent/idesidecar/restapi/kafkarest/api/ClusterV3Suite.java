package io.confluent.idesidecar.restapi.kafkarest.api;

import static org.hamcrest.Matchers.equalTo;

import io.confluent.idesidecar.restapi.integration.ITSuite;
import io.confluent.idesidecar.restapi.util.ConfluentLocalKafkaWithRestProxyContainer;
import org.junit.jupiter.api.Test;

public interface ClusterV3Suite extends ITSuite {

  @Test
  default void shouldListKafkaClusters() {
    givenConnectionId()
        .when()
        .get("/internal/kafka/v3/clusters")
        .then()
        .statusCode(200)
        .body("data.size()", equalTo(1))
        .body("data[0].cluster_id",
            equalTo(ConfluentLocalKafkaWithRestProxyContainer.CLUSTER_ID));
  }

  @Test
  default void shouldGetKafkaCluster() {
    givenConnectionId()
        .when()
        .get("/internal/kafka/v3/clusters/{cluster_id}",
            ConfluentLocalKafkaWithRestProxyContainer.CLUSTER_ID)
        .then()
        .statusCode(200)
        .body("cluster_id", equalTo(ConfluentLocalKafkaWithRestProxyContainer.CLUSTER_ID));
  }

  @Test
  default void shouldReturn404WhenClusterNotFound() {
    givenConnectionId()
        .when()
        .get("/internal/kafka/v3/clusters/{cluster_id}",
            "non-existent-cluster")
        .then()
        .statusCode(404)
        .body("error_code", equalTo(404))
        .body("message", equalTo("Kafka cluster 'non-existent-cluster' not found."));
  }

  @Test
  default void shouldRaiseErrorWhenGettingClustersAndConnectionIdIsMissing() {
    shouldRaiseErrorWhenConnectionIdIsMissing("/internal/kafka/v3/clusters");
  }

  @Test
  default void shouldRaiseErrorWhenGettingClustersAndConnectionNotFound() {
    shouldRaiseErrorWhenConnectionNotFound("/internal/kafka/v3/clusters");
  }
}