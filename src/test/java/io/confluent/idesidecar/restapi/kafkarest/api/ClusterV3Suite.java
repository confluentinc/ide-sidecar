package io.confluent.idesidecar.restapi.kafkarest.api;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.confluent.idesidecar.restapi.integration.ITSuite;
import org.junit.jupiter.api.Test;

public interface ClusterV3Suite extends ITSuite {

  @Test
  default void shouldListKafkaClusters() {
    assertNotNull(getClusterId());
  }

  /**
   * Get the cluster ID of the first cluster in the list of clusters.
   */
  private String getClusterId() {
    return givenConnectionId()
        .when()
        .get("/internal/kafka/v3/clusters")
        .then()
        .statusCode(200)
        .body("data.size()", equalTo(1))
        .extract()
        .path("data[0].cluster_id");
  }

  @Test
  default void shouldGetKafkaCluster() {
    var clusterId = getClusterId();
    givenConnectionId()
        .when()
        .get("/internal/kafka/v3/clusters/{cluster_id}", clusterId)
        .then()
        .statusCode(200)
        .body("cluster_id", equalTo(clusterId));
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