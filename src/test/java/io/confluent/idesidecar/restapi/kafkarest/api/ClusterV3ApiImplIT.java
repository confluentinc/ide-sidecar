package io.confluent.idesidecar.restapi.kafkarest.api;

import static org.hamcrest.Matchers.equalTo;

import io.confluent.idesidecar.restapi.testutil.NoAccessFilterProfile;
import io.confluent.idesidecar.restapi.util.AbstractSidecarIT;
import io.confluent.idesidecar.restapi.util.ConfluentLocalKafkaWithRestProxyContainer;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@QuarkusIntegrationTest
@Tag("io.confluent.common.utils.IntegrationTest")
@TestProfile(NoAccessFilterProfile.class)
public class ClusterV3ApiImplIT extends AbstractSidecarIT {

  @BeforeEach
  public void beforeEach() {
    setupLocalConnection();
  }

  @Test
  void shouldListKafkaClusters() {
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
  void shouldGetKafkaCluster() {
    givenConnectionId()
        .when()
        .get("/internal/kafka/v3/clusters/{cluster_id}",
            ConfluentLocalKafkaWithRestProxyContainer.CLUSTER_ID)
        .then()
        .statusCode(200)
        .body("cluster_id", equalTo(ConfluentLocalKafkaWithRestProxyContainer.CLUSTER_ID));
  }

  @Test
  void shouldReturn404WhenClusterNotFound() {
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
  void shouldRaiseErrorWhenConnectionIdIsMissing() {
    shouldRaiseErrorWhenConnectionIdIsMissing("/internal/kafka/v3/clusters");
  }

  @Test
  void shouldRaiseErrorWhenConnectionNotFound() {
    shouldRaiseErrorWhenConnectionNotFound("/internal/kafka/v3/clusters");
  }
}