package io.confluent.idesidecar.restapi.kafkarest.impl;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;

import io.confluent.idesidecar.restapi.testutil.NoAccessFilterProfile;
import io.confluent.idesidecar.restapi.util.ConfluentLocalKafkaWithRestProxyContainer;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.Test;


@QuarkusIntegrationTest
@TestProfile(NoAccessFilterProfile.class)
public class ClusterV3ApiImplIT extends KafkaRestTestBed {
  @Test
  void shouldListKafkaClusters() {
    given()
        .header("X-connection-id", CONNECTION_ID)
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
    given()
        .header("X-connection-id", CONNECTION_ID)
        .when()
        .get("/internal/kafka/v3/clusters/{cluster_id}",
            ConfluentLocalKafkaWithRestProxyContainer.CLUSTER_ID)
        .then()
        .statusCode(200)
        .body("cluster_id", equalTo(ConfluentLocalKafkaWithRestProxyContainer.CLUSTER_ID));
  }

  @Test
  void shouldReturn404WhenClusterNotFound() {
    given()
        .header("X-connection-id", CONNECTION_ID)
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
    given()
        .when()
        .get("/internal/kafka/v3/clusters")
        .then()
        .statusCode(400)
        .body("error_code", equalTo(400))
        .body("message", equalTo("Missing required header: x-connection-id"));
  }

  @Test
  void shouldRaiseErrorWhenConnectionNotFound() {
    given()
        .header("X-connection-id", "non-existent-connection")
        .when()
        .get("/internal/kafka/v3/clusters")
        .then()
        .statusCode(404)
        .body("error_code", equalTo(404))
        .body("message", equalTo("Connection not found: non-existent-connection"));
  }
}