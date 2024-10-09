package io.confluent.idesidecar.restapi.kafkarest.api;

import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.loadResource;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static io.confluent.idesidecar.restapi.testutil.QueryResourceUtil.queryGraphQLRaw;

import io.confluent.idesidecar.restapi.testutil.NoAccessFilterProfile;
import io.confluent.idesidecar.restapi.util.ConfluentLocalKafkaWithRestProxyContainer;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;


@QuarkusIntegrationTest
@Tag("io.confluent.common.utils.IntegrationTest")
@TestProfile(NoAccessFilterProfile.class)
public class ClusterV3ApiImplIT extends KafkaRestTestBed {
  @Test
  void shouldListKafkaClusters() {
    // Try to list Kafka clusters when none are available, we should get an empty list
    given()
        .header("X-connection-id", CONNECTION_ID)
        .when()
        .get("/internal/kafka/v3/clusters")
        .then()
        .statusCode(200)
        .body("data.size()", equalTo(0));

    // Issue GraphQL query to create a Kafka cluster
    // The internal Kafka REST implementation _intentionally_ does not have
    // the ability to discover and fetch metadata about Kafka clusters that it
    // does not already know about.

    // Issue a get local connections GraphQL query. We don't care about the response.
    // By issuing the query, GraphQL will try and discover the
    // Confluent local Kafka cluster by hitting its kafka-rest server running at
    // http://localhost:8082, upon which the cluster details get cached in the ClusterCache.
    // The internal Kafka REST implementation then looks in the ClusterCache to fetch
    // metadata about Kafka clusters.
    queryGraphQLRaw(loadResource("graph/real/local-connections-query.graphql"));

    // And now, we should be able to list the Kafka cluster
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