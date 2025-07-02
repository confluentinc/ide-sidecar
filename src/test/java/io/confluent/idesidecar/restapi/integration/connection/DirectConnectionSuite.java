package io.confluent.idesidecar.restapi.integration.connection;

import static io.restassured.RestAssured.given;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import io.confluent.idesidecar.restapi.credentials.Password;
import io.confluent.idesidecar.restapi.integration.ITSuite;
import io.confluent.idesidecar.restapi.models.Connection;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType;
import io.confluent.idesidecar.restapi.models.ConnectionStatus.ConnectedState;
import io.confluent.idesidecar.restapi.util.SidecarClient;
import io.restassured.http.ContentType;
import jakarta.ws.rs.core.MediaType;
import java.time.Duration;
import org.junit.jupiter.api.Test;

public interface DirectConnectionSuite extends ITSuite {

  @Test
  default void shouldTestDirectConnection() {
    // Not all environments support direct connections
    var spec = environment().directConnectionSpec().orElse(null);
    assertNotNull(spec,
        "Expected environment %s has direct connection spec".formatted(environment().name()));

    // Test the connection and mark it as the one we'll use
    var rsps = testConnectionWithResponse(spec)
        .statusCode(200)
        .contentType(ContentType.JSON)
        .body("api_version", equalTo("gateway/v1"))
        .body("kind", equalTo("Connection"))
        .body("metadata.self", containsString("/gateway/v1/connections/"))
        .body("metadata.resource_name", nullValue())
        .body("id", equalTo(spec.id()))
        .body("spec.id", equalTo(spec.id()))
        .body("spec.name", equalTo(spec.name()))
        .body("spec.type", equalTo(ConnectionType.DIRECT.name()))
        .body("status.kafka_cluster.state", equalTo(ConnectedState.SUCCESS.name()));

    assertNotNull(rsps.extract().body().as(Connection.class));

    if (spec.schemaRegistryConfig() != null) {
      rsps.body("status.schema_registry.state", equalTo(ConnectedState.SUCCESS.name()));

      // Now without the Schema Registry
      testConnectionWithResponse(spec.withSchemaRegistry(null))
          .statusCode(200)
          .contentType(ContentType.JSON)
          .body("api_version", equalTo("gateway/v1"))
          .body("kind", equalTo("Connection"))
          .body("metadata.self", containsString("/gateway/v1/connections/"))
          .body("metadata.resource_name", nullValue())
          .body("id", equalTo(spec.id()))
          .body("spec.id", equalTo(spec.id()))
          .body("spec.name", equalTo(spec.name()))
          .body("spec.type", equalTo(ConnectionType.DIRECT.name()))
          .body("spec.kafka_cluster", notNullValue())
          .body("spec.schema_registry", nullValue())
          .body("status.kafka_cluster.state", equalTo(ConnectedState.SUCCESS.name()))
          .body("status.schema_registry", nullValue())
          .extract().body().as(Connection.class);

      // Now without the Kafka cluster
      testConnectionWithResponse(spec.withKafkaCluster(null))
          .statusCode(200)
          .contentType(ContentType.JSON)
          .body("api_version", equalTo("gateway/v1"))
          .body("kind", equalTo("Connection"))
          .body("metadata.self", containsString("/gateway/v1/connections/"))
          .body("metadata.resource_name", nullValue())
          .body("id", equalTo(spec.id()))
          .body("spec.id", equalTo(spec.id()))
          .body("spec.name", equalTo(spec.name()))
          .body("spec.type", equalTo(ConnectionType.DIRECT.name()))
          .body("spec.kafka_cluster", nullValue())
          .body("spec.schema_registry", notNullValue())
          .body("status.kafka_cluster", nullValue())
          .body("status.schema_registry.state", equalTo(ConnectedState.SUCCESS.name()))
          .extract().body().as(Connection.class);
    }
  }

  @Test
  default void shouldTestDirectConnectionWithoutId() {
    // Not all environments support direct connections
    var spec = environment().directConnectionSpec().orElse(null);
    assertNotNull(spec,
        "Expected environment %s has direct connection spec".formatted(environment().name()));

    // Test the connection with a spec that has no ID
    testConnectionWithResponse(spec.withId(null))
        .statusCode(200)
        .contentType(ContentType.JSON)
        .body("api_version", equalTo("gateway/v1"))
        .body("kind", equalTo("Connection"))
        .body("metadata.self", containsString("/gateway/v1/connections/"))
        .body("metadata.resource_name", nullValue())
        .body("id", nullValue())
        .body("spec.id", nullValue())
        .body("spec.name", equalTo(spec.name()))
        .body("spec.type", equalTo(ConnectionType.DIRECT.name()))
        .body("status.kafka_cluster.state", equalTo(ConnectedState.SUCCESS.name()))
        .extract().body().as(Connection.class);
  }

  @Test
  default void shouldCreateAndListAndGetAndDeleteDirectConnection() {
    // Not all environments support direct connections
    var spec = environment().directConnectionSpec().orElse(null);
    assertNotNull(spec,
        "Expected environment %s has direct connection spec".formatted(environment().name()));

    // Create the connection and mark it as the one we'll use
    var connection = createConnection(spec);
    useConnection(connection.id());

    final boolean startedWithKafka = spec.kafkaClusterConfig() != null;
    final boolean startedWithSr = spec.schemaRegistryConfig() != null;

    // Verify the response has the necessary objects
    assertEquals(connection.id(), connection.spec().id());
    assertNotNull(connection.spec());
    assertEquals(spec.name(), connection.spec().name());
    assertEquals(spec.kafkaClusterConfig(), connection.spec().kafkaClusterConfig());
    assertEquals(spec.schemaRegistryConfig(), connection.spec().schemaRegistryConfig());
    assertNotNull(connection.status());
    var expectedKafkaState = ConnectedState.NONE;
    var expectedSrState = ConnectedState.NONE;
    if (startedWithKafka) {
      assertNotNull(connection.spec().kafkaClusterConfig().bootstrapServers());
      assertNotNull(connection.status().kafkaCluster());
      assertNotNull(connection.status().kafkaCluster().state());
      expectedKafkaState = ConnectedState.SUCCESS;
    } else {
      assertNull(connection.status().kafkaCluster());
    }
    if (startedWithSr) {
      assertNotNull(connection.status().schemaRegistry());
      assertNotNull(connection.status().schemaRegistry().state());
      expectedSrState = ConnectedState.SUCCESS;
    } else {
      assertNull(connection.status().schemaRegistry());
    }

    // Update the spec to include the generated ID
    spec = spec.withId(connection.id());

    // Wait until the statuses have been updated to the expected states
    final var expectedKafkaStateName = expectedKafkaState.name();
    final var expectedSrStateName = expectedSrState.name();
    await().atMost(Duration.ofSeconds(10)).until(() -> {
      try {
        given()
            .when()
            .get("/gateway/v1/connections/{id}", connection.id())
            .then()
            .statusCode(200)
            .body("status.kafka_cluster.state", equalTo(expectedKafkaStateName))
            .body("status.schema_registry.state", equalTo(expectedSrStateName));
        return true;
      } catch (AssertionError e) {
        return false;
      }
    });

    // Get the connection again
    var connection2 = given()
        .when()
        .get("/gateway/v1/connections/{id}", connection.id())
        .then()
        .statusCode(200)
        .body("id", equalTo(connection.id()))
        .body("metadata.self", notNullValue())
        .body("spec.id", equalTo(connection.id()))
        .body("spec.name", equalTo(spec.name()))
        .body("spec.type", equalTo(ConnectionType.DIRECT.name()))
        .body("spec.local_config", nullValue())
        .body("spec.ccloud_config", nullValue())
        .body("spec.kafka_cluster.bootstrap_servers",
            equalTo(spec.kafkaClusterConfig().bootstrapServers()))
        .extract().body().as(Connection.class);

    if (startedWithKafka) {
      assertEquals(spec.kafkaClusterConfig(), connection2.spec().kafkaClusterConfig());
    }
    if (startedWithSr) {
      assertEquals(spec.schemaRegistryConfig(), connection2.spec().schemaRegistryConfig());
    }
    assertNotNull(connection2.status().kafkaCluster());
    assertEquals(expectedKafkaState, connection2.status().kafkaCluster().state());
    assertNotNull(connection2.status().schemaRegistry());
    assertEquals(expectedSrState, connection2.status().schemaRegistry().state());

    // Query for resources
    if (startedWithKafka) {
      submitDirectConnectionsGraphQL()
          .body("data.directConnections[0].id", equalTo(spec.id()))
          .body("data.directConnections[0].name", equalTo(spec.name()))
          .body("data.directConnections[0].type", equalTo("DIRECT"))
          .body("data.directConnections[0].kafkaCluster.id", notNullValue())
          .body("data.directConnections[0].kafkaCluster.bootstrapServers",
              equalTo(spec.kafkaClusterConfig().bootstrapServers()));
    }
    if (startedWithSr) {
      submitDirectConnectionsGraphQL()
          .body("data.directConnections[0].id", equalTo(spec.id()))
          .body("data.directConnections[0].name", equalTo(spec.name()))
          .body("data.directConnections[0].type", equalTo("DIRECT"))
          .body("data.directConnections[0].schemaRegistry.id", notNullValue())
          .body("data.directConnections[0].schemaRegistry.uri",
              equalTo(spec.schemaRegistryConfig().uri()));
    }

    if (startedWithSr && startedWithKafka) {
      // Update the connection to remove the schema registry
      var specNoSr = spec.withSchemaRegistry(null);

      given()
          .when()
          .contentType(MediaType.APPLICATION_JSON)
          .body(specNoSr)
          .put("/gateway/v1/connections/{id}", connection.id())
          .then()
          .statusCode(200)
          .body("id", equalTo(connection.id()))
          .body("metadata.self", notNullValue())
          .body("spec.id", equalTo(connection.id()))
          .body("spec.name", equalTo(spec.name()))
          .body("spec.type", equalTo(ConnectionType.DIRECT.name()))
          .body("spec.local_config", nullValue())
          .body("spec.ccloud_config", nullValue())
          .body("spec.kafka_cluster.bootstrap_servers",
              equalTo(specNoSr.kafkaClusterConfig().bootstrapServers()))
          .body("spec.schema_registry", nullValue())
          .extract().body().as(Connection.class);

      // Wait until the statuses have been updated to the expected states
      await().atMost(Duration.ofSeconds(10)).until(() -> {
        try {
          given()
              .when()
              .get("/gateway/v1/connections/{id}", connection.id())
              .then()
              .statusCode(200)
              .body("status.kafka_cluster.state", equalTo(ConnectedState.SUCCESS.name()))
              .body("status.schema_registry.state", nullValue());
          return true;
        } catch (AssertionError e) {
          return false;
        }
      });

      // Query for resources
      submitDirectConnectionsGraphQL()
          .body("data.directConnections[0].id", equalTo(spec.id()))
          .body("data.directConnections[0].name", equalTo(spec.name()))
          .body("data.directConnections[0].type", equalTo("DIRECT"))
          .body("data.directConnections[0].kafkaCluster.id", notNullValue())
          .body("data.directConnections[0].kafkaCluster.bootstrapServers", notNullValue())
          .body("data.directConnections[0].schemaRegistry", nullValue());
    }

    // Query for resources finds our connection
    assertTrue(
        directConnectionsGraphQLResponseContains(connection.id())
    );

    // Delete the connection
    deleteConnection(connection.id());

    // Get the connection again
    given()
        .when()
        .get("/gateway/v1/connections/{id}", connection.id())
        .then()
        .statusCode(404)
        .body("status", equalTo("404"))
        .body("code", equalTo("None"))
        .body("title", equalTo("Not Found"))
        .body("id", notNullValue())
        .body("errors", hasSize(1))
        .body("errors[0].code", equalTo("None"))
        .body("errors[0].title", equalTo("Not Found"))
        .body("errors[0].detail",
            equalTo("Connection %s is not found.".formatted(connection.id())));

    // Query for resources does not find our connection
    assertFalse(
        directConnectionsGraphQLResponseContains(connection.id())
    );
  }

  @Test
  default void shouldTransitionToInitialStatusOnConnectionUpdate() throws JsonProcessingException {
    var correctSpec = environment().directConnectionSpec().orElse(null);
    assertNotNull(correctSpec,
        "Expected environment %s has direct connection spec".formatted(environment().name()));

    correctSpec = correctSpec.withId("direct-connection-update");

    // Create the connection with incorrect Kafka cluster credentials
    var incorrectSpec = correctSpec
        .withKafkaClusterConfig(
            correctSpec.kafkaClusterConfig().withBootstrapServers("invalid:9092")
        );
    var connection = createConnection(
        incorrectSpec,
        // Don't wait until the connection is ready
        false
    );
    useConnection(connection.id());

    // Wait until the status turns to FAILED
    await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
      given()
          .when()
          .get("/gateway/v1/connections/{id}", connection.id())
          .then()
          .statusCode(200)
          .body("status.kafka_cluster.state", equalTo(ConnectedState.FAILED.name()));
    });

    var objectMapper = new ObjectMapper();
    objectMapper.registerModule(
        new SimpleModule().addSerializer(Password.class, new SidecarClient.PasswordSerializer())
    );

    // Update the connection with correct Kafka cluster credentials
    given()
        .when()
        .contentType(MediaType.APPLICATION_JSON)
        .body(objectMapper.writeValueAsString(correctSpec))
        .put("/gateway/v1/connections/{id}", connection.id())
        .then()
        .statusCode(200)
        .body("status.kafka_cluster.state", equalTo(ConnectedState.ATTEMPTING.name()));

    // Now wait until the status changes to SUCCESS
    await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
        given()
            .when()
            .get("/gateway/v1/connections/{id}", connection.id())
            .then()
            .statusCode(200)
            .body("status.kafka_cluster.state", equalTo(ConnectedState.SUCCESS.name()))
    );

    // Update the connection with incorrect Kafka cluster credentials
    given()
        .when()
        .contentType(MediaType.APPLICATION_JSON)
        .body(objectMapper.writeValueAsString(incorrectSpec))
        .put("/gateway/v1/connections/{id}", connection.id())
        .then()
        .statusCode(200)
        .body("status.kafka_cluster.state", equalTo(ConnectedState.ATTEMPTING.name()));

    // Now wait until the status changes to FAILED
    await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
        given()
            .when()
            .get("/gateway/v1/connections/{id}", connection.id())
            .then()
            .statusCode(200)
            .body("status.kafka_cluster.state", equalTo(ConnectedState.FAILED.name()))
    );
  }
}
