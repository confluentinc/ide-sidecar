package io.confluent.idesidecar.restapi.integration.connection;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.idesidecar.restapi.integration.ITSuite;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType;
import io.confluent.idesidecar.restapi.models.ConnectionStatus.Authentication.Status;
import io.restassured.http.ContentType;
import jakarta.ws.rs.core.MediaType;
import org.junit.jupiter.api.Test;

public interface LocalConnectionSuite extends ITSuite {

  @Test
  default void shouldTestLocalConnection() {
    // Not all environments support local connections
    var spec = environment().localConnectionSpec().orElse(null);
    assertNotNull(spec, "Expected environment %s has local connection spec".formatted(environment().name()));

    // Test the connection and mark it as the one we'll use
    testConnectionWithResponse(spec)
        .statusCode(200)
        .contentType(ContentType.JSON)
        .body("api_version", equalTo("gateway/v1"))
        .body("kind", equalTo("Connection"))
        .body("metadata.self", startsWith("http://localhost:26637/gateway/v1/connections/"))
        .body("metadata.resource_name", nullValue())
        .body("id", equalTo(spec.id()))
        .body("spec.id", equalTo(spec.id()))
        .body("spec.name", equalTo(spec.name()))
        .body("spec.type", equalTo(ConnectionType.LOCAL.name()))
        .body("spec.local_config", notNullValue())
        .body("spec.kafka_cluster", nullValue())
        .body("spec.schema_registry", nullValue())
        .body("status.authentication.status", equalTo(Status.NO_TOKEN.name()))
        .body("status.kafka_cluster", nullValue())
        .body("status.schema_registry", nullValue());

  }

  @Test
  default void shouldTestLocalConnectionWithoutId() {
    // Not all environments support local connections
    var spec = environment().localConnectionSpec().orElse(null);
    assertNotNull(spec, "Expected environment %s has local connection spec".formatted(environment().name()));

    // Test the connection with a spec that has no ID
    testConnectionWithResponse(spec.withId(null))
        .statusCode(200)
        .contentType(ContentType.JSON)
        .body("api_version", equalTo("gateway/v1"))
        .body("kind", equalTo("Connection"))
        .body("metadata.self", startsWith("http://localhost:26637/gateway/v1/connections/"))
        .body("metadata.resource_name", nullValue())
        .body("id", nullValue())
        .body("spec.id", nullValue())
        .body("spec.name", equalTo(spec.name()))
        .body("spec.type", equalTo(ConnectionType.LOCAL.name()))
        .body("spec.local_config", notNullValue())
        .body("spec.kafka_cluster", nullValue())
        .body("spec.schema_registry", nullValue())
        .body("status.authentication.status", equalTo(Status.NO_TOKEN.name()))
        .body("status.kafka_cluster", nullValue())
        .body("status.schema_registry", nullValue());
  }

  @Test
  default void shouldCreateAndListAndGetAndDeleteLocalConnection() {
    // Not all environments support local connections
    var spec = environment().localConnectionSpec().orElse(null);
    assertNotNull(spec, "Expected environment %s has local connection spec".formatted(environment().name()));

    // Create the connection and mark it as the one we'll use
    var connection = createConnection(spec);
    useConnection(connection.id());

    // Verify the response has the necessary objects
    assertEquals(connection.id(), connection.spec().id());
    assertNotNull(connection.spec());
    assertEquals(spec.name(), connection.spec().name());
    assertNotNull(connection.spec().localConfig());
    assertEquals(spec.localConfig().schemaRegistryUri(), connection.spec().localConfig().schemaRegistryUri());
    assertNotNull(connection.status());
    assertNotNull(connection.status().authentication());
    assertEquals(Status.NO_TOKEN, connection.status().authentication().status());

    // Update the spec to include the generated ID
    spec = spec.withId(connection.id());

    // Get the connection again
    given()
        .when()
        .get("/gateway/v1/connections/{id}", connection.id())
        .then()
        .statusCode(200)
        .body("id", equalTo(connection.id()))
        .body("metadata.self", notNullValue())
        .body("spec.id", equalTo(connection.id()))
        .body("spec.name", equalTo(spec.name()))
        .body("spec.type", equalTo(ConnectionType.LOCAL.name()))
        .body("spec.local_config", notNullValue())
        .body("spec.local_config.schema-registry-uri", equalTo(spec.localConfig().schemaRegistryUri()))
        .body("spec.ccloud_config", nullValue())
        .body("spec.kafka_cluster", nullValue())
        .body("spec.schema_registry", nullValue())
        .body(
            "status.authentication.status",
            equalTo(Status.NO_TOKEN.name())
        );

    // Query for resources
    submitLocalConnectionsGraphQL()
        .body("data.localConnections[0].id", equalTo(spec.id()))
        .body("data.localConnections[0].name", equalTo("local")) // hard-coded in LocalConnection
        .body("data.localConnections[0].type", equalTo("LOCAL"))
        .body("data.localConnections[0].kafkaCluster.id", notNullValue())
        .body("data.localConnections[0].kafkaCluster.bootstrapServers", notNullValue())
        .body("data.localConnections[0].schemaRegistry.uri", notNullValue());

    // Update the connection to remove the schema registry
    var specNoSr = spec.withLocalConfig("");

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
        .body("spec.type", equalTo(ConnectionType.LOCAL.name()))
        .body("spec.local_config", notNullValue())
        .body("spec.local_config.schema-registry-uri", equalTo(""))
        .body("spec.ccloud_config", nullValue())
        .body("spec.kafka_cluster", nullValue())
        .body("spec.schema_registry", nullValue())
        .body(
            "status.authentication.status",
            equalTo(Status.NO_TOKEN.name())
        );

    // Query for resources
    submitLocalConnectionsGraphQL()
        .body("data.localConnections[0].id", equalTo(spec.id()))
        .body("data.localConnections[0].name", equalTo("local")) // hard-coded in LocalConnection
        .body("data.localConnections[0].type", equalTo("LOCAL"))
        .body("data.localConnections[0].kafkaCluster.id", notNullValue())
        .body("data.localConnections[0].kafkaCluster.bootstrapServers", notNullValue())
        .body("data.localConnections[0].schemaRegistry", nullValue());

    // Query for resources finds our connection
    assertTrue(
        localConnectionsGraphQLResponseContains(connection.id())
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
        .body("errors[0].detail", equalTo("Connection local-connection is not found."));

    // Query for resources does not find our connection
    assertFalse(
        localConnectionsGraphQLResponseContains(connection.id())
    );
  }

}
