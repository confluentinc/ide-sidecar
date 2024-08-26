package io.confluent.idesidecar.restapi.resources;

import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.asJson;
import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.loadResource;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.tomakehurst.wiremock.client.WireMock;
import io.confluent.idesidecar.restapi.connections.ConnectionStateManager;
import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.CCloudConfig;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType;
import io.confluent.idesidecar.restapi.models.ConnectionStatus;
import io.confluent.idesidecar.restapi.models.ConnectionStatus.Authentication.Status;
import io.confluent.idesidecar.restapi.util.CCloudTestUtil.AccessToken;
import io.confluent.idesidecar.restapi.util.UuidFactory;
import io.confluent.idesidecar.restapi.testutil.NoAccessFilterProfile;
import io.confluent.idesidecar.restapi.testutil.QueryResourceUtil;
import io.confluent.idesidecar.restapi.util.CCloudTestUtil;
import io.quarkiverse.wiremock.devservice.ConnectWireMock;
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.http.TestHTTPEndpoint;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.http.ContentType;
import io.restassured.response.ValidatableResponse;
import jakarta.inject.Inject;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.mockito.Mockito;

@QuarkusTest
@ConnectWireMock
@TestProfile(NoAccessFilterProfile.class)
public class ConnectionsResourceTest {
  @Inject
  ConnectionStateManager connectionStateManager;

  @InjectMock
  UuidFactory uuidFactory;

  WireMock wireMock;

  CCloudTestUtil ccloudTestUtil;

  @ConfigProperty(name = "ide-sidecar.connections.ccloud.resources.org-list-uri")
  String orgListUri;

  @BeforeEach
  public void setup() {
    connectionStateManager.clearAllConnectionStates();
  }

  private static final String FAKE_AUTHORIZATION_CODE = "fake_authorization_code";

  @BeforeEach
  void registerWireMockRoutes() {
    ccloudTestUtil = new CCloudTestUtil(wireMock, connectionStateManager);
    ccloudTestUtil.registerWireMockRoutesForCCloudOAuth(
        FAKE_AUTHORIZATION_CODE,
        "Development Org",
        null
    );
  }

  @AfterEach
  void resetWireMock() {
    wireMock.removeMappings();
  }

  @Test
  @TestHTTPEndpoint(ConnectionsResource.class)
  void listConnections_emptyListResponse() throws IOException {
    JsonNode expectedJson = asJson(
        loadResource("connections/empty-list-connections-response.json")
    );

    var actualResponse = given()
        .when()
        .get()
        .then()
        .statusCode(200)
        .contentType(ContentType.JSON)
        .extract().body().asString();

    JsonNode actualJson = asJson(actualResponse);

    assertEquals(expectedJson, actualJson);
  }

  @Test
  @TestHTTPEndpoint(ConnectionsResource.class)
  void createConnection_createsAndReturnsConnection() throws IOException {
    var requestBody = loadResource("connections/create-connection-request.json");
    var expectedJson = asJson(
        loadResource("connections/create-connection-response.json")
    );

    var actualResponse = given()
        .contentType(ContentType.JSON)
        .body(requestBody)
        .when().post()
        .then()
        .statusCode(200)
        .contentType(ContentType.JSON)
        .extract().body().asString();

    JsonNode actualJson = asJson(actualResponse);
    assertEquals(expectedJson, actualJson);

    //  Fail the recreating the same connection
    given()
        .contentType(ContentType.JSON)
        .body(requestBody)
        .when().post()
        .then()
        .statusCode(409) // Conflict
        .contentType(ContentType.JSON)
        .extract().body().asString();
  }

  @Test
  @TestHTTPEndpoint(ConnectionsResource.class)
  void getConnection_withoutToken_shouldReturnWithStatus() throws IOException {
    List<ConnectionSpec> specs = Arrays.asList(
        new ConnectionSpec("c-1", "Connection 1", ConnectionType.CCLOUD)
    );
    // Create connection
    var actualResponse = given()
        .contentType(ContentType.JSON)
        .body(specs.get(0))
        .when().post()
        .then()
        .statusCode(200)
        .contentType(ContentType.JSON)
        .extract().body().asString();

    // Get Connection
    var actualGetConnection = given()
        .contentType(ContentType.JSON)
        .when().get("/{id}", "c-1")
        .then()
        .statusCode(200)
        .contentType(ContentType.JSON)
        .extract().body().asString();

    // Verify ConnectionSpec
    JsonNode treeActual = asJson(actualGetConnection);
    ConnectionSpec expectedSpec = new ConnectionSpec(
        "c-1",
        "Connection 1",
        ConnectionType.CCLOUD
    );
    JsonNode expectedSpecAsJson = asJson(expectedSpec);
    assertTrue(treeActual.has("spec"));
    assertEquals(expectedSpecAsJson, treeActual.get("spec"));

    // Verify ConnectionStatus
    ConnectionStatus expectedStatus = ConnectionStatus.INITIAL_STATUS;
    JsonNode expectedStatusAsJson = asJson(expectedStatus);
    assertTrue(treeActual.has("status"));
    assertEquals(expectedStatusAsJson, treeActual.get("status"));

    // Status should include neither user nor organization if token is absent
    assertFalse(treeActual.get("status").get("authentication").has("user"));
    assertFalse(treeActual.get("status").get("authentication").has("organization"));
  }

  @Test
  void getConnection_withToken_shouldReturnWithStatusIncludingUserAndOrg() throws IOException {
    var connectionId = "c-1";
    var connectionName = "Connection 1";
    var connectionType = ConnectionType.CCLOUD;

    // Create authenticated connection
    ccloudTestUtil.createAuthedConnection(connectionId, connectionName, connectionType);

    // Get Connection
    var actualGetConnection = given()
        .contentType(ContentType.JSON)
        .when().get("/gateway/v1/connections/{id}", connectionId)
        .then()
        .statusCode(200)
        .contentType(ContentType.JSON)
        .extract().body().asString();

    // Verify ConnectionSpec
    JsonNode treeActual = asJson(actualGetConnection);
    ConnectionSpec expectedSpec = new ConnectionSpec(
        connectionId,
        "Connection 1",
        ConnectionType.CCLOUD
    );
    JsonNode expectedSpecAsJson = asJson(expectedSpec);
    assertTrue(treeActual.has("spec"));
    assertEquals(expectedSpecAsJson, treeActual.get("spec"));

    // Verify ConnectionStatus
    assertTrue(treeActual.has("status"));
    assertTrue(treeActual.get("status").has("authentication"));
    var authenticationStatus = treeActual.get("status").get("authentication");

    // Verify that token is valid
    assertEquals(Status.VALID_TOKEN.name(), authenticationStatus.get("status").textValue());

    // Status should include user and organization because token is valid
    assertTrue(treeActual.get("status").get("authentication").has("user"));
  }

  @Test
  void getConnection_withToken_failedAuthCheck_shouldReturnWithErrors() {
    var connectionId = "c-1";
    var connectionName = "Connection 1";
    var connectionType = ConnectionType.CCLOUD;

    // Create authenticated connection
    ccloudTestUtil.createAuthedConnection(connectionId, connectionName, connectionType);

    wireMock.register(
        WireMock
            .get("/api/check_jwt")
            .willReturn(
                WireMock
                    .aResponse()
                    .withStatus(401)
                    .withBody("invalid_json"))
            .atPriority(50));

    // Get Connection
    var connection = given()
        .contentType(ContentType.JSON)
        .when().get("/gateway/v1/connections/{id}", connectionId)
        .then()
        .statusCode(200)
        .contentType(ContentType.JSON)
        .extract().body().asString();

    // Verify ConnectionSpec
    JsonNode connectionAsJson = asJson(connection);

    // Verify ConnectionStatus
    assertTrue(connectionAsJson.has("status"));
    assertTrue(connectionAsJson.get("status").has("authentication"));
    var authenticationStatus = connectionAsJson.get("status").get("authentication");

    // Verify that token is valid
    assertEquals(Status.INVALID_TOKEN.name(), authenticationStatus.get("status").textValue());

    // Verify that error related to auth status check is present
    assertTrue(authenticationStatus.has("errors"));
    assertTrue(authenticationStatus.get("errors").has("auth_status_check"));
  }

  @Test
  @TestHTTPEndpoint(ConnectionsResource.class)
  void listConnections_returnsAllConnections() throws IOException {
    List<ConnectionSpec> specs = Arrays.asList(
        new ConnectionSpec("1", "Connection 1", ConnectionType.LOCAL),
        new ConnectionSpec("2", "Connection 2", ConnectionType.CCLOUD)
    );

    // When one Local connection is created
    var actualResponse = given()
        .contentType(ContentType.JSON)
        .body(specs.get(0))
        .when().post()
        .then()
        .statusCode(200)
        .contentType(ContentType.JSON)
        .extract().body().asString();

    // and one CCloud connection is created
    actualResponse = given()
        .contentType(ContentType.JSON)
        .body(specs.get(1))
        .when().post()
        .then()
        .statusCode(200)
        .contentType(ContentType.JSON)
        .extract().body().asString();

    // Then listing the connections should return both resources
    var actualResponse1 = given()
        .when().get()
        .then()
        .statusCode(200)
        .contentType(ContentType.JSON)
        .extract().body().asString();

    // should return both resources
    var actualJson = asJson(actualResponse1);
    var expectedJson = asJson(
        loadResource("connections/list-connections-response.json")
    );

    assertNotNull(actualJson);
    assertNotNull(expectedJson);
    assertEquals(expectedJson.get("api_version"), actualJson.get("api_version"));
    assertEquals(expectedJson.get("kind"), actualJson.get("kind"));
    assertEquals(expectedJson.get("metadata"), actualJson.get("metadata"));
    assertEquals(2, actualJson.get("data").size());

    // The local connection should match exactly
    var expectedConnection1 = expectedJson.get("data").get(0);
    var actualConnection1 = actualJson.get("data").get(0);
    assertEquals(expectedConnection1, actualConnection1);

    // but don't compare the sign-in URI since that contains a variable token
    var expectedConnection2 = expectedJson.get("data").get(1);
    var actualConnection2 = actualJson.get("data").get(1);
    assertTrue(actualConnection2.has("metadata"));
    assertTrue(actualConnection2.get("metadata").has("sign_in_uri"));
    assertEquals(expectedConnection2.get("api_version"), actualConnection2.get("api_version"));
    assertEquals(expectedConnection2.get("kind"), actualConnection2.get("kind"));
    assertEquals(expectedConnection2.get("spec"), actualConnection2.get("spec"));
    assertEquals(expectedConnection2.get("status"), actualConnection2.get("status"));
  }

  @Test
  @TestHTTPEndpoint(ConnectionsResource.class)
  void updateConnection_updatesAndReturnsUpdatedConnection() throws IOException {
    ConnectionSpec originalSpec = new ConnectionSpec(
        "1",
        "Connection 1",
        ConnectionType.LOCAL
    );
    given()
        .contentType(ContentType.JSON)
        .body(originalSpec)
        .when().post()
        .then()
        .statusCode(200);

    ConnectionSpec updatedSpec = new ConnectionSpec(originalSpec.id(), "foo", originalSpec.type());
    given()
        .contentType(ContentType.JSON)
        .body(updatedSpec)
        .when().put("/{id}", "1")
        .then()
        .statusCode(200);

    var updatedResponse = given()
        .contentType(ContentType.JSON)
        .when().get("/{id}", "1")
        .then()
        .statusCode(200)
        .contentType(ContentType.JSON)
        .extract().body().asString();

    JsonNode treeActual = asJson(updatedResponse);
    JsonNode expected = asJson(updatedSpec);

    assertEquals(expected, treeActual.get("spec"));
  }

  @Test
  @TestHTTPEndpoint(ConnectionsResource.class)
  void updateConnection_FailUpdateToId() throws IOException {
    ConnectionSpec originalSpec = new ConnectionSpec(
        "1",
        "Connection 1",
        ConnectionType.LOCAL
    );
    var actualResponse = given()
        .contentType(ContentType.JSON)
        .body(originalSpec)
        .when().post()
        .then()
        .statusCode(200)
        .contentType(ContentType.JSON)
        .extract().body().asString();

    // Change the ID & Name of the connection.
    ConnectionSpec updatedSpec = new ConnectionSpec("2", "foo", originalSpec.type());
    given()
        .contentType(ContentType.JSON)
        .body(updatedSpec)
        .when().put("/{id}", "1")
        .then()
        .statusCode(400);

    // Asser that fields are not changed.
    var getOriginalConnectionObject = given()
        .contentType(ContentType.JSON)
        .when().get("/{id}", "1")
        .then()
        .statusCode(200)
        .contentType(ContentType.JSON)
        .extract().body().asString();

    JsonNode actual = asJson(getOriginalConnectionObject);
    JsonNode expected = asJson(originalSpec);

    assertEquals(expected, actual.get("spec"));
  }

  @ParameterizedTest
  @NullAndEmptySource
  void updateConnectionWithNullOrEmptyIdShouldFail(String id) {
    var connectionSpec = ccloudTestUtil.createConnection(
        "c1", "Connection 1", ConnectionType.LOCAL);

    var badSpec = connectionSpec.withId(id);
    given()
        .contentType(ContentType.JSON)
        .body(badSpec)
        .when().put("/gateway/v1/connections/{id}", "c1")
        .then()
        .statusCode(400)
        .body("errors.size()", is(1))
        .body("errors[0].detail", is("Connection ID cannot be changed"));
  }

  @ParameterizedTest
  @NullAndEmptySource
  void updateConnectionWithNullOrEmptyNameShouldFail(String name) {
    var connectionSpec = ccloudTestUtil.createConnection(
        "c1", "Connection 1", ConnectionType.LOCAL);

    var badSpec = connectionSpec.withName(name);
    given()
        .contentType(ContentType.JSON)
        .body(badSpec)
        .when().put("/gateway/v1/connections/{id}", "c1")
        .then()
        .statusCode(400)
        .body("errors.size()", is(1))
        .body("errors[0].detail", is("Connection name cannot be null or empty"));
  }

  @Test
  void updateConnectionWithTypeChangedShouldFail() {
    ccloudTestUtil.createConnection("c1", "Connection 1", ConnectionType.LOCAL);

    var badSpec = new ConnectionSpec("c1", "Connection 1", ConnectionType.CCLOUD);
    given()
        .contentType(ContentType.JSON)
        .body(badSpec)
        .when().put("/gateway/v1/connections/{id}", "c1")
        .then()
        .statusCode(400)
        .body("errors.size()", is(1))
        .body("errors[0].detail", is("Connection type cannot be changed"));
  }

  @Test
  void updateConnectionWithMultipleValidationErrors() {
    ccloudTestUtil.createConnection("c1", "Connection 1", ConnectionType.LOCAL);

    var badSpec = new ConnectionSpec(
        "c3", "Connection name changed!", ConnectionType.PLATFORM,
        new CCloudConfig("org-id"));
    given()
        .contentType(ContentType.JSON)
        .body(badSpec)
        .when().put("/gateway/v1/connections/{id}", "c1")
        .then()
        .statusCode(400)
        .body("errors.size()", is(3))
        .body("errors.find { it.detail == 'Connection ID cannot be changed' }",
            is(notNullValue()))
        .body("errors.find { it.detail == 'Connection type cannot be changed' }",
            is(notNullValue()))
        .body("errors.find "
                + "{ it.detail == 'CCloud config cannot be set for non-CCloud connections' }",
            is(notNullValue()));
  }

  @Test
  void updateCCloudOrganizationIdForNonCCloudConnection() {
    ccloudTestUtil.createConnection("c1", "Connection 1", ConnectionType.LOCAL);
    var original = connectionStateManager.getConnectionSpec("c1");
    String requestBody = """
        {
          "id": "c1",
          "name": "Connection 1",
          "type": "LOCAL",
          "ccloud_config": {
            "organization_id": "this-wont-work"
          }
        }
        """;
    given()
        .contentType(ContentType.JSON)
        .body(requestBody)
        .when().put("/gateway/v1/connections/{id}", "c1")
        .then()
        .statusCode(400)
        .body(
            "errors[0].detail",
            is("CCloud config cannot be set for non-CCloud connections")
        );

    // Assert that the connection spec is unchanged
    assertEquals(connectionStateManager.getConnectionSpec("c1"), original);
  }

  @Test
  void updateConnectionCCloudOrganizationId() {
    // Create authenticated connection
    var accessTokens = ccloudTestUtil.createAuthedCCloudConnection(
        "c1",
        "Connection 1"
    );
    var connectionSpec = connectionStateManager.getConnectionSpec("c1");

    assertCurrentOrganizationForConnection(
        "Development Org",
        "23b1185e-d874-4f61-81d6-c9c61aa8969c"
    );

    // Update org to Test Org and expect the current org to be updated
    accessTokens = updateCCloudOrganization(
        accessTokens,
        connectionSpec.withCCloudOrganizationId("d6fc52f8-ae8a-405c-9692-e997965b730dc"),
        "Test Org"
    );

    assertCurrentOrganizationForConnection(
        "Test Org",
        "d6fc52f8-ae8a-405c-9692-e997965b730dc"
    );

    // Update org to Staging Org and expect the current org to be updated
    accessTokens = updateCCloudOrganization(
        accessTokens,
        connectionSpec.withCCloudOrganizationId("1a507773-d2cb-4055-917e-ffb205f3c433"),
        "Staging Org"
    );

    assertCurrentOrganizationForConnection(
        "Staging Org",
        "1a507773-d2cb-4055-917e-ffb205f3c433"
    );

    updateCCloudOrganization(
        accessTokens,
        connectionSpec.withCCloudOrganizationId("23b1185e-d874-4f61-81d6-c9c61aa8969c"),
        "Development Org"
    );

    assertCurrentOrganizationForConnection(
        "Development Org",
        "23b1185e-d874-4f61-81d6-c9c61aa8969c"
    );
  }

  @Test
  void updateConnectionCCloudOrganizationIdWithSameId() {
    // Create authenticated connection
    var accessTokens = ccloudTestUtil.createAuthedCCloudConnection(
        "c1",
        "Connection 1",
        "Development Org",
        null
    );
    var connectionSpec = connectionStateManager.getConnectionSpec("c1");

    assertAuthStatus("c1", "VALID_TOKEN");

    assertCurrentOrganizationForConnection(
        "Development Org",
        "23b1185e-d874-4f61-81d6-c9c61aa8969c"
    );

    // Update org to Development Org with the same ID and expect the current org to
    // remain the same
    var updatedAccessTokens = updateCCloudOrganization(
        accessTokens,
        connectionSpec,
        "Development Org"
    );

    assertCurrentOrganizationForConnection(
        "Development Org",
        "23b1185e-d874-4f61-81d6-c9c61aa8969c"
    );

    // Expect tokens to have been refreshed and is still valid
    assertAuthStatus("c1", "VALID_TOKEN");
  }

  @Test
  void updateUnauthedConnectionCCloudOrganization() {
    // Create unauthenticated CCloud connection
    var connectionSpec = ccloudTestUtil.createConnection(
        "c1", "Connection 1", ConnectionType.CCLOUD);

    given()
        .contentType(ContentType.JSON)
        .body(connectionSpec.withCCloudOrganizationId("d6fc52f8-ae8a-405c-9692-e997965b730dc"))
        .when().put("/gateway/v1/connections/{id}", connectionSpec.id())
        .then()
        .statusCode(401)
        .body("errors.size()", is(1))
        .body("errors[0].title", is("Unauthorized"));

    // The above update should have triggered a failed auth refresh, which is fine
    assertAuthStatus("c1", "NO_TOKEN")
        // Validate that we don't update the connection state due to failed refresh
        .body("status.authentication.errors.token_refresh", is(nullValue()));

    // Let's authenticate the connection now
    ccloudTestUtil.authenticateCCloudConnection(
        "c1",
        "Development Org",
        null
    );

    assertAuthStatus("c1", "VALID_TOKEN");
  }


  @Test
  void updateConnectionCCloudOrganizationToNonExistentOrg() {
    // Start with default org
    var accessTokens = ccloudTestUtil.createAuthedCCloudConnection(
        "c1",
        "Connection 1",
        "Development Org",
        null
    );

    assertAuthStatus("c1", "VALID_TOKEN")
        .body("spec.ccloud_config.organization_id", is(nullValue()));

    var refreshedTokens = ccloudTestUtil.expectRefreshTokenExchangeRequest(
        accessTokens.refresh_token()
    );

    ccloudTestUtil.expectInvalidResourceIdOnControlPlaneTokenExchange(
        refreshedTokens.id_token(),
        "non-existent-org-id"
    );

    expectListOrganizations();

    var connectionSpec = connectionStateManager.getConnectionSpec("c1");
    given()
        .contentType(ContentType.JSON)
        .body(connectionSpec.withCCloudOrganizationId("non-existent-org-id"))
        .when().put("/gateway/v1/connections/{id}", connectionSpec.id())
        .then()
        .statusCode(400)
        .body("errors.size()", is(1))
        .body("errors[0].title", is("Invalid organization ID"));

    // Validate that the connection state is unchanged
    assertAuthStatus("c1", "VALID_TOKEN")
        .body("spec.ccloud_config.organization_id", is(nullValue()));
  }

  @Test
  void updateConnectionCCloudOrganizationToDefault() {
    // Start with non-default org
    var accessTokens = ccloudTestUtil.createAuthedCCloudConnection(
        "c1",
        "Connection 1",
        "Staging Org",
        "1a507773-d2cb-4055-917e-ffb205f3c433"
    );

    assertAuthStatus("c1", "VALID_TOKEN")
        .body("spec.ccloud_config.organization_id", is("1a507773-d2cb-4055-917e-ffb205f3c433"));


    var connectionSpec = connectionStateManager.getConnectionSpec("c1");
    updateCCloudOrganization(
        accessTokens,
        // Set ccloud_config to null
        new ConnectionSpec(connectionSpec.id(), connectionSpec.name(), connectionSpec.type()),
        "Development Org"
    );

    assertAuthStatus("c1", "VALID_TOKEN")
        .body("spec.ccloud_config.organization_id", is(nullValue()));

    assertCurrentOrganizationForConnection(
        "Development Org",
        "23b1185e-d874-4f61-81d6-c9c61aa8969c"
    );
  }

  private static ValidatableResponse assertAuthStatus(String connectionId, String authStatus) {
    return given()
        .contentType(ContentType.JSON)
        .when().get("/gateway/v1/connections/{id}", connectionId)
        .then()
        .statusCode(200)
        .body("status.authentication.status", is(authStatus));
  }

  /**
   * Update the connection with a new CCloud organization ID, returns
   * the refreshed tokens after updating the connection with the new organization ID.
   */
  private AccessToken updateCCloudOrganization(
      AccessToken accessTokens,
      ConnectionSpec newSpec,
      String ccloudOrganizationName
  ) {
    // Refresh token exchange
    var refreshedTokens = ccloudTestUtil.expectRefreshTokenExchangeRequest(
        accessTokens.refresh_token()
    );

    // Wiremock routes to handle CCloud auth for the provided organization ID
    var controlPlaneToken = ccloudTestUtil.expectControlPlaneAndDataPlaneTokenExchangeRequest(
        refreshedTokens.id_token(),
        ccloudOrganizationName,
        newSpec.ccloudOrganizationId()
    );

    // Expect check auth status requests with the refreshed tokens
    ccloudTestUtil.expectCheckJwtRequest(controlPlaneToken.token());

    // Update connection with organization ID -- this should trigger a refresh of auth tokens
    // to use the new organization ID
    given()
        .contentType(ContentType.JSON)
        .body(newSpec)
        .when().put("/gateway/v1/connections/{id}", newSpec.id())
        .then()
        .statusCode(200);

    // Get the connection to verify the updated organization ID
    given()
        .contentType(ContentType.JSON)
        .when().get("/gateway/v1/connections/{id}", newSpec.id())
        .then()
        .statusCode(200)
        .body("spec.ccloud_config.organization_id", is(newSpec.ccloudOrganizationId()));

    return refreshedTokens;
  }

  private void assertCurrentOrganizationForConnection(
      String expectedOrgName,
      String expectedOrgId
  ) {
    expectListOrganizations();

    var listOrgsQuery = """
        query {
          ccloudConnectionById (id: "c1") {
            organizations {
              name
              id
              current
            }
          }
        }
        """;

    // Should be able to list orgs and see the updated org as current
    QueryResourceUtil.queryGraphQLRaw(listOrgsQuery)
        .rootPath("data.ccloudConnectionById.organizations")
        .body("find { it.current == true }.name", is(expectedOrgName))
        .body("find { it.current == true }.id", is(expectedOrgId));
  }

  private void expectListOrganizations() {
    ccloudTestUtil.expectSuccessfulCCloudGet(
        orgListUri,
        ccloudTestUtil.getControlPlaneToken("c1"),
        "ccloud-resources-mock-responses/list-organizations.json"
    );
  }


  @Test
  @TestHTTPEndpoint(ConnectionsResource.class)
  void updateConnection_FailUpdateNonExistingConnection() throws IOException {
    ConnectionSpec spec = new ConnectionSpec(
        "1",
        "Connection 1",
        ConnectionType.LOCAL
    );

    Mockito.when(uuidFactory.getRandomUuid()).thenReturn("99a2b4ce-7a87-4dd2-b967-fe9f34fcbea4");

    var actualResponse = given()
        .contentType(ContentType.JSON)
        .body(spec)
        .when().put("/{id}", "1")
        .then()
        .statusCode(404)
        .contentType(ContentType.JSON)
        .extract().body().asString();

    JsonNode actual = asJson(actualResponse);
    JsonNode expected = asJson(
        loadResource("connections/update-non-existing-connection-response.json")
    );

    assertEquals(expected, actual);
  }

  @Test
  @TestHTTPEndpoint(ConnectionsResource.class)
  void deleteConnection_shouldDeleteAndSecondDeleteFails() throws IOException {
    // Two scenarios are tested here for delete connection.
    // 1. Deletion of existing connection
    // 2. Deletion of non-existent connection.
    String requestBodyPath = "connections/create-connection-request.json";

    var requestBody = new String(
        Objects.requireNonNull(Thread.currentThread().getContextClassLoader()
            .getResourceAsStream(requestBodyPath)).readAllBytes());

    var actualResponse = given()
        .contentType(ContentType.JSON)
        .body(requestBody)
        .when().post()
        .then()
        .statusCode(200)
        .contentType(ContentType.JSON)
        .extract().body().asString();

    given()
        .when().delete("/{id}", "3")
        .then()
        .statusCode(204); // Expecting HTTP 204 status code for no content

    given().when()
        .get("/{id}", "3")
        .then()
        .statusCode(404); // Expection HTTP 404 not found.

    given()
        .when().delete("/{id}", "3")
        .then()
        .statusCode(404); // Expecting HTTP 404 status code for no content
  }

}

