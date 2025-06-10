package io.confluent.idesidecar.restapi.resources;

import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.asJson;
import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.asObject;
import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.loadResource;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.client.WireMock;
import io.confluent.idesidecar.restapi.connections.ConnectionStateManager;
import io.confluent.idesidecar.restapi.credentials.ApiKeyAndSecret;
import io.confluent.idesidecar.restapi.credentials.ApiSecret;
import io.confluent.idesidecar.restapi.credentials.BasicCredentials;
import io.confluent.idesidecar.restapi.credentials.Password;
import io.confluent.idesidecar.restapi.exceptions.Failure;
import io.confluent.idesidecar.restapi.exceptions.Failure.Error;
import io.confluent.idesidecar.restapi.models.CollectionMetadata;
import io.confluent.idesidecar.restapi.models.Connection;
import io.confluent.idesidecar.restapi.models.ConnectionMetadata;
import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.CCloudConfig;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType;
import io.confluent.idesidecar.restapi.models.ConnectionSpecBuilder;
import io.confluent.idesidecar.restapi.models.ConnectionSpecSchemaRegistryConfigBuilder;
import io.confluent.idesidecar.restapi.models.ConnectionStatus;
import io.confluent.idesidecar.restapi.models.ConnectionStatus.Authentication.Status;
import io.confluent.idesidecar.restapi.models.ConnectionStatus.ConnectedState;
import io.confluent.idesidecar.restapi.models.ConnectionsList;
import io.confluent.idesidecar.restapi.models.ObjectMetadata;
import io.confluent.idesidecar.restapi.testutil.NoAccessFilterProfile;
import io.confluent.idesidecar.restapi.testutil.QueryResourceUtil;
import io.confluent.idesidecar.restapi.util.CCloudTestUtil;
import io.confluent.idesidecar.restapi.util.CCloudTestUtil.AccessToken;
import io.confluent.idesidecar.restapi.util.UuidFactory;
import io.quarkiverse.wiremock.devservice.ConnectWireMock;
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.http.TestHTTPEndpoint;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.restassured.parsing.Parser;
import io.restassured.response.ValidatableResponse;
import io.vertx.junit5.VertxTestContext;
import io.vertx.junit5.VertxTestContext.ExecutionBlock;
import jakarta.inject.Inject;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.mockito.Mockito;

@QuarkusTest
@ConnectWireMock
@TestProfile(NoAccessFilterProfile.class)
public class ConnectionsResourceTest {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

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
    RestAssured.defaultParser = Parser.JSON;
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
  void listConnections_emptyListResponse() {
    var expectedContent = loadResource("connections/empty-list-connections-response.json");
    JsonNode expectedJson = asJson(expectedContent);

    var actualResponse = given()
        .when()
        .get()
        .then()
        .statusCode(200)
        .contentType(ContentType.JSON);

    JsonNode actualJson = asJson(actualResponse.extract().body().asString());
    assertConnectionList(expectedJson, actualJson, 0);

    ConnectionsList actualList = actualResponse.extract().body().as(ConnectionsList.class);
    ConnectionsList expectedList = asObject(expectedContent, ConnectionsList.class);
    assertConnectionList(expectedList, actualList);
  }

  @Test
  @TestHTTPEndpoint(ConnectionsResource.class)
  void createConnection_createsAndReturnsConnection() {
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
    assertConnection(expectedJson, actualJson);

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
  void testLocalConnectionDryRun_returnsConnectionWithoutCreatingConnection() {
    var requestSpec = asObject(
        loadResource("connections/create-connection-request.json"),
        ConnectionSpec.class
    );

    for (int i = 0; i < 2; i++) {
      var actualResponse = given()
          .contentType(ContentType.JSON)
          .queryParam("dry_run", true)
          .body(requestSpec)
          .when().post()
          .then()
          .statusCode(200)
          .contentType(ContentType.JSON)
          .body("api_version", equalTo("gateway/v1"))
          .body("kind", equalTo("Connection"))
          .body("metadata.self", startsWith("http://localhost:26637/gateway/v1/connections/"))
          .body("metadata.resource_name", nullValue())
          .body("id", equalTo(requestSpec.id()))
          .body("spec.id", equalTo(requestSpec.id()))
          .body("spec.name", equalTo(requestSpec.name()))
          .body("spec.type", equalTo(ConnectionType.LOCAL.name()))
          .body("status.authentication.status", equalTo(Status.NO_TOKEN.name()));

      // Verify it can be deserialized
      var testedConnection = actualResponse.extract().as(Connection.class);
      assertNotNull(testedConnection);

      // And the tested connection does not really exist
      given()
          .contentType(ContentType.JSON)
          .when().get("/{id}", testedConnection.id())
          .then()
          .statusCode(404);
    }
  }

  @Test
  @TestHTTPEndpoint(ConnectionsResource.class)
  void testLocalConnectionDryRunWithoutId_returnsConnectionWithoutCreatingConnection() {
    var requestSpec = asObject(
        loadResource("connections/create-connection-request.json"),
        ConnectionSpec.class
    ).withId(null); // remove the ID

    // Expect the ID generator to be called (when generating the ID only for validation).
    // This will not be included in the response.
    var generatedId = "99a2b4ce-7a87-4dd2-b967-fe9f34fcbea4";
    Mockito.when(uuidFactory.getRandomUuid()).thenReturn(generatedId);
    var actualResponse = given()
        .contentType(ContentType.JSON)
        .queryParam("dry_run", true)
        .body(requestSpec)
        .when().post()
        .then()
        .statusCode(200)
        .contentType(ContentType.JSON)
        .body("api_version", equalTo("gateway/v1"))
        .body("kind", equalTo("Connection"))
        .body("metadata.self", startsWith("http://localhost:26637/gateway/v1/connections/"))
        .body("metadata.resource_name", nullValue())
        .body("id", nullValue())
        .body("spec.id", nullValue())
        .body("spec.name", equalTo(requestSpec.name()))
        .body("spec.type", equalTo(ConnectionType.LOCAL.name()))
        .body("status.authentication.status", equalTo(Status.NO_TOKEN.name()));

    // Verify it can be deserialized
    var testedConnection = actualResponse.extract().as(Connection.class);
    assertNotNull(testedConnection);
  }

  @Test
  @TestHTTPEndpoint(ConnectionsResource.class)
  void createCCloudConnectionDryRun_returnsConnectionWithoutCreatingConnection() {
    var requestSpec = new ConnectionSpec("c-1", "Connection 1", ConnectionType.CCLOUD);

    for (int i = 0; i < 2; i++) {
      var actualResponse = given()
          .contentType(ContentType.JSON)
          .queryParam("dry_run", true)
          .body(requestSpec)
          .when().post()
          .then()
          .statusCode(200)
          .contentType(ContentType.JSON)
          .body("api_version", equalTo("gateway/v1"))
          .body("kind", equalTo("Connection"))
          .body("metadata.self", startsWith("http://localhost:26637/gateway/v1/connections/"))
          .body("metadata.resource_name", nullValue())
          .body("metadata.sign_in_uri", startsWith(
              "https://login.confluent.io/oauth/authorize?response_type=code&code_challenge_method="))
          .body("id", equalTo(requestSpec.id()))
          .body("spec.id", equalTo(requestSpec.id()))
          .body("spec.name", equalTo(requestSpec.name()))
          .body("spec.type", equalTo(ConnectionType.CCLOUD.name()))
          .body("status.authentication.status", equalTo(Status.NO_TOKEN.name()))
          .body("status.ccloud.state", equalTo(ConnectedState.NONE.name()));

      // Verify it can be deserialized
      var testedConnection = actualResponse.extract().as(Connection.class);
      assertNotNull(testedConnection);

      // And the tested connection does not really exist
      given()
          .contentType(ContentType.JSON)
          .when().get("/{id}", "c-1")
          .then()
          .statusCode(404);
    }
  }

  @Test
  @TestHTTPEndpoint(ConnectionsResource.class)
  void getConnection_withoutToken_shouldReturnWithStatus() {
    List<ConnectionSpec> specs = List.of(
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
    ConnectionStatus expectedStatus = ConnectionStatus.INITIAL_CCLOUD_STATUS;
    JsonNode expectedStatusAsJson = asJson(expectedStatus);
    assertTrue(treeActual.has("status"));
    assertEquals(expectedStatusAsJson, treeActual.get("status"));

    // Status should include neither user nor organization if token is absent
    assertFalse(treeActual.get("status").get("authentication").has("user"));
    assertFalse(treeActual.get("status").get("authentication").has("organization"));
  }

  @Test
  void getConnection_withToken_shouldReturnWithStatusIncludingUserAndOrg() {
    var connectionId = "c-1";
    var connectionName = "Connection 1";
    var connectionType = ConnectionType.CCLOUD;

    // Create authenticated connection
    ccloudTestUtil.createAuthedConnection(connectionId, connectionName, connectionType);

    var testContext = new VertxTestContext();
    refreshConnectionStatusAndThen(connectionId, testContext, () -> {
      // Get Connection
      var actualGetConnection = given()
          .contentType(ContentType.JSON)
          .when().get("/gateway/v1/connections/{id}", connectionId)
          .then()
          .statusCode(200)
          .contentType(ContentType.JSON)
          .extract().body().asString();
      var treeActual = asJson(actualGetConnection);
      var expectedSpec = new ConnectionSpec(
          connectionId,
          "Connection 1",
          ConnectionType.CCLOUD
      );
      var expectedSpecAsJson = asJson(expectedSpec);
      // Verify ConnectionSpec
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
      testContext.completeNow();
    });
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

    var testContext = new VertxTestContext();
    refreshConnectionStatusAndThen(connectionId, testContext, () -> {
      // Get Connection
      var connection = given()
          .contentType(ContentType.JSON)
          .when().get("/gateway/v1/connections/{id}", connectionId)
          .then()
          .statusCode(200)
          .contentType(ContentType.JSON)
          .extract().body().asString();

      // Verify ConnectionSpec
      var connectionAsJson = asJson(connection);

      // Verify ConnectionStatus
      assertTrue(connectionAsJson.has("status"));
      assertTrue(connectionAsJson.get("status").has("authentication"));
      var authenticationStatus = connectionAsJson.get("status").get("authentication");

      // Verify that token is valid
      assertEquals(
          Status.INVALID_TOKEN.name(),
          authenticationStatus.get("status").textValue()
      );

      // Verify that error related to auth status check is present
      assertTrue(authenticationStatus.has("errors"));
      assertTrue(authenticationStatus.get("errors").has("auth_status_check"));
      testContext.completeNow();
    });
  }

  @Test
  @TestHTTPEndpoint(ConnectionsResource.class)
  void listConnections_returnsAllConnections() {
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
        .contentType(ContentType.JSON);

    // should return both resources
    var actualJson = asJson(
        actualResponse1.extract().body().asString()
    );
    var expectedContent = loadResource("connections/list-connections-response.json");
    var expectedJson = asJson(expectedContent);

    assertNotNull(actualJson);
    assertNotNull(expectedJson);
    assertConnectionList(expectedJson, actualJson, 2);

    var actualList = actualResponse1.extract().body().as(ConnectionsList.class);
    var expectedList = asObject(expectedContent, ConnectionsList.class);
    assertConnectionList(expectedList, actualList);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("partialPayloadParams")
  void updateConnectionViaPatchWithPartialPayloadsShouldSucceed(
      String testName,
      ConnectionSpec payload,
      int expectedStatusCode) throws Exception {

    // Create authenticated connection
    var connectionId = "c1";
    var connectionName = "Connection 1";
    var orgName = "Development Org";
    var accessTokens = ccloudTestUtil.createAuthedCCloudConnection(
        connectionId,
        connectionName,
        orgName,
        null
    );
    var connectionSpec = connectionStateManager.getConnectionSpec("c1");
    var testUpdatesConnectionType =
        payload.type() != null && !connectionSpec.type().equals(payload.type());

    var testContext = new VertxTestContext();
    refreshConnectionStatusAndThen(connectionId, testContext, () -> {
      assertAuthStatus(connectionId, "VALID_TOKEN");

      var mapper = new ObjectMapper();
      var patchRequest = mapper.writeValueAsString(payload);
      var response = given()
          .contentType(ContentType.JSON)
          .body(patchRequest)
          .when()
          .log().all()
          .patch("/gateway/v1/connections/{id}", connectionId)
          .then()
          .log().all()
          .statusCode(expectedStatusCode);

      if (expectedStatusCode == 200 && testUpdatesConnectionType) {
        var currentConnection = given()
            .when()
            .get("/gateway/v1/connections/{id}", connectionId)
            .then()
            .extract()
            .response();

        // Verify type hasn't changed if testing type change
        var afterConnection = given()
            .when()
            .get("/gateway/v1/connections/{id}", connectionId)
            .then()
            .extract()
            .response();
      }
      testContext.completeNow();
    });
  }

  private static Stream<Arguments> partialPayloadParams() {
    return Stream.of(
        Arguments.of(
            "Change Type",
            new ConnectionSpec(
                "c1",
                "Connection 1",
                ConnectionType.PLATFORM
            ),
            200
        ),
        Arguments.of(
            "Name Only",
            new ConnectionSpec(
                null,
                "New Connection name",
                null
            ),
            200
        ),
        Arguments.of(
            "ID and Name",
            new ConnectionSpec(
                "c1",
                "New Connection name",
                null
            ),
            200
        ),
        // Negative test cases
        Arguments.of(
            "Invalid Type",
            new ConnectionSpec(
                "c1",
                "Connection 1",
                null
            ),
            400
        ),
        Arguments.of(
            "Empty Name",
            new ConnectionSpec(
                "c1",
                "",
                ConnectionType.PLATFORM
            ),
            400
        ),
        Arguments.of(
            "Null ID",
            new ConnectionSpec(
                null,
                "Connection 1",
                ConnectionType.PLATFORM
            ),
            400
        )
    );
  }

  @ParameterizedTest
  @NullAndEmptySource
  void updateConnectionWithNullOrEmptyIdShouldFail(String id) {
    var originalId = "c1";
    var connectionSpec = ccloudTestUtil.createConnection(
        originalId, "Connection 1", ConnectionType.LOCAL);

    var badSpec = connectionSpec.withId(id);
    var response = given()
        .contentType(ContentType.JSON)
        .body(badSpec)
        .when().put("/gateway/v1/connections/{id}", originalId)
        .then()
        .statusCode(400);
    assertResponseMatches(
        response,
        createError().withSource("id").withDetail("Connection ID is required and may not be blank")
    );
  }

  @ParameterizedTest
  @NullAndEmptySource
  void updateConnectionWithNullOrEmptyNameShouldFail(String name) {
    var originalName = "Connection 1";
    var connectionSpec = ccloudTestUtil.createConnection(
        "c1", originalName, ConnectionType.LOCAL);

    var badSpec = connectionSpec.withName(name);
    var response = given()
        .contentType(ContentType.JSON)
        .body(badSpec)
        .when().put("/gateway/v1/connections/{id}", "c1")
        .then()
        .statusCode(400);
    assertResponseMatches(
        response,
        createError().withSource("name")
            .withDetail("Connection name is required and may not be blank")
    );
  }

  @Test
  void updateConnectionWithTypeChangedShouldFail() {
    ccloudTestUtil.createConnection("c1", "Connection 1", ConnectionType.LOCAL);

    var badSpec = new ConnectionSpec("c1", "Connection 1", ConnectionType.CCLOUD);
    var response = given()
        .contentType(ContentType.JSON)
        .body(badSpec)
        .when().put("/gateway/v1/connections/{id}", "c1")
        .then()
        .statusCode(400);
    assertResponseMatches(
        response,
        createError().withSource("type").withDetail("Connection type may not be changed")
    );
  }

  @Test
  void updateConnectionWithMultipleValidationErrors() {
    ccloudTestUtil.createConnection("c1", "Connection 1", ConnectionType.LOCAL);

    // This connection spec is not valid
    var badSpec = ConnectionSpecBuilder
        .builder()
        .id("c3")
        .name("Connection name changed!")
        .type(ConnectionType.PLATFORM)
        .ccloudConfig(new CCloudConfig("org-id", null))
        .build();

    var response = given()
        .contentType(ContentType.JSON)
        .body(badSpec)
        .when().put("/gateway/v1/connections/{id}", "c1")
        .then()
        .statusCode(400);
    assertResponseMatches(
        response,
        createError().withSource("id").withDetail("Connection ID may not be changed"),
        createError().withSource("type").withDetail("Connection type may not be changed")
    );
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
    var response = given()
        .contentType(ContentType.JSON)
        .body(requestBody)
        .when().put("/gateway/v1/connections/{id}", "c1")
        .then()
        .statusCode(400);
    assertResponseMatches(
        response,
        createError().withSource("ccloud_config")
            .withDetail("CCloud configuration is not allowed when type is LOCAL")
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
    accessTokens = updateCCloudConfig(
        accessTokens,
        connectionSpec.withCCloudConfig(
            new CCloudConfig("d6fc52f8-ae8a-405c-9692-e997965b730dc", null)),
        "Test Org"
    );

    assertCurrentOrganizationForConnection(
        "Test Org",
        "d6fc52f8-ae8a-405c-9692-e997965b730dc"
    );

    // Update org to Staging Org and expect the current org to be updated
    accessTokens = updateCCloudConfig(
        accessTokens,
        connectionSpec.withCCloudConfig(
            new CCloudConfig("1a507773-d2cb-4055-917e-ffb205f3c433", null)),
        "Staging Org"
    );

    assertCurrentOrganizationForConnection(
        "Staging Org",
        "1a507773-d2cb-4055-917e-ffb205f3c433"
    );

    updateCCloudConfig(
        accessTokens,
        connectionSpec.withCCloudConfig(
            new CCloudConfig("23b1185e-d874-4f61-81d6-c9c61aa8969c", null)),
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
    var connectionId = "c1";
    var connectionName = "Connection 1";
    var orgName = "Development Org";
    var accessTokens = ccloudTestUtil.createAuthedCCloudConnection(
        connectionId,
        connectionName,
        orgName,
        null
    );
    var connectionSpec = connectionStateManager.getConnectionSpec("c1");

    var testContext = new VertxTestContext();
    refreshConnectionStatusAndThen(connectionId, testContext, () -> {
      assertAuthStatus(connectionId, "VALID_TOKEN");

      assertCurrentOrganizationForConnection(
          orgName,
          "23b1185e-d874-4f61-81d6-c9c61aa8969c"
      );

      // Update org to Development Org with the same ID and expect the current org to
      // remain the same
      updateCCloudConfig(
          accessTokens,
          connectionSpec,
          orgName
      );

      assertCurrentOrganizationForConnection(
          orgName,
          "23b1185e-d874-4f61-81d6-c9c61aa8969c"
      );

      // Expect tokens to have been refreshed and is still valid
      assertAuthStatus(connectionId, "VALID_TOKEN");
      testContext.completeNow();
    });
  }

  @Test
  void updateUnauthedConnectionCCloudOrganization() {
    // Create unauthenticated CCloud connection
    var connectionId = "c1";
    var connectionSpec = ccloudTestUtil.createConnection(
        connectionId, "Connection 1", ConnectionType.CCLOUD);

    var testContext = new VertxTestContext();
    refreshConnectionStatusAndThen(connectionId, testContext, () -> {
      given()
          .contentType(ContentType.JSON)
          .body(connectionSpec.withCCloudConfig(
              new CCloudConfig("d6fc52f8-ae8a-405c-9692-e997965b730dc", null)))
          .when().put("/gateway/v1/connections/{id}", connectionSpec.id())
          .then()
          .statusCode(400)
          .body("errors.size()", is(1))
          .body("errors[0].title", is("Could not authenticate"));

      // The above update should have triggered a failed auth refresh, which is fine
      assertAuthStatus(connectionId, "NO_TOKEN")
          // Validate that we don't update the connection state due to failed refresh
          .body("status.authentication.errors.token_refresh", is(nullValue()));

      testContext.completeNow();
    });

    // Let's authenticate the connection now
    ccloudTestUtil.authenticateCCloudConnection(
        connectionId,
        "Development Org",
        null
    );
    refreshConnectionStatusAndThen(connectionId, testContext, () -> {
      assertAuthStatus(connectionId, "VALID_TOKEN");
      testContext.completeNow();
    });
  }


  @Test
  void updateConnectionCCloudOrganizationToNonExistentOrg() {
    // Start with default org
    var connectionId = "c1";
    var accessTokens = ccloudTestUtil.createAuthedCCloudConnection(
        connectionId,
        "Connection 1",
        "Development Org",
        null
    );

    var testContext = new VertxTestContext();
    refreshConnectionStatusAndThen(connectionId, testContext, () -> {
      assertAuthStatus(connectionId, "VALID_TOKEN")
          .body("spec.ccloud_config.organization_id", is(nullValue()));
      var refreshedTokens = ccloudTestUtil.expectRefreshTokenExchangeRequest(
          accessTokens.refresh_token()
      );

      ccloudTestUtil.expectInvalidResourceIdOnControlPlaneTokenExchange(
          refreshedTokens.id_token(),
          "non-existent-org-id"
      );

      expectListOrganizations();

      var connectionSpec = connectionStateManager.getConnectionSpec(connectionId);
      given()
          .contentType(ContentType.JSON)
          .body(connectionSpec.withCCloudConfig(new CCloudConfig("non-existent-org-id", null)))
          .when().put("/gateway/v1/connections/{id}", connectionSpec.id())
          .then()
          .statusCode(400)
          .body("errors.size()", is(1))
          .body("errors[0].title", is("Invalid organization ID"));

      testContext.completeNow();
    });
  }

  @Test
  void updateConnectionCCloudOrganizationToDefault() {
    // Start with non-default org
    var connectionId = "c1";
    var accessTokens = ccloudTestUtil.createAuthedCCloudConnection(
        connectionId,
        "Connection 1",
        "Staging Org",
        "1a507773-d2cb-4055-917e-ffb205f3c433"
    );

    var testContext = new VertxTestContext();
    refreshConnectionStatusAndThen(connectionId, testContext, () -> {
      assertAuthStatus("c1", "VALID_TOKEN")
          .body("spec.ccloud_config.organization_id", is("1a507773-d2cb-4055-917e-ffb205f3c433"));
      testContext.completeNow();
    });

    var connectionSpec = connectionStateManager.getConnectionSpec(connectionId);
    updateCCloudConfig(
        accessTokens,
        // Set ccloud_config to null
        new ConnectionSpec(connectionSpec.id(), connectionSpec.name(), connectionSpec.type()),
        "Development Org"
    );
    refreshConnectionStatusAndThen(connectionId, testContext, () -> {
      assertAuthStatus("c1", "VALID_TOKEN")
          .body("spec.ccloud_config.organization_id", is(nullValue()));

      assertCurrentOrganizationForConnection(
          "Development Org",
          "23b1185e-d874-4f61-81d6-c9c61aa8969c"
      );
      testContext.completeNow();
    });
  }

  @Test
  void updateConnectionCCloudIdeAuthCallbackUri() {
    // Create authenticated CCloud connection
    var connectionId = "c1";
    var accessTokens = ccloudTestUtil.createAuthedCCloudConnection(connectionId, "Connection 1");
    var connectionSpec = connectionStateManager.getConnectionSpec(connectionId);

    // By default, the CCloudConfig is null
    assertNull(connectionSpec.ccloudConfig());

    // Update the connection's ideAuthCallbackUri
    var ideAuthCallbackUri = "vscode://confluentinc.vscode-confluent/my-custom-auth-callback-uri";
    accessTokens = updateCCloudConfig(
        accessTokens,
        connectionSpec.withCCloudConfig(new CCloudConfig(null, ideAuthCallbackUri)),
        "default"
    );

    // The ideAuthCallbackUri should be updated and the org ID should be null
    var updatedConnectionSpec = connectionStateManager.getConnectionSpec(connectionId);
    assertEquals(ideAuthCallbackUri, updatedConnectionSpec.ccloudConfig().ideAuthCallbackUri());
    assertNull(updatedConnectionSpec.ccloudConfig().organizationId());

    // We should be able to set it to null
    updateCCloudConfig(
        accessTokens,
        connectionSpec.withCCloudConfig(new CCloudConfig(null, null)),
        "default"
    );
    updatedConnectionSpec = connectionStateManager.getConnectionSpec(connectionId);
    assertNull(updatedConnectionSpec.ccloudConfig().ideAuthCallbackUri());
    assertNull(updatedConnectionSpec.ccloudConfig().organizationId());
  }

  private Error createError() {
    return Error.create();
  }

  @TestFactory
  Stream<DynamicTest> shouldAllowCreateWithValidSpecsOrFailWithInvalidSpecs() {
    record TestInput(
        String displayName,
        String specJson,
        Error... expectedErrors
    ) {

    }
    var inputs = List.of(
        new TestInput(
            "Direct spec is valid with name and Kafka w/ scram credentials for 512 and no Schema Registry",
            """
                {
                  "name": "Some connection name",
                  "type": "DIRECT",
                  "kafka_cluster": {
                    "bootstrap_servers": "localhost:9092",
                    "credentials": {
                      "type": "SCRAM",
                      "hash_algorithm": "SCRAM_SHA_512",
                      "scram_username": "user",
                      "scram_password": "pass"
                    },
                    "ssl": { "enabled": true }
                  }
                }
                """
        ),
        new TestInput(
            "Direct spec is valid with name and Kafka w/ scram credentials for 256 and no Schema Registry",
            """
                {
                  "name": "Some connection name",
                  "type": "DIRECT",
                  "kafka_cluster": {
                    "bootstrap_servers": "localhost:9092",
                    "credentials": {
                      "type": "SCRAM",
                      "hash_algorithm": "SCRAM_SHA_256",
                      "scram_username": "user",
                      "scram_password": "pass"
                    },
                    "ssl": { "enabled": true }
                  }
                }
                """
        ),
        new TestInput(
            "Direct spec is invalid with name and Kafka w/ scram credentials for 256 without username",
            """
                    {
                        "name": "string",
                        "type": "DIRECT",
                        "kafka_cluster": {
                            "bootstrap_servers": "localhost:9092",
                            "credentials": {
                                "type": "SCRAM",
                                "hash_algorithm": "SCRAM_SHA_256",
                                "scram_password" : "pass"
                
                            },
                            "ssl": {
                                "enabled": true
                            }
                        },
                        "ssl": {
                            "enabled": true
                        }
                    }
                """,
            createError()
                .withSource("kafka_cluster.credentials.scram_username")
                .withDetail("Kafka cluster Username is required and may not be blank")
        ),
        new TestInput(
            "Direct spec is invalid with name and Kafka w/ scram credentials for 256 without hash algorithm and no Schema Registry",
            """
                    {
                        "name": "string",
                        "type": "DIRECT",
                        "kafka_cluster": {
                            "bootstrap_servers": "localhost:9092",
                            "credentials": {
                                "type": "SCRAM",
                                "scram_password" : "pass",
                                "scram_username" : "user"
                            },
                            "ssl": {
                                "enabled": true
                            }
                        },
                        "ssl": {
                            "enabled": true
                        }
                    }
                """,
            createError()
                .withSource("kafka_cluster.credentials.hash_algorithm")
                .withDetail(
                    "Kafka cluster Hash algorithm is required, may not be blank, and must be one of the supported algorithms (SCRAM_SHA_256 or SCRAM_SHA_512)")
        ),
        // Local connections
        new TestInput(
            "Local spec is valid with name but no config",
            """
                {
                  "name": "Some connection name",
                  "type": "LOCAL"
                }
                """
        ),
        new TestInput(
            "Local spec is valid with empty local config",
            """
                {
                  "name": "Connection 1",
                  "type": "LOCAL",
                  "local_config": {
                  }
                }
                """
        ),
        new TestInput(
            "Local spec is valid with valid Schema Registry URI",
            """
                {
                  "name": "Connection 1",
                  "type": "LOCAL",
                  "local_config": {
                    "schema-registry-uri": "http://localhost:8081"
                  }
                }
                """
        ),
        new TestInput(
            "Local spec is valid with new Schema Registry config but without credentials",
            """
                {
                  "name": "Connection 1",
                  "type": "LOCAL",
                  "schema_registry": {
                    "uri": "http://localhost:8081"
                  }
                }
                """
        ),
        new TestInput(
            "Local spec is valid with new Schema Registry config and null credentials",
            """
                {
                  "name": "Connection 1",
                  "type": "LOCAL",
                  "schema_registry": {
                    "uri": "http://localhost:8081",
                    "credentials": null
                  }
                }
                """
        ),
        new TestInput(
            "Local spec is valid with new Schema Registry config including basic credentials",
            """
                {
                  "name": "Connection 1",
                  "type": "LOCAL",
                  "schema_registry": {
                    "uri": "http://localhost:8081",
                    "credentials": {
                      "type": "BASIC",
                      "username": "user",
                      "password": "pass"
                    }
                  }
                }
                """
        ),
        new TestInput(
            "Local spec is valid with new Schema Registry config including API key and secret credentials",
            """
                {
                  "name": "Connection 1",
                  "type": "LOCAL",
                  "schema_registry": {
                    "uri": "http://localhost:8081",
                    "credentials": {
                      "type": "API_KEY_AND_SECRET",
                      "api_key": "my-api-key",
                      "api_secret": "my-api-secret"
                    }
                  }
                }
                """
        ),
        new TestInput(
            "Local spec is invalid without name",
            """
                {
                  "type": "LOCAL",
                  "local_config": {}
                }
                """,
            createError()
                .withSource("name")
                .withDetail("Connection name is required and may not be blank")
        ),
        new TestInput(
            "Local spec is invalid with blank name",
            """
                {
                  "name": "  ",
                  "type": "LOCAL",
                  "local_config": {}
                }
                """,
            createError()
                .withSource("name")
                .withDetail("Connection name is required and may not be blank")
        ),
        new TestInput(
            "Local spec is invalid with blank Schema Registry URI (old config)",
            """
                {
                  "name": "Connection 1",
                  "type": "LOCAL",
                  "local_config": {
                    "schema-registry-uri": "  "
                  }
                }
                """,
            createError()
                .withSource("local_config.schema-registry-uri")
                .withDetail(
                    "Schema Registry URI may be null (use default local SR) or empty (do not use SR), but may not have only whitespace")
        ),
        new TestInput(
            "Local spec is invalid with null Schema Registry URI (new config)",
            """
                {
                  "name": "Connection 1",
                  "type": "LOCAL",
                  "schema_registry": {
                    "uri": null
                  }
                }
                """,
            createError()
                .withSource("schema_registry.uri")
                .withDetail("Schema Registry URI is required and may not be blank")
        ),
        new TestInput(
            // If the URI is empty, then we assume that the user does not want to use SR
            // If the URI is null, then we assume the user wants to use SR at the default local port
            "Local spec is invalid with blank Schema Registry URI (new config)",
            """
                {
                  "name": "Connection 1",
                  "type": "LOCAL",
                  "schema_registry": {
                    "uri": "   "
                  }
                }
                """,
            createError()
                .withSource("schema_registry.uri")
                .withDetail("Schema Registry URI is required and may not be blank")
        ),
        new TestInput(
            "Local spec is invalid with null Schema Registry URI",
            """
                {
                  "name": "Connection 1",
                  "type": "LOCAL",
                  "schema_registry": {
                    "uri": null
                  }
                }
                """,
            createError()
                .withSource("schema_registry.uri")
                .withDetail("Schema Registry URI is required and may not be blank")
        ),
        new TestInput(
            "Local spec is invalid with both local config and Schema Registry",
            """
                {
                  "name": "Connection 1",
                  "type": "LOCAL",
                  "local_config": {
                    "schema-registry-uri": "http://localhost:8081"
                  },
                  "schema_registry": {
                    "uri": "http://localhost:8081"
                  }
                }
                """,
            createError()
                .withSource("local_config.schema-registry-uri")
                .withDetail("Local config cannot be used with schema_registry configuration")
        ),
        new TestInput(
            "Local spec is invalid with new Schema Registry config and incomplete basic credentials",
            """
                {
                  "name": "Connection 1",
                  "type": "LOCAL",
                  "schema_registry": {
                    "uri": "http://localhost:8081",
                    "credentials": {
                      "type": "BASIC",
                      "username": "user"
                    }
                  }
                }
                """,
            createError()
                .withSource("schema_registry.credentials.password")
                .withDetail("Schema Registry password is required")
        ),
        new TestInput(
            "Local spec is invalid with malformed Schema Registry URI (old config)",
            """
                {
                  "name": "Connection 1",
                  "type": "LOCAL",
                  "local_config": {
                    "schema-registry-uri": "this-is-not a valid URI"
                  }
                }
                """,
            createError()
                .withSource("local_config.schema-registry-uri")
                .withDetail("Schema Registry URI is not a valid URI")
        ),
        new TestInput(
            "Local spec is invalid with malformed Schema Registry URI (new config)",
            """
                {
                  "name": "Connection 1",
                  "type": "LOCAL",
                  "schema_registry": {
                    "uri": "this-is-not a valid URI"
                  }
                }
                """,
            createError()
                .withSource("schema_registry.uri")
                .withDetail("Schema Registry URI is not a valid URI")
        ),
        new TestInput(
            "Local spec is invalid with file Schema Registry URI (old config)",
            """
                {
                  "name": "Connection 1",
                  "type": "LOCAL",
                  "local_config": {
                    "schema-registry-uri": "file://path/to/non/existent/file.txt"
                  }
                }
                """,
            createError()
                .withSource("local_config.schema-registry-uri")
                .withDetail("Schema Registry URI must use 'http' or 'https'")
        ),
        new TestInput(
            "Local spec is invalid with file Schema Registry URI (new config)",
            """
                {
                  "name": "Connection 1",
                  "type": "LOCAL",
                  "schema_registry": {
                    "uri": "file://path/to/non/existent/file.txt"
                  }
                }
                """,
            createError()
                .withSource("schema_registry.uri")
                .withDetail("Schema Registry URI must use 'http' or 'https'")
        ),
        new TestInput(
            "Local spec is invalid with CCloud spec",
            """
                {
                  "name": "Connection 1",
                  "type": "LOCAL",
                  "ccloud_config": {
                    "organization_id": "this-wont-work"
                  }
                }
                """,
            createError()
                .withSource("ccloud_config")
                .withDetail("CCloud configuration is not allowed when type is LOCAL")
        ),
        new TestInput(
            "Local spec is invalid with Kafka spec",
            """
                {
                  "name": "Connection 1",
                  "type": "LOCAL",
                  "kafka_cluster": {
                  }
                }
                """,
            createError()
                .withSource("kafka_cluster")
                .withDetail("Kafka cluster configuration is not allowed when type is LOCAL")
        ),
        // CCloud connections
        new TestInput(
            "CCloud spec is valid with name and no config",
            """
                {
                  "name": "Some connection name",
                  "type": "CCLOUD"
                }
                """
        ),
        new TestInput(
            "CCloud spec is valid with name and CCloud config w/ null organization",
            """
                {
                  "name": "Some connection name",
                  "type": "CCLOUD",
                  "ccloud_config": {
                    "organization_id": null
                  }
                }
                """
        ),
        new TestInput(
            "CCloud spec is valid with name and CCloud config w/ blank organization",
            """
                {
                  "name": "Some connection name",
                  "type": "CCLOUD",
                  "ccloud_config": {
                    "organization_id": ""
                  }
                }
                """
        ),
        new TestInput(
            "CCloud spec is valid with name and CCloud config w/ organization",
            """
                {
                  "name": "Some connection name",
                  "type": "CCLOUD",
                  "ccloud_config": {
                    "organization_id": "12345"
                  }
                }
                """
        ),
        new TestInput(
            "CCloud spec is invalid without name",
            """
                {
                  "type": "CCLOUD",
                  "ccloud_config": {
                    "organization_id": "12345"
                  }
                }
                """,
            createError()
                .withSource("name")
                .withDetail("Connection name is required and may not be blank")
        ),
        new TestInput(
            "CCloud spec is invalid with blank name",
            """
                {
                  "name": "  ",
                  "type": "CCLOUD",
                  "ccloud_config": {
                    "organization_id": "12345"
                  }
                }
                """,
            createError()
                .withSource("name")
                .withDetail("Connection name is required and may not be blank")
        ),
        new TestInput(
            "CCloud spec is invalid with local config",
            """
                {
                  "name": "Connection 1",
                  "type": "CCLOUD",
                  "local_config": {
                  }
                }
                """,
            createError()
                .withSource("local_config")
                .withDetail("Local configuration is not allowed when type is CCLOUD")
        ),
        new TestInput(
            "CCloud spec is invalid with Kafka spec",
            """
                {
                  "name": "Connection 1",
                  "type": "CCLOUD",
                  "kafka_cluster": {
                  }
                }
                """,
            createError()
                .withSource("kafka_cluster")
                .withDetail("Kafka cluster configuration is not allowed when type is CCLOUD")
        ),
        new TestInput(
            "CCloud spec is invalid with Schema Registry spec",
            """
                {
                  "name": "Connection 1",
                  "type": "CCLOUD",
                  "schema_registry": {
                  }
                }
                """,
            createError()
                .withSource("schema_registry")
                .withDetail("Schema Registry configuration is not allowed when type is CCLOUD")
        ),
        // Direct connections
        new TestInput(
            "Direct spec is valid with name and no config",
            """
                {
                  "name": "Some connection name",
                  "type": "DIRECT"
                }
                """
        ),
        new TestInput(
            "Direct spec is valid with name and Kafka and no Schema Registry",
            """
                {
                  "name": "Some connection name",
                  "type": "DIRECT",
                  "kafka_cluster": {
                    "bootstrap_servers": "localhost:9092",
                    "ssl": { "enabled": true }
                  }
                }
                """
        ),
        new TestInput(
            "Direct spec is valid with name and Kafka w/ basic credentials and no Schema Registry",
            """
                {
                  "name": "Some connection name",
                  "type": "DIRECT",
                  "kafka_cluster": {
                    "bootstrap_servers": "localhost:9092",
                    "credentials": {
                      "type": "BASIC",
                      "username": "user",
                      "password": "pass"
                    },
                    "ssl": { "enabled": true }
                  }
                }
                """
        ),
        new TestInput(
            "Direct spec is valid with name and Schema Registry and no Kafka",
            """
                {
                  "name": "Some connection name",
                  "type": "DIRECT",
                  "schema_registry": {
                    "uri": "http://localhost:8081",
                    "ssl": { "enabled": true }
                  }
                }
                """
        ),
        new TestInput(
            "Direct spec is valid with name and Kafka and Schema Registry",
            """
                {
                  "name": "Some connection name",
                  "type": "DIRECT",
                  "kafka_cluster": {
                    "bootstrap_servers": "localhost:9092"
                  },
                  "schema_registry": {
                    "uri": "http://localhost:8081",
                    "ssl": { "enabled": true }
                  }
                }
                """
        ),
        new TestInput(
            "Direct spec is valid with Kafka and verify server certificate hostname",
            """
                {
                  "name": "Some connection name",
                  "type": "DIRECT",
                  "kafka_cluster": {
                    "bootstrap_servers": "localhost:9092",
                    "ssl": { "enabled": true, "verify_hostname": true}
                  }
                }
                """
        ),
        new TestInput(
            "Direct spec is valid with Kafka and don't verify server certificate hostname",
            """
                {
                  "name": "Some connection name",
                  "type": "DIRECT",
                  "kafka_cluster": {
                    "bootstrap_servers": "localhost:9092",
                    "ssl": { "enabled": true, "verify_hostname": false}
                  }
                }
                """
        ),
        new TestInput(
            "Direct spec is valid with Kafka and SR over TLS",
            """
                {
                  "name": "Some connection name",
                  "type": "DIRECT",
                  "kafka_cluster": {
                    "bootstrap_servers": "localhost:9092",
                    "ssl": {
                      "enabled": true,
                      "truststore": {
                        "path": "/path/to/truststore.jks",
                        "password": "truststore-password"
                      }
                    }
                  },
                  "schema_registry": {
                    "uri": "https://localhost:8081"
                  }
                }
                """
        ),
        new TestInput(
            "Direct spec is valid over SSL with truststore path only",
            """
                {
                  "name": "Connection 1",
                  "type": "DIRECT",
                  "kafka_cluster": {
                    "bootstrap_servers": "localhost:9092",
                    "ssl": {
                      "truststore": {
                        "path": "/path/to/truststore.jks"
                      }
                    }
                  }
                }
                """
        ),
        new TestInput(
            "Direct spec is valid with Kafka and SR over mutual TLS",
            """
                {
                  "name": "Some connection name",
                  "type": "DIRECT",
                  "kafka_cluster": {
                    "bootstrap_servers": "localhost:9092",
                    "ssl": {
                      "truststore": {
                        "path": "/path/to/truststore.jks",
                        "password": "truststore-password"
                      },
                      "keystore": {
                        "path": "/path/to/keystore.jks",
                        "password": "keystore-password",
                        "key_password": "key-password"
                      }
                    }
                  },
                  "schema_registry": {
                    "uri": "https://localhost:8081",
                    "ssl": {
                      "truststore": {
                        "path": "/path/to/truststore.jks",
                        "password": "truststore-password"
                      },
                      "keystore": {
                        "path": "/path/to/keystore.jks",
                        "password": "keystore-password",
                        "key_password": "key-password"
                      }
                    }
                  }
                }
                """
        ),
        new TestInput(
            "Direct spec is invalid without name",
            """
                {
                  "type": "DIRECT",
                  "kafka_cluster": {
                    "bootstrap_servers": null
                  }
                }
                """,
            createError()
                .withSource("name")
                .withDetail("Connection name is required and may not be blank"),
            createError()
                .withSource("kafka_cluster.bootstrap_servers")
                .withDetail("Kafka cluster bootstrap_servers is required and may not be blank")
        ),
        new TestInput(
            "Direct spec is invalid with blank name",
            """
                {
                  "name": "  ",
                  "type": "DIRECT",
                  "kafka_cluster": {
                    "bootstrap_servers": null
                  }
                }
                """,
            createError()
                .withSource("name")
                .withDetail("Connection name is required and may not be blank"),
            createError()
                .withSource("kafka_cluster.bootstrap_servers")
                .withDetail("Kafka cluster bootstrap_servers is required and may not be blank")
        ),
        new TestInput(
            "Direct spec is invalid with local config",
            """
                {
                  "name": "Connection 1",
                  "type": "DIRECT",
                  "local_config": {
                  }
                }
                """,
            createError()
                .withSource("local_config")
                .withDetail("Local configuration is not allowed when type is DIRECT")
        ),
        new TestInput(
            "Direct spec is invalid with Kafka spec",
            """
                {
                  "name": "Connection 1",
                  "type": "DIRECT",
                  "ccloud_config": {
                    "organization_id": ""
                  }
                }
                """,
            createError()
                .withSource("ccloud_config")
                .withDetail("CCloud configuration is not allowed when type is DIRECT")
        ),
        new TestInput(
            "Direct spec is invalid with SSL without truststore path",
            """
                {
                  "name": "Connection 1",
                  "type": "DIRECT",
                  "kafka_cluster": {
                    "bootstrap_servers": "localhost:9092",
                    "ssl": {
                      "truststore": {
                        "password": "truststore-password"
                      }
                    }
                  }
                }
                """,
            createError()
                .withSource("kafka_cluster.ssl.truststore.path")
                .withDetail("Kafka cluster truststore path is required and may not be blank")
        ),
        new TestInput(
            "Direct spec is valid with SSL having keystore only",
            """
                {
                  "name": "Connection 1",
                  "type": "DIRECT",
                  "kafka_cluster": {
                    "bootstrap_servers": "localhost:9092",
                    "ssl": {
                      "keystore": {
                        "path": "/path/to/keystore.jks",
                        "password": "keystore-password",
                        "key_password": "key-password"
                      }
                    }
                  }
                }
                """
        ),
        new TestInput(
            "Direct spec is invalid with SSL keystore not having path",
            """
                {
                  "name": "Connection 1",
                  "type": "DIRECT",
                  "kafka_cluster": {
                    "bootstrap_servers": "localhost:9092",
                    "ssl": {
                      "truststore": {
                        "path": "/path/to/truststore.jks",
                        "password": "truststore-password"
                      },
                      "keystore": {
                        "password": "keystore-password",
                        "key_password": "key-password"
                      }
                    }
                  }
                }
                """,
            createError()
                .withSource("kafka_cluster.ssl.keystore.path")
                .withDetail("Kafka cluster keystore path is required and may not be blank")
        ),
        new TestInput(
            "Direct spec is valid with Kerberos keytab and principal",
            """
            {
              "name": "Connection 1",
              "type": "DIRECT",
              "kafka_cluster": {
                "bootstrap_servers": "localhost:9092",
                "credentials": {
                  "type": "KERBEROS",
                  "keytab_path": "/path/to/keytab",
                  "principal": "alice@EXAMPLE.com"
                }
              }
            }
            """
        ),
        new TestInput(
            "Direct spec is valid with Kerberos keytab, principal and service name",
            """
            {
              "name": "Connection 1",
              "type": "DIRECT",
              "kafka_cluster": {
                "bootstrap_servers": "localhost:9092",
                "credentials": {
                  "type": "KERBEROS",
                  "keytab_path": "/path/to/keytab",
                  "principal": "alice@EXAMPLE.com",
                  "service_name": "foobar"
                }
              }
            }
            """
        ),
        new TestInput(
            "Direct spec is invalid with Kerberos keytab but no principal",
            """
            {
              "name": "Connection 1",
              "type": "DIRECT",
              "kafka_cluster": {
                "bootstrap_servers": "localhost:9092",
                "credentials": {
                  "type": "KERBEROS",
                  "keytab_path": "/path/to/keytab"
                }
              }
            }
            """,
            createError()
                .withSource("kafka_cluster.credentials.principal")
                .withDetail("Kafka cluster principal is required and may not be blank")
        ),
        new TestInput(
            "Direct spec is invalid with Kerberos principal but no keytab",
            """
            {
              "name": "Connection 1",
              "type": "DIRECT",
              "kafka_cluster": {
                "bootstrap_servers": "localhost:9092",
                "credentials": {
                  "type": "KERBEROS",
                  "principal": "alice@EXAMPLE.com"
                }
              }
            }
            """,
            createError()
                .withSource("kafka_cluster.credentials.keytab_path")
                .withDetail("Kafka cluster keytab path is required and may not be blank")
        ),

        // Combination
        new TestInput(
            "Local spec is invalid with Kafka spec, SR spec, and CCloud spec",
            """
                {
                  "name": "Connection 1",
                  "type": "LOCAL",
                  "ccloud_config": {
                    "organization_id": ""
                  },
                  "kafka_cluster": {
                    "bootstrap_servers": null
                  },
                  "schema_registry": {
                    "uri": null
                  }
                }
                """,
            createError()
                .withSource("ccloud_config")
                .withDetail("CCloud configuration is not allowed when type is LOCAL"),
            createError()
                .withSource("kafka_cluster")
                .withDetail("Kafka cluster configuration is not allowed when type is LOCAL"),
            createError()
                .withSource("schema_registry.uri")
                .withDetail("Schema Registry URI is required and may not be blank")
        )
    );
    return inputs
        .stream()
        .map(input -> DynamicTest.dynamicTest(
            "Testing: " + input.displayName,
            () -> {
              connectionStateManager.clearAllConnectionStates();

              // Parse the spec and add a name and ID
              var id = "c1";
              var spec = OBJECT_MAPPER
                  .readValue(input.specJson(), ConnectionSpec.class)
                  .withId(id);

              // Attempt to update the connection with the invalid spec
              var response = given()
                  .contentType(ContentType.JSON)
                  .body(spec)
                  .when().post("/gateway/v1/connections")
                  .then();

              // Verify the results
              var success = assertResponseMatches(response, input.expectedErrors);
              if (success) {
                // The connection spec should have been created changed
                assertEquals(spec, connectionStateManager.getConnectionSpec(id));
              } else {
                // The connection spec should not have been created
                given()
                    .contentType(ContentType.JSON)
                    .when().get("/gateway/v1/connections/{id}", id)
                    .then()
                    .statusCode(404);
              }
            }
        ));
  }

  @TestFactory
  Stream<DynamicTest> shouldAllowUpdatesWithValidSpecsOrFailWithInvalidSpecs() {
    record TestInput(
        String displayName,
        ConnectionSpec initialSpec,
        ConnectionSpec updatedSpec,
        Error... expectedErrors
    ) {

    }
    final ConnectionSpec validLocalSpec = ConnectionSpec.createLocal("c1", "Connection 1", null);
    var inputs = List.of(
        // Local configs
        new TestInput(
            "Updated of local config is valid with new name",
            validLocalSpec,
            validLocalSpec.withName("Updated Connection")
        ),
        new TestInput(
            "Updated of local config is valid with (old) Schema Registry URI",
            validLocalSpec,
            validLocalSpec.withLocalConfig("http://localhost:8081")
        ),
        new TestInput(
            "Updated of local config is valid with (new) Schema Registry URI and no credentials",
            validLocalSpec,
            validLocalSpec
                .withoutLocalConfig()
                .withSchemaRegistry(
                    ConnectionSpecSchemaRegistryConfigBuilder
                        .builder()
                        .uri("http://localhost:8081")
                        .build()
                )
        ),
        new TestInput(
            "Updated of local config is valid with (new) Schema Registry URI and basic credentials",
            validLocalSpec,
            validLocalSpec
                .withoutLocalConfig()
                .withSchemaRegistry(
                    ConnectionSpecSchemaRegistryConfigBuilder
                        .builder()
                        .uri("http://localhost:8081")
                        .credentials(new BasicCredentials(
                            "user",
                            new Password("pass".toCharArray())
                        ))
                        .build()
                )
        ),
        new TestInput(
            "Updated of local config is valid with (new) Schema Registry URI and API key and secret credentials",
            validLocalSpec,
            validLocalSpec
                .withoutLocalConfig()
                .withSchemaRegistry(
                    ConnectionSpecSchemaRegistryConfigBuilder
                        .builder()
                        .uri("http://localhost:8081")
                        .credentials(new ApiKeyAndSecret(
                            "api-key-123",
                            new ApiSecret("api-secret-123456".toCharArray())
                        ))
                        .build()
                )
        ),
        new TestInput(
            "Updated of local config is invalid without name",
            validLocalSpec,
            validLocalSpec.withName(null),
            createError().withSource("name")
                .withDetail("Connection name is required and may not be blank")
        ),
        new TestInput(
            "Updated of local config is invalid with blank name",
            validLocalSpec,
            validLocalSpec.withName("  "),
            createError().withSource("name")
                .withDetail("Connection name is required and may not be blank")
        ),
        new TestInput(
            "Updated of local config is invalid with both local and CCloud config",
            validLocalSpec,
            validLocalSpec
                .withCCloudConfig(new CCloudConfig("12345", null)),
            createError().withSource("ccloud_config")
                .withDetail("CCloud configuration is not allowed when type is LOCAL")
        ),
        new TestInput(
            "Updated of local config is invalid with CCloud config",
            validLocalSpec,
            validLocalSpec
                .withoutLocalConfig()
                .withCCloudConfig(new CCloudConfig("12345", null)),
            createError().withSource("ccloud_config")
                .withDetail("CCloud configuration is not allowed when type is LOCAL")
        )
    );
    return inputs
        .stream()
        .map(input -> DynamicTest.dynamicTest(
            "Testing: " + input.displayName,
            () -> {
              connectionStateManager.clearAllConnectionStates();

              // The initial spec should be valid
              assertTrue(input.initialSpec().validate().isEmpty());

              // Then successfully create the initial connection
              var createdSpec = ccloudTestUtil.createConnection(input.initialSpec());

              // Assert that the connection spec can be read
              assertEquals(createdSpec,
                  connectionStateManager.getConnectionSpec(input.initialSpec.id()));

              // Attempt to update the connection with the invalid spec
              var response = given()
                  .contentType(ContentType.JSON)
                  .body(input.updatedSpec)
                  .when().put("/gateway/v1/connections/{id}", createdSpec.id())
                  .then();

              // Verify the results
              var success = assertResponseMatches(response, input.expectedErrors);
              if (success) {
                // The connection spec should have changed
                assertEquals(input.updatedSpec,
                    connectionStateManager.getConnectionSpec(input.initialSpec.id()));
              } else {
                // The connection spec should not have changed
                assertEquals(input.initialSpec,
                    connectionStateManager.getConnectionSpec(input.initialSpec.id()));
              }
            }
        ));
  }

  boolean assertResponseMatches(ValidatableResponse response, Error... expectedErrors) {
    if (expectedErrors.length == 0) {
      response.statusCode(200);
      response.body("status", is(notNullValue()));
      return true;
    } else {
      response.statusCode(400);
      // Verify all the errors are reported
      var failure = response.extract().as(Failure.class);
      var actualErrors = Set.copyOf(failure.errors());
      var expectedErrorSet = Set.of(expectedErrors);
      assertEquals(expectedErrorSet, actualErrors);
      return false;
    }
  }

  protected static ValidatableResponse assertAuthStatus(String connectionId, String authStatus) {
    return given()
        .contentType(ContentType.JSON)
        .when().get("/gateway/v1/connections/{id}", connectionId)
        .then()
        .statusCode(200)
        .body("status.authentication.status", is(authStatus));
  }

  /**
   * Update the connection with a new CCloudConfig, returns the refreshed tokens after updating the
   * connection with the new config.
   */
  private AccessToken updateCCloudConfig(
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
  void updateConnection_FailUpdateNonExistingConnection() {
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
  void deleteConnection_shouldDeleteAndSecondDeleteFails() {
    // Two scenarios are tested here for delete connection.
    // 1. Deletion of existing connection
    // 2. Deletion of non-existent connection.

    // When we create a connection
    var requestBody = loadResource("connections/create-connection-request.json");
    var newConnection = given()
        .contentType(ContentType.JSON)
        .body(requestBody)
        .when().post()
        .then()
        .statusCode(200)
        .contentType(ContentType.JSON)
        .extract().body().as(Connection.class);
    var id = newConnection.id();

    // Then we can delete it
    given()
        .when().delete("/{id}", id)
        .then()
        .statusCode(204); // Expecting HTTP 204 status code for no content

    // And cannot delete it again
    given().when()
        .get("/{id}", id)
        .then()
        .statusCode(404); // Expection HTTP 404 not found.

    // And cannot delete it yet again
    given()
        .when().delete("/{id}", id)
        .then()
        .statusCode(404); // Expecting HTTP 404 status code for no content
  }

  protected void assertConnectionList(ConnectionsList expected, ConnectionsList actual) {
    if (expected != null && actual != null) {
      assertEquals(expected.apiVersion(), actual.apiVersion());
      assertEquals(expected.kind(), actual.kind());
      assertMetadata(expected.metadata(), actual.metadata());

      // Check the elements individually, since metadata checks deal with port differences
      assertEquals(expected.data().size(), actual.data().size());
      for (int i = 0; i != expected.data().size(); i++) {
        assertConnection(expected.data().get(i), actual.data().get(i));
      }
    } else {
      assertEquals(expected, actual);
    }
  }

  protected void assertConnectionList(JsonNode expected, JsonNode actual, int expectedSize) {
    assertEquals(expected.get("api_version"), actual.get("api_version"));
    assertEquals(expected.get("kind"), actual.get("kind"));
    assertMetadata(expected.get("metadata"), actual.get("metadata"));
    assertEquals(expectedSize, expected.get("data").size());
    assertEquals(expectedSize, actual.get("data").size());

    for (int i = 0; i != expectedSize; i++) {
      assertConnection(expected.get("data").get(i), actual.get("data").get(i));
    }
  }

  protected void assertConnection(Connection expected, Connection actual) {
    if (expected != null && actual != null) {
      assertEquals(expected.apiVersion(), actual.apiVersion());
      assertEquals(expected.kind(), actual.kind());
      assertMetadata(expected.metadata(), actual.metadata());
      assertEquals(expected.spec(), actual.spec());
      assertEquals(expected.status(), actual.status());
    } else {
      assertEquals(expected, actual);
    }
  }

  protected void assertConnection(JsonNode expected, JsonNode actual) {
    assertEquals(expected.get("api_version"), actual.get("api_version"));
    assertEquals(expected.get("kind"), actual.get("kind"));
    assertMetadata(expected.get("metadata"), actual.get("metadata"));
    assertEquals(expected.get("spec"), actual.get("spec"));
    assertEquals(expected.get("status"), actual.get("status"));
  }

  protected void assertMetadata(CollectionMetadata expected, CollectionMetadata actual) {
    if (expected != null && actual != null) {
      assertEquals(expected.totalSize(), actual.totalSize());
      assertEquals(
          linkWithoutPort(expected.self()),
          linkWithoutPort(actual.self())
      );
      assertEquals(
          linkWithoutPort(expected.next()),
          linkWithoutPort(actual.next())
      );
    } else {
      assertEquals(expected, actual);
    }
  }

  protected void assertMetadata(ObjectMetadata expected, ObjectMetadata actual) {
    if (expected != null && actual != null) {
      assertEquals(expected.self(), actual.self());
      assertEquals(expected.resourceName(), actual.resourceName());
      if (expected instanceof ConnectionMetadata expectedConnectionMetadata) {
        if (actual instanceof ConnectionMetadata actualConnectionMetadata) {
          var expectedSignInUri = expectedConnectionMetadata.signInUri();
          var actualSignInUri = actualConnectionMetadata.signInUri();
          assertEquals(
              expectedSignInUri == null || !expectedSignInUri.trim().isEmpty(),
              actualSignInUri == null || !actualSignInUri.trim().isEmpty()
          );
        } else {
          assertEquals(expected.getClass(), actual.getClass());
        }
      }
    } else {
      assertEquals(expected, actual);
    }
  }

  protected void assertMetadata(JsonNode expected, JsonNode actual) {
    // Don't compare the values of the sign-in URI since that contains a variable token
    // and instead just ensure that both have or do not have the field
    assertNullOrNonBlankText(expected.get("sign_in_uri"), actual.get("sign_in_uri"));
    assertEqualsOrNull(expected.get("resource_name"), actual.get("resource_name"));
    // The actual port is dynamic and will likely differ from the expected port read from files
    assertEquals(
        linkWithoutPort(expected.get("self")),
        linkWithoutPort(actual.get("self"))
    );
  }

  protected void assertNullOrNonBlankText(JsonNode expected, JsonNode actual) {
    assertEquals(
        expected == null || expected.isNull() || !expected.asText().trim().isEmpty(),
        actual == null || actual.isNull() || !actual.asText().trim().isEmpty()
    );
  }

  protected void assertEqualsOrNull(JsonNode expected, JsonNode actual) {
    assertEquals(
        expected == null || expected.isNull() ? null : expected,
        actual == null || actual.isNull() ? null : actual
    );
  }

  protected String linkWithoutPort(JsonNode node) {
    return node == null ? null : linkWithoutPort(node.asText());
  }

  protected String linkWithoutPort(String url) {
    return url == null ? null : url.replaceAll("localhost:\\d+", "");
  }

  /**
   * Refreshes the status of a given connection and, after completing the refresh, runs an
   * {@link ExecutionBlock} of code that can perform assertions. The {@link ExecutionBlock} runs on
   * a {@link VertxTestContext} and must call {@link VertxTestContext#completeNow()} at the end so
   * that the {@link VertxTestContext} can be completed successfully. Note that this method will let
   * the test <code>fail()</code> if the {@link VertxTestContext} has failed.
   *
   * @param connectionId The ID of the connection
   * @param testContext  The {@link VertxTestContext} instance used for executing the
   *                     {@link ExecutionBlock}
   * @param block        The {@link ExecutionBlock} that should be executed after completing the
   *                     status refresh
   */
  void refreshConnectionStatusAndThen(
      String connectionId,
      VertxTestContext testContext,
      ExecutionBlock block
  ) {
    connectionStateManager.getConnectionState(connectionId)
        .refreshStatus()
        .onComplete(testContext.succeeding(ignored -> testContext.verify(block)));
    if (testContext.failed()) {
      fail();
    }
  }
}