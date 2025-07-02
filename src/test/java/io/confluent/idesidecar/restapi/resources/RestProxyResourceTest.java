package io.confluent.idesidecar.restapi.resources;

import static io.confluent.idesidecar.restapi.resources.ConnectionsResourceTest.assertAuthStatus;
import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.loadResource;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.EqualToPattern;
import io.confluent.idesidecar.restapi.connections.CCloudConnectionState;
import io.confluent.idesidecar.restapi.connections.ConnectionStateManager;
import io.confluent.idesidecar.restapi.models.ConnectionStatus.ConnectedState;
import io.confluent.idesidecar.restapi.testutil.NoAccessFilterProfile;
import io.confluent.idesidecar.restapi.util.CCloudTestUtil;
import io.quarkiverse.wiremock.devservice.ConnectWireMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.MediaType;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@QuarkusTest
@TestProfile(NoAccessFilterProfile.class)
@ConnectWireMock
class RestProxyResourceTest {

  @Inject
  ConnectionStateManager connectionStateManager;

  @ConfigProperty(name = "quarkus.wiremock.devservices.port")
  int wireMockPort;

  WireMock wireMock;

  CCloudTestUtil ccloudTestUtil;

  private static final String CONNECTION_ID = "fake-connection-id";

  @BeforeEach
  void setUp() {
    wireMock = new WireMock(wireMockPort);
    ccloudTestUtil = new CCloudTestUtil(
        wireMock,
        connectionStateManager
    );
  }

  @AfterEach
  void tearDown() {
    connectionStateManager.clearAllConnectionStates();
    wireMock.removeMappings();
  }

  private static Stream<Arguments> pathSource() {
    return Stream.of(
        Arguments.of("/metadata/security/v2alpha1/authorize")
    );
  }

  @ParameterizedTest
  @MethodSource("pathSource")
  void testConnectionHeaderNotPassedReturns400(String path) {
    given()
        .contentType(MediaType.APPLICATION_JSON)
        .accept(MediaType.APPLICATION_JSON)
        .header("X-Debug", "true")
        .body(
            "{\"userPrincipal\":\"User:YOUR_USER_ID\",\"actions\":[{\"resourceType\":\"Organization\",\"resourceName\":\"organization\",\"operation\":\"CreateEnvironment\",\"scope\":{\"clusters\":{},\"path\":[\"organization=YOUR_ORG_ID\"]}}]}")
        .when()
        .put(path)
        .then()
        .statusCode(400)
        .contentType(MediaType.APPLICATION_JSON)
        .body("title", containsString("x-connection-id header is required"));
  }

  @ParameterizedTest
  @MethodSource("pathSource")
  void testConnectionNotFoundReturns404(String path) {
    given()
        .contentType(MediaType.APPLICATION_JSON)
        .accept(MediaType.APPLICATION_JSON)
        .header("x-connection-id", CONNECTION_ID)
        .body(
            "{\"userPrincipal\":\"User:YOUR_USER_ID\",\"actions\":[{\"resourceType\":\"Organization\",\"resourceName\":\"organization\",\"operation\":\"CreateEnvironment\",\"scope\":{\"clusters\":{},\"path\":[\"organization=YOUR_ORG_ID\"]}}]}")
        .when()
        .put(path)
        .then()
        .statusCode(404)
        .contentType(MediaType.APPLICATION_JSON)
        .body("title", is("Connection id=%s not found".formatted(CONNECTION_ID)));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("pathSource")
  void testRBACProxyAgainstCCloud(String path) throws Throwable {
    // Create authenticated connection
    var connectionId = "c1";
    var connectionName = "Connection 1";
    var orgName = "Development Org";
    ccloudTestUtil.createAuthedCCloudConnection(connectionId, connectionName, orgName, null);

    assertAuthStatus(connectionId, ConnectedState.SUCCESS.name());

    // Get the data plane token directly from the connection manager
    var controlPlaneToken =
        ((CCloudConnectionState) connectionStateManager.getConnectionState(connectionId))
            .getOauthContext()
            .getControlPlaneToken();

    // Given we have a fake CCloud RBAC server endpoint
    wireMock.register(
        WireMock.put(path)
            .withHeader("Authorization",
                new EqualToPattern("Bearer %s".formatted(controlPlaneToken.token()))
            )
            .willReturn(
                WireMock.aResponse()
                    .withStatus(200)
                    .withBody(
                        new String(Objects.requireNonNull(
                            Thread
                                .currentThread()
                                .getContextClassLoader()
                                .getResourceAsStream(
                                    "rbac-proxy-mock-responses/authorize-response.json")
                        ).readAllBytes()))).atPriority(100));

    // When we hit the Sidecar RBAC proxy endpoint with the right connection ID
    var actualResponse = given()
        .contentType(MediaType.APPLICATION_JSON)
        .accept(MediaType.APPLICATION_JSON)
        .headers(Map.of("x-connection-id", connectionId))
        .header("Authorization", "Bearer " + controlPlaneToken.token()) // Ensure the header is set
        .body(
            "{\"userPrincipal\":\"User:YOUR_USER_ID\",\"actions\":[{\"resourceType\":\"Organization\",\"resourceName\":\"organization\",\"operation\":\"CreateEnvironment\",\"scope\":{\"clusters\":{},\"path\":[\"organization=YOUR_ORG_ID\"]}}]}")
        .when()
        .put(path)
        .then();

    // Then we should get a 200 response
    actualResponse.statusCode(200);

    var actualResponseBody = actualResponse.extract().asString();

    var expectedResponseBody = loadResource("rbac-proxy-mock-responses/authorize-response.json");

    // Then the response body should be the same as the expected response body
    assertEquals(expectedResponseBody, actualResponseBody);
  }

  @ParameterizedTest
  @MethodSource("pathSource")
  void testRBACProxyWithoutAuthReturns401(String path) {
    // Create authenticated connection
    var connectionId = "c1";
    var connectionName = "Connection 1";
    var orgName = "Development Org";
    ccloudTestUtil.createAuthedCCloudConnection(
        connectionId,
        connectionName,
        orgName,
        null
    );

    assertAuthStatus(connectionId, ConnectedState.SUCCESS.name());

    // Given we have a fake CCloud RBAC server endpoint
    // that always returns a 401
    wireMock.register(
        WireMock.put(path)
            .willReturn(
                WireMock.aResponse()
                    .withStatus(401)
                    .withHeader("Content-Type", MediaType.APPLICATION_JSON)
                    .withBody("{\"title\":\"Unauthorized\"}")
            )
        .atPriority(100)
        );

    // When we hit the Sidecar RBAC proxy endpoint without the Authorization header
    var actualResponse = given()
        .contentType(MediaType.APPLICATION_JSON)
        .accept(MediaType.APPLICATION_JSON)
        .headers(Map.of("x-connection-id", connectionId))
        .body(
            "{\"userPrincipal\":\"User:YOUR_USER_ID\",\"actions\":[{\"resourceType\":\"Organization\",\"resourceName\":\"organization\",\"operation\":\"CreateEnvironment\",\"scope\":{\"clusters\":{},\"path\":[\"organization=YOUR_ORG_ID\"]}}]}")
        .when()
        .put(path)
        .then();

    // Then we should get a 401 response
    actualResponse.statusCode(401)
        .contentType(MediaType.APPLICATION_JSON)
        .body("title", is("Unauthorized"));
  }
}