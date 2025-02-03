package io.confluent.idesidecar.restapi.resources;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.EqualToPattern;
import io.confluent.idesidecar.restapi.connections.CCloudConnectionState;
import io.confluent.idesidecar.restapi.connections.ConnectionStateManager;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType;
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
import org.junit.jupiter.api.Test;
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
  private static final Map<String, String> REQUEST_HEADERS = Map.of(
      "x-connection-id", CONNECTION_ID
  );

  @BeforeEach
  void setUp() {
    ccloudTestUtil = new CCloudTestUtil(wireMock, connectionStateManager);
  }

  @AfterEach
  void tearDown() {
    connectionStateManager.clearAllConnectionStates();
    wireMock.removeMappings();
  }

  private static Stream<Arguments> pathSource() {
    return Stream.of(
        Arguments.of("/api/metadata/security/v2alpha1/authorize")
    );
  }

  @ParameterizedTest
  @MethodSource("pathSource")
  void testConnectionHeaderNotPassedReturns400(String path) {
    given()
        .when()
        .get(path)
        .then()
        .statusCode(400)
        .contentType(MediaType.APPLICATION_JSON)
        .body("title", containsString("x-connection-id header is required"));
  }

  @ParameterizedTest
  @MethodSource("pathSource")
  void testConnectionNotFoundReturns404(String path) {
    given()
        .when()
        .header("x-connection-id", CONNECTION_ID)
        .get(path)
        .then()
        .statusCode(404)
        .contentType(MediaType.APPLICATION_JSON)
        .body("title", is("Connection id=%s not found".formatted(CONNECTION_ID)));
  }

  @Test
  void testRBACProxyAgainstCCloud() throws Throwable {
    // Given an authenticated CCloud connection
    ccloudTestUtil.createAuthedConnection(CONNECTION_ID, ConnectionType.CCLOUD);

    // Get the data plane token directly from the connection manager
    var controlPlaneToken =
        ((CCloudConnectionState) connectionStateManager.getConnectionState(CONNECTION_ID))
            .getOauthContext()
            .getControlPlaneToken();

    // Given we have a fake CCloud RBAC server endpoint
    wireMock.register(
        WireMock.put("/api/metadata/security/v2alpha1/authorize")
            .withHeader("Authorization",
                new EqualToPattern("Bearer %s".formatted(controlPlaneToken.token()))
            )
            .willReturn(
                WireMock.aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
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
        .when()
        .headers(REQUEST_HEADERS)
        .put("/api/metadata/security/v2alpha1/authorize")
        .then();

    // Then we should get a 200 response
    actualResponse.statusCode(200);
    // The response should have the correct headers
    actualResponse.header("Content-Type", "application/json");

    var actualResponseBody = actualResponse.extract().asString();
    var expectedResponseBody = new String(Objects.requireNonNull(
        Thread
            .currentThread()
            .getContextClassLoader()
            .getResourceAsStream(
                "rbac-proxy-mock-responses/authorize-response.json")
    ).readAllBytes());
    // Then the response body should be the same as the expected response body
    assertEquals(expectedResponseBody, actualResponseBody);
  }

  @Test
  void testUnauthedRBACProxyAgainstCCloud() {
    // Given a non-authenticated CCloud connection
    ccloudTestUtil.createConnection(CONNECTION_ID, "My Connection", ConnectionType.CCLOUD);

    // Then requests to the RBAC proxy should return 401
    given()
        .when()
        .headers(REQUEST_HEADERS)
        .put("/api/metadata/security/v2alpha1/authorize")
        .then()
        .statusCode(401)
        .body("title", containsString("Unauthorized"));
  }
}