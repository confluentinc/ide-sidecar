package io.confluent.idesidecar.restapi.resources;

import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.asJson;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.EqualToPattern;
import io.confluent.idesidecar.restapi.connections.CCloudConnectionState;
import io.confluent.idesidecar.restapi.connections.ConnectionStateManager;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.CCloudConfig;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType;
import io.confluent.idesidecar.restapi.processors.Processor;
import io.confluent.idesidecar.restapi.proxy.ControlPlaneProxyProcessor;
import io.confluent.idesidecar.restapi.proxy.ProxyContext;
import io.confluent.idesidecar.restapi.testutil.NoAccessFilterProfile;
import io.confluent.idesidecar.restapi.util.CCloudTestUtil;
import io.quarkiverse.wiremock.devservice.ConnectWireMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.vertx.core.Future;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
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
class DataPlaneProxyResourceTest {

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
        Arguments.of("/sql/v1/organizations")
    );
  }

  @ParameterizedTest
  @MethodSource("pathSource")
  void testConnectionHeaderNotPassedReturns400(String path) {
    given()
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
        .when()
        .header("x-connection-id", CONNECTION_ID)
        .put(path)
        .then()
        .statusCode(404)
        .contentType(MediaType.APPLICATION_JSON)
        .body("title", is("Connection id=%s not found".formatted(CONNECTION_ID)));
  }

  @Test
  void testDataPlaneProxyAgainstCCloud() throws Throwable {
    // Given an authenticated CCloud connection
    ccloudTestUtil.createAuthedConnection(CONNECTION_ID, ConnectionType.CCLOUD);

    // Get the control plane token directly from the connection manager
    var dataPlaneToken =
        ((CCloudConnectionState) connectionStateManager.getConnectionState(CONNECTION_ID))
            .getOauthContext()
            .getDataPlaneToken();

    // Given we have a fake CCloud Control Plane server endpoint
    wireMock.register(
        WireMock.put("/sql/v1/organizations")
            .withHeader("Authorization",
                new EqualToPattern("Bearer %s".formatted(dataPlaneToken.token()))
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
                                    "data-plane-proxy-mock-responses/statement-exceptions-response.json")
                        ).readAllBytes()))).atPriority(100));

    // When we hit the Sidecar Control Plane proxy endpoint with the right connection ID
    var actualResponse = given()
        .when()
        .headers(REQUEST_HEADERS)
        .header("Authorization", "Bearer " + dataPlaneToken.token())
        .put("http://localhost:%d/sql/v1/organizations".formatted(wireMockPort))
        .then();

    // Then we should get a 200 response
    actualResponse.statusCode(200);

    var actualResponseBody = actualResponse.extract().asString();
    var expectedResponseBody = new String(Objects.requireNonNull(
        Thread
            .currentThread()
            .getContextClassLoader()
            .getResourceAsStream(
                "data-plane-proxy-mock-responses/statement-exceptions-response.json")
    ).readAllBytes());

    // Then the response body should be the same as the expected response body
    assertEquals(expectedResponseBody, actualResponseBody);
  }

  @Test
  void testDataPlaneProxyProcessorNoInitialHeaders() {
    // Given a ProxyContext with no initial headers
    var proxyContext = new ProxyContext(
        "http://localhost/test",
        null,
        HttpMethod.PUT,
        Buffer.buffer("TestBody"),
        Map.of(),
        "test-connection-id"
    );

    // Create a mock processor to be the next in the chain
    Processor<ProxyContext, Future<ProxyContext>> nextProcessor = new Processor<>() {
      @Override
      public Future<ProxyContext> process(ProxyContext context) {
        return Future.succeededFuture(context);
      }
    };

    // Create an instance of ControlPlaneProxyProcessor and set the next processor
    ControlPlaneProxyProcessor processor = new ControlPlaneProxyProcessor();
    processor.setNext(nextProcessor);

    // When the ControlPlaneProxyProcessor processes the context
    var result = processor.process(proxyContext).result();
    // And the absolute URL should be set correctly
    assertEquals("http://localhost/test", result.getProxyRequestAbsoluteUrl());
    // And the request method should be PUT
    assertEquals(HttpMethod.PUT, result.getProxyRequestMethod());
    // And the request body should be "TestBody"
    assertEquals("TestBody", result.getProxyRequestBody().toString());
  }

}
