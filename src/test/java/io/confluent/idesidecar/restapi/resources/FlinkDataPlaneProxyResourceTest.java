package io.confluent.idesidecar.restapi.resources;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.EqualToPattern;
import io.confluent.idesidecar.restapi.connections.CCloudConnectionState;
import io.confluent.idesidecar.restapi.connections.ConnectionStateManager;
import io.confluent.idesidecar.restapi.exceptions.ProcessorFailedException;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType;
import io.confluent.idesidecar.restapi.processors.Processor;
import io.confluent.idesidecar.restapi.proxy.FlinkDataPlaneProxyProcessor;
import io.confluent.idesidecar.restapi.proxy.ProxyContext;
import io.confluent.idesidecar.restapi.testutil.NoAccessFilterProfile;
import io.confluent.idesidecar.restapi.util.CCloudTestUtil;
import io.confluent.idesidecar.restapi.util.UriUtil;
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
import java.util.concurrent.CompletionException;
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
class FlinkDataPlaneProxyResourceTest {

  @Inject
  ConnectionStateManager connectionStateManager;

  @ConfigProperty(name = "quarkus.wiremock.devservices.port")
  int wireMockPort;

  @Inject
  FlinkDataPlaneProxyProcessor flinkDataPlaneProxyProcessor;

  WireMock wireMock;

  CCloudTestUtil ccloudTestUtil;

  private static final String CONNECTION_ID = "fake-connection-id";
  private static final Map<String, String> REQUEST_HEADERS = Map.of(
      "x-connection-id", CONNECTION_ID
  );

  private io.vertx.core.MultiMap createSampleHeaders() {
    io.vertx.core.MultiMap headers = io.vertx.core.MultiMap.caseInsensitiveMultiMap();
    headers.add("Content-Type", "application/json");
    headers.add("Accept", "application/json");
    headers.add("User-Agent", "IDE-Sidecar-Test");
    headers.add("x-request-id", "test-request-id");
    headers.add("x-ccloud-region", "us-west-2");
    headers.add("x-ccloud-provider", "aws");
    return headers;
  }

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

    var dataPlaneToken =
        ((CCloudConnectionState) connectionStateManager.getConnectionState(CONNECTION_ID))
            .getOauthContext()
            .getDataPlaneToken();


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
        "/test",
        createSampleHeaders(),
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

    flinkDataPlaneProxyProcessor.setNext(nextProcessor);

    // When processing the context
    ProxyContext result = flinkDataPlaneProxyProcessor.process(proxyContext)
        .toCompletionStage()
        .toCompletableFuture()
        .join();

    // Then the URL should be transformed to the Flink URL
    String expectedUrl = "https://flink.us-west-2.aws.confluent.cloud/test";
    assertEquals(expectedUrl, result.getProxyRequestAbsoluteUrl());
    assertEquals(HttpMethod.PUT, result.getProxyRequestMethod());
    assertEquals("TestBody", result.getProxyRequestBody().toString());
  }

  @Test
  void testDataPlaneProxyProcessorNoRegionHeader() {
    // Given a ProxyContext with no region header
    MultiMap headers = createSampleHeaders();
    headers.remove("x-ccloud-region");
    var proxyContext = new ProxyContext(
        "http://localhost/test",
        headers,
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

    flinkDataPlaneProxyProcessor.setNext(nextProcessor);

    // The processor should throw a ProcessorFailedException
    CompletionException exception = assertThrows(CompletionException.class, () -> {
      flinkDataPlaneProxyProcessor.process(proxyContext)
          .toCompletionStage()
          .toCompletableFuture()
          .join();
    });

    // Verify the exception contains the expected message
    assertInstanceOf(ProcessorFailedException.class, exception.getCause());
    assertEquals("Missing required headers: x-ccloud-region and x-ccloud-provider are required for Flink requests",
        exception.getCause().getMessage());
  }

  @Test
  void testDataPlaneProxyProcessorNoProviderHeader() {
    // Given a ProxyContext with no region header
    MultiMap headers = createSampleHeaders();
    headers.remove("x-ccloud-provider");
    var proxyContext = new ProxyContext(
        "http://localhost/test",
        headers,
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

    flinkDataPlaneProxyProcessor.setNext(nextProcessor);

    // The processor should throw a ProcessorFailedException
    CompletionException exception = assertThrows(CompletionException.class, () -> {
      flinkDataPlaneProxyProcessor.process(proxyContext)
          .toCompletionStage()
          .toCompletableFuture()
          .join();
    });

    // Verify the exception contains the expected message
    assertTrue(exception.getCause() instanceof ProcessorFailedException);
    assertEquals("Missing required headers: x-ccloud-region and x-ccloud-provider are required for Flink requests",
        exception.getCause().getMessage());
  }

  @Test
  void testHeadersAreSanitized() {
    // Setup - create a valid CCloud connection
    ccloudTestUtil.createAuthedConnection(CONNECTION_ID, ConnectionType.CCLOUD);

    // Setup request headers with region and provider
    MultiMap headers = MultiMap.caseInsensitiveMultiMap();
    headers.add("x-ccloud-region", "us-west-2");
    headers.add("x-ccloud-provider", "aws");
    headers.add("Authorization", "Bearer token123");
    headers.add("Content-Type", "application/json");
    headers.add("Host", "original-host.com");

    // Mock the next processor to avoid actual remote calls
    Processor<ProxyContext, Future<ProxyContext>> nextProcessor = new Processor<>() {
      @Override
      public Future<ProxyContext> process(ProxyContext context) {
        return Future.succeededFuture(context);
      }
    };
    flinkDataPlaneProxyProcessor.setNext(nextProcessor);

    ProxyContext context = new ProxyContext(
        "/test-path",
        headers,
        HttpMethod.GET,
        Buffer.buffer(),
        null,
        CONNECTION_ID  // Use the valid connection ID
    );

    // Process the context
    ProxyContext result = flinkDataPlaneProxyProcessor.process(context)
        .toCompletionStage().toCompletableFuture().join();

    // Verify the proxy headers were correctly sanitized
    MultiMap proxyHeaders = result.getProxyRequestHeaders();
    assertNotNull(proxyHeaders);
    assertEquals("application/json", proxyHeaders.get("Content-Type"));
    assertFalse(proxyHeaders.contains("Authorization"));
    assertFalse(proxyHeaders.contains("Host"));
    assertEquals("us-west-2", headers.get("x-ccloud-region")); // Original headers unchanged
  }

  @Test
  void testRequestBodyAndMethodPreserved() {
    // Setup - create a valid CCloud connection
    ccloudTestUtil.createAuthedConnection(CONNECTION_ID, ConnectionType.CCLOUD);

    // Mock the next processor to avoid actual remote calls
    Processor<ProxyContext, Future<ProxyContext>> nextProcessor = new Processor<>() {
      @Override
      public Future<ProxyContext> process(ProxyContext context) {
        return Future.succeededFuture(context);
      }
    };
    flinkDataPlaneProxyProcessor.setNext(nextProcessor);

    // Setup headers
    MultiMap headers = MultiMap.caseInsensitiveMultiMap();
    headers.add("x-ccloud-region", "us-west-2");
    headers.add("x-ccloud-provider", "aws");

    Buffer testBody = Buffer.buffer("test-request-body");

    ProxyContext context = new ProxyContext(
        "/test-path",
        headers,
        HttpMethod.POST,
        testBody,
        null,
        CONNECTION_ID  // Use the connection ID that was properly setup
    );

    // Process the context
    ProxyContext result = flinkDataPlaneProxyProcessor.process(context)
        .toCompletionStage().toCompletableFuture().join();

    // Verify body and method are preserved
    assertEquals(HttpMethod.POST, result.getProxyRequestMethod());
    assertEquals("test-request-body", result.getProxyRequestBody().toString());
  }
}
