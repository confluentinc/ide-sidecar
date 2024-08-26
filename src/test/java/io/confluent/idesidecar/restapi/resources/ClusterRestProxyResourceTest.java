package io.confluent.idesidecar.restapi.resources;

import static io.confluent.idesidecar.restapi.cache.ClusterCacheExpectations.expectClusterInCache;
import static io.confluent.idesidecar.restapi.cache.ClusterCacheExpectations.expectClusterNotInCache;
import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.EqualToPattern;
import io.confluent.idesidecar.restapi.cache.ClusterCache;
import io.confluent.idesidecar.restapi.connections.CCloudConnectionState;
import io.confluent.idesidecar.restapi.connections.ConnectionStateManager;
import io.confluent.idesidecar.restapi.models.ClusterType;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType;
import io.confluent.idesidecar.restapi.testutil.NoAccessFilterProfile;
import io.confluent.idesidecar.restapi.util.CCloudTestUtil;
import io.quarkiverse.wiremock.devservice.ConnectWireMock;
import io.quarkus.test.InjectMock;
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
import org.junit.jupiter.params.provider.ValueSource;

@QuarkusTest
@TestProfile(NoAccessFilterProfile.class)
@ConnectWireMock
class ClusterRestProxyResourceTest {

  @Inject
  ConnectionStateManager connectionStateManager;

  @InjectMock
  ClusterCache clusterCache;

  @ConfigProperty(name = "quarkus.wiremock.devservices.port")
  int wireMockPort;

  WireMock wireMock;

  CCloudTestUtil ccloudTestUtil;

  private static final String CLUSTER_ID = "fake-cluster-id";
  private static final String CONNECTION_ID = "fake-connection-id";
  private static final String ENV_ID = "env-1234";
  private static final Map<String, String> CLUSTER_REQUEST_HEADERS = Map.of(
      "x-connection-id", CONNECTION_ID,
      "x-cluster-id", CLUSTER_ID
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
        Arguments.of("/kafka/v3/clusters/fake-cluster-id/topics"),
        Arguments.of("/schemas/id/fake-schema-id/subjects"),
        Arguments.of("/subjects/fake-subject/versions/fake-version/schema")
    );
  }

  private static Stream<Arguments> pathAndClusterTypeSource() {
    return Stream.of(
        Arguments.of(
            "/kafka/v3/clusters/fake-cluster-id/topics",
            ClusterType.KAFKA
        ),
        Arguments.of(
            "/schemas/id/fake-schema-id/subjects",
            ClusterType.SCHEMA_REGISTRY
        ),
        Arguments.of(
            "/subjects/fake-subject/versions/fake-version/schema",
            ClusterType.SCHEMA_REGISTRY
        )
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

  @ParameterizedTest
  @ValueSource(strings = {
      "/schemas/id/fake-schema-id/subjects",
      "/subjects/fake-subject/versions/fake-version/schema"
  })
  void testClusterIdHeaderNotPassedReturns400(String path) {
    // Given we have an authenticated CCloud connection
    ccloudTestUtil.createAuthedConnection(CONNECTION_ID, ConnectionType.CCLOUD);

    given()
        .when()
        .header("x-connection-id", CONNECTION_ID)
        .get(path)
        .then()
        .statusCode(400)
        .contentType(MediaType.APPLICATION_JSON)
        .body("title", containsString("x-cluster-id header is required"));
  }

  @ParameterizedTest
  @MethodSource("pathAndClusterTypeSource")
  void testNonExistentClusterInfoReturns404(String path, ClusterType clusterType) {
    ccloudTestUtil.createAuthedConnection(CONNECTION_ID, ConnectionType.CCLOUD);

    // And expect the cluster to not be found in the cache
    expectClusterNotInCache(
        clusterCache,
        CONNECTION_ID,
        CLUSTER_ID,
        clusterType
    );

    // Now trying to hit the cluster proxy endpoint without cached cluster info
    // should return a 500 error
    given()
        .when()
        .header("x-connection-id", CONNECTION_ID)
        .header("x-cluster-id", CLUSTER_ID)
        .get(path)
        .then()
        .statusCode(404)
        .contentType(MediaType.APPLICATION_JSON)
        .body("title", containsString("Cluster %s not found".formatted(CLUSTER_ID)));
  }

  @Test
  void testKafkaRestProxyThrows400IfClusterIdInPathDoesNotMatchHeader() {
    ccloudTestUtil.createAuthedConnection(CONNECTION_ID, ConnectionType.CCLOUD);

    given()
        .when()
        .headers(Map.of(
            "x-connection-id", CONNECTION_ID,
            "x-cluster-id", "lkc-abcd123"
        ))
        .get("/kafka/v3/clusters/lkc-defg456/topics")
        .then()
        .statusCode(400)
        .contentType(MediaType.APPLICATION_JSON)
        .body("title",
            containsString("Cluster id in the header does not match the one in the path."));
  }

  @ParameterizedTest
  @MethodSource("validClusterRequests")
  void testBadClusterInfoReturnsError(
      ConnectionType connectionType,
      ClusterType clusterType,
      String path
  ) {

    // Given an authenticated connection
    ccloudTestUtil.createAuthedConnection(CONNECTION_ID, connectionType);

    // And given a cluster in the cache
    expectClusterInCache(
        clusterCache,
        CONNECTION_ID,
        CLUSTER_ID,
        "http://invalid-host:%d".formatted(wireMockPort),
        clusterType
    );

    // Then requests to the cluster will return error
    given()
        .when()
        .headers(CLUSTER_REQUEST_HEADERS)
        .get(path)
        .then()
        .statusCode(500)
        .contentType(MediaType.APPLICATION_JSON)
        .body("title", is("Something went wrong while proxying request"))
        .body("errors[0].detail", containsString("invalid-host"));
  }


  private static Stream<Arguments> validClusterRequests() {
    return Stream.of(
        Arguments.of(
            // Type of connection
            ConnectionType.CCLOUD,
            // Type of cluster
            ClusterType.KAFKA,
            // The sidecar proxy path to hit
            "/kafka/v3/clusters/%s/topics".formatted(CLUSTER_ID),
            // The remote cluster endpoint to wiremock
            "/kafka/v3/clusters/%s/topics".formatted(CLUSTER_ID)),
        Arguments.of(
            ConnectionType.LOCAL, ClusterType.KAFKA,
            "/kafka/v3/clusters/%s/topics".formatted(CLUSTER_ID),
            "/v3/clusters/%s/topics".formatted(CLUSTER_ID)),
        Arguments.of(ConnectionType.CCLOUD, ClusterType.SCHEMA_REGISTRY,
            "/subjects/fake-subject/versions/fake-version/schema",
            "/subjects/fake-subject/versions/fake-version/schema"),
        Arguments.of(ConnectionType.CCLOUD, ClusterType.SCHEMA_REGISTRY,
            "/schemas/id/fake-schema-id/subjects",
            "/schemas/id/fake-schema-id/subjects")
    );
  }

  @ParameterizedTest
  @MethodSource("validClusterRequests")
  void testBasicProxyingClusterRequests(
      ConnectionType connectionType,
      ClusterType clusterType,
      String path,
      String wireMockPath
  ) {
    wireMock.register(
        WireMock.get(wireMockPath)
            .willReturn(
                WireMock.aResponse()
                    .withStatus(204)));

    // Given an authenticated connection
    ccloudTestUtil.createAuthedConnection(CONNECTION_ID, connectionType);

    // And given a cluster in the cache
    expectClusterInCache(
        clusterCache,
        CONNECTION_ID,
        CLUSTER_ID,
        "http://localhost:%d".formatted(wireMockPort),
        clusterType
    );

    // Then requests to the cluster will be accepted
    given()
        .when()
        .headers(CLUSTER_REQUEST_HEADERS)
        .get(path)
        .then()
        .statusCode(204)
        .body(emptyString());
  }


  @Test
  void testKafkaRestProxyAgainstCCloud() throws Throwable {
    // Given an authenticated CCloud connection
    ccloudTestUtil.createAuthedConnection(CONNECTION_ID, ConnectionType.CCLOUD);

    // And given a kafka cluster in the cache
    expectClusterInCache(
        clusterCache,
        CONNECTION_ID,
        CLUSTER_ID,
        "http://localhost:%d".formatted(wireMockPort),
        ClusterType.KAFKA
    );

    // Get the data plane token directly from the connection manager
    var dataPlaneToken =
        ((CCloudConnectionState) connectionStateManager.getConnectionState(CONNECTION_ID))
        .getOauthContext()
        .getDataPlaneToken();

    // Given we have a fake CCloud Kafka REST server endpoint for list topics
    wireMock.register(
        WireMock.get("/kafka/v3/clusters/%s/topics".formatted(CLUSTER_ID))
            .withHeader("Authorization",
                new EqualToPattern("Bearer %s".formatted(dataPlaneToken.token()))
            )
            .willReturn(
                WireMock.aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withHeader("x-ccloud-specific-header", "fake-value")
                    .withBody(
                        new String(Objects.requireNonNull(
                            Thread
                                .currentThread()
                                .getContextClassLoader()
                                .getResourceAsStream(
                                    "kafka-rest-proxy-mock-responses/"
                                        + "list-topics-delegate-ccloud-response.json")
                        ).readAllBytes()))).atPriority(100));

    // When we hit the Sidecar Kafka proxy endpoint with the
    // right connection ID and cluster ID
    var actualResponse = given()
        .when()
        .headers(CLUSTER_REQUEST_HEADERS)
        .get("/kafka/v3/clusters/%s/topics".formatted(CLUSTER_ID))
        .then();

    // Then we should get a 200 response
    actualResponse.statusCode(200);
    // The response should have the correct headers
    actualResponse.header("Content-Type", "application/json");
    actualResponse.header("x-ccloud-specific-header", "fake-value");

    var actualResponseBody = actualResponse.extract().asString();
    var expectedResponseBody = new String(Objects.requireNonNull(
        Thread
            .currentThread()
            .getContextClassLoader()
            .getResourceAsStream(
                "kafka-rest-proxy-mock-responses/list-topics-sidecar-proxy-response.json")
    ).readAllBytes());
    // Then the response body should be the same as the expected response body
    assertEquals(expectedResponseBody, actualResponseBody);
  }

  @Test
  void testSchemaRegistryRestProxyAgainstCCloud() throws Throwable {
    // Given an authenticated CCloud connection
    ccloudTestUtil.createAuthedConnection(CONNECTION_ID, ConnectionType.CCLOUD);

    // And given a kafka cluster in the cache
    String srClusterId = "lsrc-defg456";
    expectClusterInCache(
        clusterCache,
        CONNECTION_ID,
        srClusterId,
        "http://localhost:%d".formatted(wireMockPort),
        ClusterType.SCHEMA_REGISTRY
    );

    // Get the data plane token directly from the connection manager
    var dataPlaneToken =
        ((CCloudConnectionState) connectionStateManager.getConnectionState(CONNECTION_ID))
        .getOauthContext()
        .getDataPlaneToken();

    // Given we have a fake CCloud Schema Registry server endpoint for list schemas
    wireMock.register(
        WireMock.get("/schemas")
            .withHeader("Authorization",
                new EqualToPattern("Bearer %s".formatted(dataPlaneToken.token()))
            )
            .withHeader("target-sr-cluster", new EqualToPattern(srClusterId))
            .willReturn(
                WireMock.aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withHeader("x-ccloud-specific-header", "fake-value")
                    .withBody(
                        new String(Objects.requireNonNull(
                            Thread
                                .currentThread()
                                .getContextClassLoader()
                                .getResourceAsStream(
                                    "schema-registry-rest-mock-responses/"
                                        + "list-schemas-ccloud.json")
                        ).readAllBytes()))).atPriority(100));

    // When we hit the Sidecar Kafka proxy endpoint with the
    // right connection ID and cluster ID
    var actualResponse = given()
        .when()
        .headers(Map.of(
            "x-connection-id", CONNECTION_ID,
            "x-cluster-id", srClusterId
        ))
        .get("/schemas")
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
            // We expect the same response passed through
            .getResourceAsStream(
                "schema-registry-rest-mock-responses/list-schemas-ccloud.json")
    ).readAllBytes());
    // Then the response body should be the same as the expected response body
    assertEquals(expectedResponseBody, actualResponseBody);
  }

  @Test
  void testUnauthedKafkaRestProxyAgainstCCloud() {
    // Given an non-authenticated CCloud connection
    ccloudTestUtil.createConnection(CONNECTION_ID, "My Connection", ConnectionType.CCLOUD);

    // And given a kafka cluster in the cache
    expectClusterInCache(
        clusterCache,
        CONNECTION_ID,
        CLUSTER_ID,
        "http://localhost:%d".formatted(wireMockPort),
        ClusterType.KAFKA
    );

    // Then requests to the cluster should be completed
    given()
        .when()
        .headers(CLUSTER_REQUEST_HEADERS)
        .get("/kafka/v3/clusters/%s/topics".formatted(CLUSTER_ID))
        .then()

        .statusCode(401)
        .body("title", containsString("Unauthorized"));
  }

  @Test
  void testKafkaRestAgainstConfluentLocal() throws Throwable {
    // Given a connection
    ccloudTestUtil.createAuthedConnection(CONNECTION_ID, ConnectionType.LOCAL);

    // And given a kafka cluster in the cache
    expectClusterInCache(
        clusterCache,
        CONNECTION_ID,
        CLUSTER_ID,
        "http://localhost:%d".formatted(wireMockPort),
        ClusterType.KAFKA
    );

    // Given we have a fake Confluent Local Kafka REST server endpoint for list topics
    wireMock.register(
        // Notice how the mocked endpoint is /v3/clusters/%s/topics, this is what
        // the sidecar tries to hit in case of Confluent Local Kafka REST
        WireMock.get("/v3/clusters/%s/topics".formatted(CLUSTER_ID))
            .willReturn(
                WireMock.aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withHeader("x-local-specific-header", "fake-value")
                    .withBody(
                        new String(Objects.requireNonNull(
                            Thread
                                .currentThread()
                                .getContextClassLoader()
                                .getResourceAsStream(
                                    "kafka-rest-proxy-mock-responses/"
                                        + "list-topics-delegate-local-response.json")
                        ).readAllBytes()))).atPriority(100));

    // When we hit the Sidecar Kafka proxy endpoint with the
    // right connection ID and cluster ID
    var actualResponse = given()
        .when()
        .headers(CLUSTER_REQUEST_HEADERS)
        .get("/kafka/v3/clusters/%s/topics".formatted(CLUSTER_ID))
        .then();

    // Then we should get a 200 response
    actualResponse.statusCode(200);
    // The response should have the correct headers
    actualResponse.header("Content-Type", "application/json");
    actualResponse.header("x-local-specific-header", "fake-value");

    var actualResponseBody = actualResponse.extract().asString();
    var expectedResponseBody = new String(Objects.requireNonNull(
        Thread
            .currentThread()
            .getContextClassLoader()
            .getResourceAsStream(
                "kafka-rest-proxy-mock-responses/list-topics-sidecar-proxy-response.json")
    ).readAllBytes());
    // Then the response body should be the same as the expected response body
    assertEquals(expectedResponseBody, actualResponseBody);
  }

  private static Stream<Arguments> invalidClusterRequests() {
    return Stream.of(
        // Local Schema Registry is not supported
        Arguments.of(ConnectionType.LOCAL, ClusterType.SCHEMA_REGISTRY,
            "/subjects/fake-subject/versions/fake-version/schema"),
        Arguments.of(ConnectionType.LOCAL, ClusterType.SCHEMA_REGISTRY,
            "/schemas/id/fake-schema-id/subjects"),
        // Platform anything is not supported
        Arguments.of(ConnectionType.PLATFORM, ClusterType.KAFKA,
            "/kafka/v3/clusters/%s/topics".formatted(CLUSTER_ID)),
        Arguments.of(ConnectionType.PLATFORM, ClusterType.SCHEMA_REGISTRY,
            "/subjects/fake-subject/versions/fake-version/schema"),
        Arguments.of(ConnectionType.PLATFORM, ClusterType.SCHEMA_REGISTRY,
            "/schemas/id/fake-schema-id/subjects")
    );
  }

  @ParameterizedTest
  @MethodSource("invalidClusterRequests")
  void testInvalidClusterRequests(
      ConnectionType connectionType,
      ClusterType clusterType,
      String path
  ) {
    // Given a connection
    ccloudTestUtil.createAuthedConnection(CONNECTION_ID, connectionType);

    // And given a kafka cluster in the cache
    expectClusterInCache(
        clusterCache,
        CONNECTION_ID,
        CLUSTER_ID,
        "http://localhost:%d".formatted(wireMockPort),
        clusterType
    );

    // Tests that Schema Registry endpoints do not work for
    // Confluent Local connections
    given()
        .when()
        .headers(CLUSTER_REQUEST_HEADERS)
        .get(path)
        .then()
        .statusCode(501)
        .contentType(MediaType.APPLICATION_JSON)
        .body("title",
            containsString("Cannot handle request"));
  }
}
