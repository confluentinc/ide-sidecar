package io.confluent.idesidecar.restapi.resources;

import static io.confluent.idesidecar.restapi.cache.ClusterCacheAssertions.assertKafkaClusterCached;
import static io.confluent.idesidecar.restapi.cache.ClusterCacheAssertions.assertKafkaClusterNotCached;
import static io.confluent.idesidecar.restapi.cache.ClusterCacheAssertions.assertSchemaRegistryCached;
import static io.confluent.idesidecar.restapi.cache.ClusterCacheAssertions.assertSchemaRegistryNotCached;
import static io.confluent.idesidecar.restapi.testutil.QueryResourceUtil.assertQueryResponseMatches;
import static io.confluent.idesidecar.restapi.testutil.QueryResourceUtil.queryGraphQLRaw;
import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.loadResource;
import static org.junit.jupiter.api.Assertions.fail;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import io.confluent.idesidecar.restapi.connections.ConnectionState;
import io.confluent.idesidecar.restapi.exceptions.ConnectionNotFoundException;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType;
import io.confluent.idesidecar.restapi.testutil.NoAccessFilterProfile;
import io.quarkiverse.wiremock.devservice.ConnectWireMock;
import io.quarkus.logging.Log;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.ExpectedToFail;

@QuarkusTest
@ConnectWireMock
@TestProfile(NoAccessFilterProfile.class)
public class ConfluentCloudQueryResourceTest extends ConfluentQueryResourceTestBase {

  private WireMockServer wireMockServer;

  @BeforeEach
  void setup() {
    Log.info("Setting up before test");
    wireMockServer = new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort());
    wireMockServer.start();
    // Clean up any pre-existing connections to avoid conflicts
    super.setup();
    ccloudTestUtil.createAuthedConnection(
        "ccloud-dev",
        "CCloud Dev",
        ConnectionType.CCLOUD
    );

    ccloudTestUtil.createAuthedConnection(
        "ccloud-prod",
        "CCloud Prod",
        ConnectionType.CCLOUD
    );
  }

  @AfterEach
  void afterEach() {
    Log.info("Cleaning up after test");
    wireMockServer.stop();
    super.afterEach();
  }

  @Test
  void shouldGetCCloudConnectionsWithDetail() {
    setupCCloudApiMocks(
        ccloudTestUtil.getControlPlaneToken("ccloud-dev"));
    setupCCloudApiMocks(
        ccloudTestUtil.getControlPlaneToken("ccloud-prod"));

    assertQueryResponseMatches(
        "graph/real/all-ccloud-connections-query.graphql",
        "graph/real/all-ccloud-connections-expected.json"
    );
  }

  @Test
  void shouldRejectFetchingLocalConnectionWithCCloudQuery() {
    ccloudTestUtil.createAuthedConnection(
        "local-1",
        "Local 1",
        ConnectionType.LOCAL
    );

    assertQueryResponseMatches(
        "graph/real/bad-ccloud-connections-query.graphql",
        "graph/real/bad-ccloud-connections-expected.json"
    );
  }

  @Test
  void shouldOnlyListCCloudConnections() {
    ccloudTestUtil.createAuthedConnection(
        "local-1",
        "Local 1",
        ConnectionType.LOCAL
    );

    ccloudTestUtil.createAuthedConnection(
        "local-2",
        "Local 2",
        ConnectionType.LOCAL
    );
    setupCCloudApiMocks(
        ccloudTestUtil.getControlPlaneToken("ccloud-dev"));
    setupCCloudApiMocks(
        ccloudTestUtil.getControlPlaneToken("ccloud-prod"));

    assertQueryResponseMatches(
        "graph/real/all-ccloud-connections-query.graphql",
        "graph/real/all-ccloud-connections-expected.json"
    );
  }

  @Test
  void shouldGetCCloudConnectionsWithoutDetail() {
    assertQueryResponseMatches(
        "graph/real/simple-ccloud-connections-query.graphql",
        "graph/real/simple-ccloud-connections-expected.json"
    );
  }


  @Test
  void shouldGetCCloudConnectionById() {
    setupCCloudApiMocks(
        ccloudTestUtil.getControlPlaneToken("ccloud-dev"));

    assertQueryResponseMatches(
        "graph/real/get-ccloud-connection-by-id-query.graphql",
        "graph/real/get-ccloud-connection-by-id-expected.json"
    );
  }

  @Test
  void shouldFindKafkaClustersWithPartialRegion() {
    setupCCloudApiMocks(
        ccloudTestUtil.getControlPlaneToken("ccloud-dev"));

    assertQueryResponseMatches(
        "graph/real/find-ccloud-clusters-with-region-query.graphql",
        "graph/real/find-ccloud-clusters-with-region-expected.json"
    );
  }

  @Test
  void shouldFindKafkaClustersWithConnectionId() {
    setupCCloudApiMocks(
        ccloudTestUtil.getControlPlaneToken("ccloud-dev"));

    assertQueryResponseMatches(
        "graph/real/find-ccloud-clusters-query.graphql",
        "graph/real/find-ccloud-clusters-expected.json"
    );
  }

  @Test
  void shouldCacheClusterInfoOnFindKafkaClusters() {
    ConnectionState connection = connectionStateManager.getConnectionState("ccloud-dev");

    // Setup API resource mocks for CCloud connection
    setupCCloudApiMocks(
        ccloudTestUtil.getControlPlaneToken("ccloud-dev"));

    var ccloudKafkaClusterId = "lkc-123abcd";

    // Start with an empty cache
    assertKafkaClusterNotCached(clusterCache, connection, ccloudKafkaClusterId);

    // A query to fetch Kafka clusters will update the cache
    queryGraphQLRaw(loadResource("graph/real/find-ccloud-clusters-query.graphql"));

    // Verify that the cache was updated
    assertKafkaClusterCached(clusterCache, connection, ccloudKafkaClusterId);
  }

  @Test
  void shouldFindNoKafkaClusters() {
    setupCCloudApiMocks(
        ccloudTestUtil.getControlPlaneToken("ccloud-dev"));

    assertQueryResponseMatches(
        "graph/real/find-ccloud-clusters-empty-query.graphql",
        "graph/real/find-ccloud-clusters-empty-expected.json"
    );
  }

  @Test
  void shouldReturnErrorForNonExistentConnection() {
    assertQueryResponseMatches(
        "graph/real/get-ccloud-connection-id-non-existent-query.graphql",
        "graph/real/get-ccloud-connection-id-non-existent-expected.json"
    );
  }

  // TODO: Fix flaky test. Retry three times before failing.
  @Test
  @ExpectedToFail
  void shouldCacheClusterInfoOnGetKafkaClusters() {
    // Create a local connection
    ccloudTestUtil.createConnection("local-1", "Local 1", ConnectionType.LOCAL);

    ConnectionState localConnection = null;
    ConnectionState ccloudDevConnection = null;
    try {
      localConnection = connectionStateManager.getConnectionState("local-1");
      ccloudDevConnection = connectionStateManager.getConnectionState("ccloud-dev");
    } catch (ConnectionNotFoundException e) {
      fail(e);
    }

    // Setup API resource mocks for CCloud connections
    setupCCloudApiMocks(
        ccloudTestUtil.getControlPlaneToken("ccloud-dev"));
    setupCCloudApiMocks(
        ccloudTestUtil.getControlPlaneToken("ccloud-prod"));

    // Setup API resource mocks for the Confluent Local connection
    setupConfluentLocalMocks();

    var localClusterId = "4L6g3nShT-eMCtK--X86sw";
    var ccloudKafkaClusterId = "lkc-123abcd";
    var ccloudSRClusterId = "lsrc-abcdef9";

    // The cluster cache will be empty
    assertKafkaClusterNotCached(clusterCache, localConnection, localClusterId);
    assertKafkaClusterNotCached(clusterCache, ccloudDevConnection, ccloudKafkaClusterId);
    assertSchemaRegistryNotCached(clusterCache, ccloudDevConnection, ccloudSRClusterId);

    // A simple query will not load the cache, since no cluster information is loaded
    queryGraphQLRaw(loadResource("graph/real/simple-ccloud-connections-query.graphql"));
    assertKafkaClusterNotCached(clusterCache, localConnection, localClusterId);
    assertSchemaRegistryNotCached(clusterCache, ccloudDevConnection, ccloudSRClusterId);

    // A query to fetch Kafka clusters will update the cache
    queryGraphQLRaw(loadResource("graph/real/all-ccloud-connections-query.graphql"));

    // Verify that the cache was updated, only with the CCloud cluster info
    assertKafkaClusterNotCached(clusterCache, localConnection, localClusterId);
    assertKafkaClusterCached(clusterCache, ccloudDevConnection, ccloudKafkaClusterId);
    assertSchemaRegistryCached(clusterCache, ccloudDevConnection, ccloudSRClusterId);

    // A subsequent query to fetch local connections will update the cache
    queryGraphQLRaw(loadResource("graph/real/local-connections-query.graphql"));

    assertKafkaClusterCached(clusterCache, localConnection, localClusterId);
    assertKafkaClusterCached(clusterCache, ccloudDevConnection, ccloudKafkaClusterId);
    assertSchemaRegistryCached(clusterCache, ccloudDevConnection, ccloudSRClusterId);
  }

  @Test
  void shouldFailWhenErrorGettingEnvironments() {

    // When listing organizations is successful
    var bearerToken = ccloudTestUtil.getControlPlaneToken("ccloud-dev");
    ccloudTestUtil.expectSuccessfulCCloudGet(
        orgListUri,
        bearerToken,
        "ccloud-resources-mock-responses/list-organizations.json");

    // And listing environments fails
    expectNotFoundForCCloudGet(envListUri, bearerToken);

    // Then the results should have orgs but no environments.
    // This actual results are strange, in that the parent connection is null when a child
    // fetched object has an error, rather than the connection's field for the child being null.
    // It's not clear how better to handle errors. If we return an empty Multi then no error
    // is returned to the user (hides error); if we return a failed Multi for a child,
    // the GraphQL processing stops processing and uses a null value for the parent connection.
    // WTF? So we choose to return the failed Multi with the error.
    assertQueryResponseMatches(
        "graph/real/get-ccloud-connection-by-id-query.graphql",
        "graph/real/get-ccloud-connection-by-id-failed-env-expected.json",
        this::replaceWireMockPort
    );
  }

  @Test
  void shouldFailWhenErrorGettingKafkaClusters() {
    // When listing organizations is successful
    var bearerToken = ccloudTestUtil.getControlPlaneToken("ccloud-dev");
    ccloudTestUtil.expectSuccessfulCCloudGet(
        orgListUri,
        bearerToken,
        "ccloud-resources-mock-responses/list-organizations.json");

    // And listing environments is successful
    ccloudTestUtil.expectSuccessfulCCloudGet(
        envListUri,
        bearerToken,
        "ccloud-resources-mock-responses/list-environments.json");

    // And getting Kafka clusters fails for one environment
    String mainTestEnvId = "env-x7727g";
    expectNotFoundForCCloudGet(
        lkcListUri.formatted(mainTestEnvId),
        bearerToken
    );

    // But works on another environment
    String emptyEnvId = "env-kkk3jg";
    ccloudTestUtil.expectSuccessfulCCloudGet(
        lkcListUri.formatted(emptyEnvId),
        bearerToken,
        "ccloud-resources-mock-responses/list-kafka-clusters-empty.json");

    // And getting schema registry works in the first environment
    ccloudTestUtil.expectSuccessfulCCloudGet(
        srListUri.formatted(mainTestEnvId),
        bearerToken,
        "ccloud-resources-mock-responses/get-schema-registry.json");

    // And getting schema registry works (returns empty) in the second environment
    ccloudTestUtil.expectSuccessfulCCloudGet(
        srListUri.formatted(emptyEnvId),
        bearerToken,
        "ccloud-resources-mock-responses/get-schema-registry-empty.json");

    // Then the results should have orgs and environments but no kafka clusters.
    // This actual results are strange, in that the parent environment is null when a child
    // fetched object has an error, rather than the environment's field for the child being null.
    // It's not clear how better to handle errors. If we return an empty Multi then no error
    // is returned to the user (hides error); if we return a failed Multi for a child,
    // the GraphQL processing stops processing and uses a null value for the parent environment.
    // WTF? So we choose to return the failed Multi with the error.
    assertQueryResponseMatches(
        "graph/real/get-ccloud-connection-by-id-query.graphql",
        "graph/real/get-ccloud-connection-by-id-failed-kafka-expected.json",
        this::replaceWireMockPort
    );
  }

  @Test
  void shouldGetEmptyCCloudEnvironmentWithFlinkDetails() {
    var bearerToken = ccloudTestUtil.getControlPlaneToken("ccloud-dev");

    String emptyEnvId = "env-kkk3jg";
    ccloudTestUtil.expectSuccessfulCCloudGet(
        envListUri.formatted(emptyEnvId),
        bearerToken,
        "ccloud-resources-mock-responses/list-flink-compute-pools-empty.json");

    assertQueryResponseMatches(
        "graph/real/get-ccloud-environment-empty-flink-query.graphql",
        "graph/real/get-ccloud-environment-empty-flink-expected.json",
        this::replaceWireMockPort
    );
  }

  @Test
  void shouldGetCCloudEnvironmentWithFlinkDetails() {
    setupCCloudApiMocks(
        ccloudTestUtil.getControlPlaneToken("ccloud-dev"));
    setupCCloudApiMocks(
        ccloudTestUtil.getControlPlaneToken("ccloud-prod"));

    assertQueryResponseMatches(
        "graph/real/get-ccloud-environment-flink-query.graphql",
        "graph/real/get-ccloud-environment-flink-expected.json",
        this::replaceWireMockPort
    );
  }

  @Test
  void shouldFailToGetCCloudEnvironmentWithFlinkDetails() {
    setupCCloudApiMocks(
        ccloudTestUtil.getControlPlaneToken("ccloud-dev"));
    setupCCloudApiMocks(
        ccloudTestUtil.getControlPlaneToken("ccloud-prod"));

    assertQueryResponseMatches(
        "graph/real/get-ccloud-environment-flink-fail-query.graphql",
        "graph/real/get-ccloud-environment-flink-fail-expected.json",
        this::replaceWireMockPort
    );
  }
}