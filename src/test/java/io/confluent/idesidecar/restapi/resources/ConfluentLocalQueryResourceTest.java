package io.confluent.idesidecar.restapi.resources;

import static io.confluent.idesidecar.restapi.testutil.QueryResourceUtil.assertQueryResponseMatches;

import io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType;
import io.confluent.idesidecar.restapi.testutil.NoAccessFilterProfile;
import io.quarkiverse.wiremock.devservice.ConnectWireMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@QuarkusTest
@ConnectWireMock
@TestProfile(NoAccessFilterProfile.class)
public class ConfluentLocalQueryResourceTest extends ConfluentQueryResourceTestBase {

  @BeforeEach
  void setup() {
    super.setup();
  }

  @AfterEach
  void afterEach() {
    super.afterEach();
  }

  @Test
  void shouldGetLocalConnections() {
    // When there is a local connection
    ccloudTestUtil.createConnection(
        "local-1",
        "Local 1",
        ConnectionType.LOCAL
    );

    // Expect list clusters will return one cluster
    var clusterId = "4L6g3nShT-eMCtK--X86sw";
    expectSuccessfulListLocalClusters(
        "confluent-local-resources-mock-responses/list-clusters.json"
    );

    // Expect list brokers will return two brokers
    expectSuccessfulListLocalBrokers(
        clusterId,
        "confluent-local-resources-mock-responses/list-brokers.json"
    );
    // Expect get-advertised-listeners for each broker
    for (int brokerId = 1; brokerId <= 2; ++brokerId) {
      var filename = "confluent-local-resources-mock-responses/list-adv-listener-config-broker-%d.json".formatted(
          brokerId);
      expectSuccessfulGetLocalAdvertisedListenersConfig(clusterId, brokerId, filename);
    }

    assertQueryResponseMatches(
        "graph/real/local-connections-query.graphql",
        "graph/real/local-connections-expected.json"
    );
  }

  @Test
  void shouldGetLocalConnectionsWithNoClusters() {
    // When there is a local connection
    ccloudTestUtil.createConnection(
        "local-1",
        "Local 1",
        ConnectionType.LOCAL
    );

    // Expect list clusters will return zero clusters
    var clusterId = "4L6g3nShT-eMCtK--X86sw";
    expectSuccessfulListLocalClusters(
        "confluent-local-resources-mock-responses/list-empty-clusters.json"
    );

    assertQueryResponseMatches(
        "graph/real/local-connections-query.graphql",
        "graph/real/local-connections-no-cluster-expected.json"
    );
  }

  @Test
  void shouldGetLocalConnectionsWithNoBrokers() {
    // When there is a local connection
    ccloudTestUtil.createConnection(
        "local-1",
        "Local 1",
        ConnectionType.LOCAL
    );

    // Expect list clusters will return one cluster
    var clusterId = "4L6g3nShT-eMCtK--X86sw";
    expectSuccessfulListLocalClusters(
        "confluent-local-resources-mock-responses/list-clusters.json"
    );

    // Expect list brokers will return zero brokers
    expectSuccessfulListLocalBrokers(
        clusterId,
        "confluent-local-resources-mock-responses/list-empty-brokers.json"
    );

    assertQueryResponseMatches(
        "graph/real/local-connections-query.graphql",
        "graph/real/local-connections-no-brokers-expected.json"
    );
  }

  @Test
  void shouldFailWhenErrorGettingClusters() {
    // When there is a local connection
    ccloudTestUtil.createConnection(
        "local-1",
        "Local 1",
        ConnectionType.LOCAL
    );

    // Expect list clusters to fail
    expectNotFoundForListLocalClusters();

    assertQueryResponseMatches(
        "graph/real/local-connections-query.graphql",
        "graph/real/local-connections-error-fetching-clusters-expected.json",
        this::replaceWireMockPort
    );
  }

  @Test
  void shouldFailWhenErrorGettingBrokers() {
    // When there is a local connection
    ccloudTestUtil.createConnection(
        "local-1",
        "Local 1",
        ConnectionType.LOCAL
    );

    // Expect list clusters will return one cluster
    var clusterId = "4L6g3nShT-eMCtK--X86sw";
    expectSuccessfulListLocalClusters(
        "confluent-local-resources-mock-responses/list-clusters.json"
    );

    // Expect list brokers will return two brokers
    expectNotFoundForListLocalBrokers(clusterId);

    assertQueryResponseMatches(
        "graph/real/local-connections-query.graphql",
        "graph/real/local-connections-error-fetching-brokers-expected.json",
        this::replaceWireMockPort
    );
  }

  @Test
  void shouldFailWhenErrorGettingOneAdvertisedListenerConfig() {
    // When there is a local connection
    ccloudTestUtil.createConnection(
        "local-1",
        "Local 1",
        ConnectionType.LOCAL
    );

    // Expect list clusters will return one cluster
    var clusterId = "4L6g3nShT-eMCtK--X86sw";
    expectSuccessfulListLocalClusters(
        "confluent-local-resources-mock-responses/list-clusters.json"
    );

    // Expect list brokers will return two brokers
    expectSuccessfulListLocalBrokers(
        clusterId,
        "confluent-local-resources-mock-responses/list-brokers.json"
    );

    // Expect get-advertised-listeners for first broker
    var filename = "confluent-local-resources-mock-responses/list-adv-listener-config-broker-%d.json".formatted(
        1);
    expectSuccessfulGetLocalAdvertisedListenersConfig(clusterId, 1, filename);

    // Expect failed get-advertised-listeners for second broker
    expectNotFoundForGetLocalAdvertisedListenersConfig(clusterId, 2);

    assertQueryResponseMatches(
        "graph/real/local-connections-query.graphql",
        "graph/real/local-connections-error-fetching-broker-config-expected.json",
        this::replaceWireMockPort
    );
  }
}