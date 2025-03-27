package io.confluent.idesidecar.restapi.resources;

import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.loadResource;

import com.github.tomakehurst.wiremock.client.WireMock;
import io.confluent.idesidecar.restapi.cache.ClusterCache;
import io.confluent.idesidecar.restapi.connections.ConnectionStateManager;
import io.confluent.idesidecar.restapi.util.CCloudTestUtil;
import io.confluent.idesidecar.restapi.util.UriUtil;
import io.quarkiverse.wiremock.devservice.WireMockConfigKey;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

public abstract class ConfluentQueryResourceTestBase {

  static final UriUtil uriUtil = new UriUtil();

  @ConfigProperty(name = "ide-sidecar.connections.ccloud.resources.org-list-uri")
  String orgListUri;

  @ConfigProperty(name = "ide-sidecar.connections.ccloud.resources.env-list-uri")
  String envListUri;

  @ConfigProperty(name = "ide-sidecar.connections.ccloud.resources.flink-compute-pools-uri")
  String computePoolListUri;

  @ConfigProperty(name = "ide-sidecar.connections.ccloud.resources.lkc-list-uri")
  String lkcListUri;

  @ConfigProperty(name = "ide-sidecar.connections.ccloud.resources.sr-list-uri")
  String srListUri;

  @ConfigProperty(name = "ide-sidecar.connections.confluent-local.resources.clusters-list-uri")
  String confluentLocalClustersUri;

  @ConfigProperty(name = "ide-sidecar.connections.confluent-local.resources.brokers-list-uri")
  String confluentLocalBrokersUri;

  @ConfigProperty(name = "ide-sidecar.connections.confluent-local.resources.broker-adv-listeners-config-uri")
  String confluentLocalBrokerAdvertisedListenerConfigUri;

  @Inject
  ConnectionStateManager connectionStateManager;

  @Inject
  ClusterCache clusterCache;

  WireMock wireMock;

  @ConfigProperty(name = WireMockConfigKey.PORT)
  Integer wireMockPort; // the port WireMock server is listening on

  CCloudTestUtil ccloudTestUtil;

  void setup() {
    ccloudTestUtil = new CCloudTestUtil(wireMock, connectionStateManager);
  }

  void afterEach() {
    wireMock.removeMappings();
    connectionStateManager.clearAllConnectionStates();
    clusterCache.clear();
  }

  void setupConfluentLocalMocks() {
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
  }

  /**
   * Sets up the following mocks for the given control plane token (specific to a connection):
   * <ul>
   *   <li>List organizations</li>
   *   <li>List environments</li>
   *   <li>List Kafka clusters</li>
   *   <li>List Kafka clusters (empty)</li>
   *   <li>List Schema Registry clusters</li>
   *   <li>List Schema Registry clusters (empty)</li>
   *   <li>List Flink compute pools</li>
   * </ul>
   */
  void setupCCloudApiMocks(String bearerToken) {

    // Register mock for listing organizations
    ccloudTestUtil.expectSuccessfulCCloudGet(
        orgListUri,
        bearerToken,
        "ccloud-resources-mock-responses/list-organizations.json"
    );

    // Register mock for listing environments
    ccloudTestUtil.expectSuccessfulCCloudGet(
        envListUri,
        bearerToken,
        "ccloud-resources-mock-responses/list-environments.json"
    );

    // Has Kafka clusters
    String mainTestEnvId = "env-x7727g";
    ccloudTestUtil.expectSuccessfulCCloudGet(
        lkcListUri.formatted(mainTestEnvId),
        bearerToken,
        "ccloud-resources-mock-responses/list-kafka-clusters.json"
    );

    String emptyEnvId = "env-kkk3jg";
    ccloudTestUtil.expectSuccessfulCCloudGet(
        lkcListUri.formatted(emptyEnvId),
        bearerToken,
        "ccloud-resources-mock-responses/list-kafka-clusters-empty.json"
    );

    // Alright, moving on to Schema Registry
    ccloudTestUtil.expectSuccessfulCCloudGet(
        srListUri.formatted(mainTestEnvId),
        bearerToken,
        "ccloud-resources-mock-responses/get-schema-registry.json"
    );

    ccloudTestUtil.expectSuccessfulCCloudGet(
        srListUri.formatted(emptyEnvId),
        bearerToken,
        "ccloud-resources-mock-responses/get-schema-registry-empty.json"
    );

    // Register mock for listing Flink compute pools
    ccloudTestUtil.expectSuccessfulCCloudGet(
        computePoolListUri.formatted(mainTestEnvId),
        bearerToken,
        "ccloud-resources-mock-responses/list-flink-compute-pools-empty.json"
    );
    // Register mock for listing Flink compute pools within ccloud request
    ccloudTestUtil.expectSuccessfulCCloudGet(
        computePoolListUri.formatted(emptyEnvId),
        bearerToken,
        "ccloud-resources-mock-responses/list-flink-compute-pools-empty.json"
    );
    // Register mock for listing Flink compute pools
    ccloudTestUtil.expectSuccessfulCCloudGet(
        computePoolListUri.formatted(mainTestEnvId),
        bearerToken,
        "ccloud-resources-mock-responses/list-flink-compute-pools.json"
    );
    // Register mock for listing Flink compute pools within ccloud request
    ccloudTestUtil.expectSuccessfulCCloudGet(
        computePoolListUri.formatted(emptyEnvId),
        bearerToken,
        "ccloud-resources-mock-responses/list-flink-compute-pools.json"
    );
}

  void expectSuccessfulListLocalClusters(String resourceFilename) {
    expectSuccessfulGet(confluentLocalClustersUri, resourceFilename);
  }

  void expectSuccessfulListLocalBrokers(String clusterId, String resourceFilename) {
    expectSuccessfulGet(confluentLocalBrokersUri.formatted(clusterId), resourceFilename);
  }

  void expectSuccessfulGetLocalAdvertisedListenersConfig(String clusterId, int brokerId,
      String resourceFilename) {
    expectSuccessfulGet(
        confluentLocalBrokerAdvertisedListenerConfigUri.formatted(clusterId, brokerId),
        resourceFilename
    );
  }

  void expectNotFoundForListLocalClusters() {
    expectNotFoundForGet(confluentLocalClustersUri);
  }

  void expectNotFoundForListLocalBrokers(String clusterId) {
    var url = confluentLocalBrokersUri.formatted(clusterId);
    expectNotFoundForGet(url);
  }

  void expectNotFoundForGetLocalAdvertisedListenersConfig(String clusterId, int brokerId) {
    var url = confluentLocalBrokerAdvertisedListenerConfigUri.formatted(clusterId, brokerId);
    expectNotFoundForGet(url);
  }

  void expectSuccessfulGet(String url, String resourceFilename) {
    wireMock.register(
        WireMock
            .get(uriUtil.getPath(url))
            .willReturn(
                WireMock
                    .aResponse()
                    .withStatus(200)
                    .withBody(
                        loadResource(resourceFilename)
                    )
            )
    );
  }

  void expectNotFoundForCCloudGet(String url, String bearerToken) {
    wireMock.register(
        WireMock
            .get(uriUtil.getPath(url))
            .withHeader("Authorization", equalTo("Bearer %s".formatted(bearerToken)))
            .willReturn(
                WireMock
                    .aResponse()
                    .withStatus(404)
                    .withBody("""
                        {
                          "errors": [
                            {
                              "id": "a6f92dfec830d00ae4a5c2d7a65ba4ed",
                              "status": "404",
                              "detail": "Not found",
                              "source": {}
                            }
                          ]
                        }
                        """)
            )
    );
  }

  void expectNotFoundForGet(String url) {
    wireMock.register(
        WireMock
            .get(uriUtil.getPath(url))
            .willReturn(
                WireMock
                    .aResponse()
                    .withStatus(404)
                    .withBody("{\"error_code\":404,\"message\":\"HTTP 404 Not Found\"}")
            )
    );
  }

  String replaceWireMockPort(String input) {
    return input.replaceAll(
        "\\$\\{quarkus\\.wiremock\\.devservices\\.port}",
        Integer.toString(wireMockPort)
    );
  }
}