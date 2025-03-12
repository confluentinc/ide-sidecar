package io.confluent.idesidecar.restapi.models.graph;

import static io.confluent.idesidecar.restapi.models.graph.RealLocalFetcher.CONFLUENT_LOCAL_CLUSTER_NAME;
import static io.confluent.idesidecar.restapi.models.graph.RealLocalFetcher.CONFLUENT_LOCAL_KAFKAREST_HOSTNAME;
import static io.confluent.idesidecar.restapi.models.graph.RealLocalFetcher.CONFLUENT_LOCAL_KAFKAREST_URI;
import static io.confluent.idesidecar.restapi.util.ResourceIOUtil.loadResource;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.idesidecar.restapi.connections.ConnectionStateManager;
import io.confluent.idesidecar.restapi.exceptions.CreateConnectionException;
import io.confluent.idesidecar.restapi.exceptions.ResourceFetchingException;
import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import io.confluent.idesidecar.restapi.models.graph.ConfluentRestClient.PaginationState;
import io.confluent.idesidecar.restapi.models.graph.RealLocalFetcher.KafkaBrokerConfigResponse;
import io.confluent.idesidecar.restapi.models.graph.RealLocalFetcher.SchemaRegistryConfigResponse;
import io.confluent.idesidecar.restapi.testutil.NoAccessFilterProfile;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(NoAccessFilterProfile.class)
class RealLocalFetcherTest {

  private static final String URL = "http://acme.com";

  private static final String ERROR_JSON = "{\"error_code\":404,\"message\":\"HTTP 404 Not Found\"}";

  private static final String CLUSTER_ID = "4L6g3nShT-eMCtK--X86sw";

  @Inject
  RealLocalFetcher localFetcher;

  @Inject
  ConnectionStateManager manager;

  @Test
  void shouldParseKafkaClusterListFromValidJson() {
    var json = loadResource("confluent-local-resources-mock-responses/list-clusters.json");
    var pagination = paginationState();
    var results = localFetcher.parseKafkaClusterList(json, pagination);
    assertFalse(results.hasNextPage());
    assertEquals(1, results.items().size());

    var cluster = results.items().getFirst();
    assertEquals(CLUSTER_ID, cluster.id());
  }

  @Test
  void shouldParseKafkaBrokerListFromValidJson() {
    var json = loadResource("confluent-local-resources-mock-responses/list-brokers.json");
    var pagination = paginationState();
    var results = localFetcher.parseKafkaBrokerList(json, pagination);
    assertFalse(results.hasNextPage());
    assertEquals(2, results.items().size());

    var broker1 = results.items().getFirst();
    assertEquals(CLUSTER_ID, broker1.clusterId());
    assertEquals("1", broker1.brokerId());

    var broker2 = results.items().getLast();
    assertEquals(CLUSTER_ID, broker2.clusterId());
    assertEquals("2", broker2.brokerId());
  }

  @Test
  void shouldFailToParseKafkaBrokerListFromError() {
    var pagination = paginationState();
    var exception = assertThrows(
        ResourceFetchingException.class,
        () -> localFetcher.parseKafkaBrokerList(ERROR_JSON, pagination)
    );
    assertTrue(exception.getMessage().contains("HTTP 404 Not Found"));
    assertTrue(exception.getMessage().contains(URL));
  }

  @Test
  void shouldParseConfigResponseFromValidJson() {
    var json = loadResource(
        "confluent-local-resources-mock-responses/list-adv-listener-config-broker-1.json"
    );
    var config = localFetcher.parseConfigResponse(URL, json);
    assertEquals(CLUSTER_ID, config.clusterId());
    assertEquals("1", config.brokerId());
    assertEquals("PLAINTEXT://confluent-local-broker-1:50003,PLAINTEXT_HOST://localhost:50002",
        config.value());
  }

  @Test
  void shouldFailToParseConfigResponseFromError() {
    var exception = assertThrows(
        ResourceFetchingException.class,
        () -> localFetcher.parseConfigResponse(URL, ERROR_JSON)
    );
    assertTrue(exception.getMessage().contains("HTTP 404 Not Found"));
    assertTrue(exception.getMessage().contains(URL));
  }

  @Test
  void shouldCombineMultipleBrokerConfigsAndSortsAddresses() {
    assertLocalClusterBootstrapServersMatches(
        "localhost:123,localhost:456",
        "localhost:456", "localhost:123"
    );
  }

  @Test
  void shouldCombineSingleBrokerConfig() {
    assertLocalClusterBootstrapServersMatches(
        "localhost:123",
        "localhost:123"
    );
  }

  @Test
  void shouldExtractLocalAddress() {
    assertExtractLocalhostAddresses(
        "PLAINTEXT://other:50003,PLAINTEXT_HOST://localhost:50002",
        "localhost:50002"
    );
  }

  @Test
  void shouldExtractLocalAddressWithDifferentProtocols() {
    assertExtractLocalhostAddresses(
        "OTHER://other:50003,ACME://localhost:50002",
        "localhost:50002"
    );
  }

  @Test
  void shouldExtractLocalNoAddressesIfNoLocalhostIsIncluded() {
    assertExtractLocalhostAddresses(
        "OTHER://other:50003"
    );
  }

  @Test
  void shouldExtractMultipleLocalAddresses() {
    assertExtractLocalhostAddresses(
        "OTHER://other:50003,ACME://localhost:50001,ACME://localhost:50002",
        "localhost:50001", "localhost:50002"
    );
  }

  @Test
  void shouldExtractLocalAddressesWithTwoToSevenDigitsInPort() {
    assertExtractLocalhostAddresses(
        "localhost:11,ACME://localhost:13,ACME://localhost:0,ACME://localhost:0123456,ACME://localhost:01234567,ACME://localhost:12",
        "localhost:11", "localhost:13", "localhost:0123456", "localhost:12"
    );
  }

  PaginationState paginationState() {
    return new PaginationState(URL, ConfluentRestClient.PageLimits.DEFAULT);
  }

  KafkaBrokerConfigResponse configResponse(String valueIncludes) {
    return new KafkaBrokerConfigResponse(
        null,
        null,
        CLUSTER_ID,
        null,
        null,
        "PLAINTEXT://confluent-local-broker-1:50003,PLAINTEXT_HOST://" + valueIncludes,
        false,
        false,
        false,
        null,
        null
    );
  }

  void assertLocalClusterBootstrapServersMatches(String expectedAddresses,
      String... actualAddresses) {
    List<KafkaBrokerConfigResponse> configs = new ArrayList<>();
    for (String address : actualAddresses) {
      configs.add(configResponse(address));
    }
    var actual = RealLocalFetcher.asLocalCluster(CLUSTER_ID, configs);
    var expected = new ConfluentLocalKafkaCluster(
        CLUSTER_ID,
        CONFLUENT_LOCAL_CLUSTER_NAME,
        CONFLUENT_LOCAL_KAFKAREST_URI,
        CONFLUENT_LOCAL_KAFKAREST_HOSTNAME,
        expectedAddresses
    );
    assertEquals(expected, actual);
  }

  void assertExtractLocalhostAddresses(String input, String... expectedAddresses) {
    var addresses = RealLocalFetcher.extractLocalAddresses(input);
    assertEquals(Arrays.asList(expectedAddresses), addresses);
  }

  @Test
  void schemaRegistryFetch_shouldParseConfigResponseFromValidJson() {
    var json = loadResource(
        "confluent-local-resources-mock-responses/get-schema-registry-response.json"
    );
    SchemaRegistryConfigResponse config = localFetcher.parseSchemaRegistryConfig(URL, json);
    assertEquals("BACKWARD", config.compatibilityLevel());

  }

  @Test
  void shouldReturnValidSchemaRegistryUri() throws CreateConnectionException {
    var localConfig = new ConnectionSpec.LocalConfig("http://localhost:8085");
    var connectionSpec = ConnectionSpec.createLocal("1", "Local Connection", localConfig);
    manager.createConnectionState(connectionSpec);

    String uri = localFetcher.resolveSchemaRegistryUri("1");
    assertEquals("http://localhost:8085", uri);
  }

  @Test
  void shouldReturnNullWhenSchemaRegistryUriIsBlank() throws CreateConnectionException {
    var localConfig = new ConnectionSpec.LocalConfig("");
    var connectionSpec = ConnectionSpec.createLocal("3", "Local Connection", localConfig);
    manager.createConnectionState(connectionSpec);

    String uri = localFetcher.resolveSchemaRegistryUri("3");
    assertNull(uri); // No Schema Registry
  }
}