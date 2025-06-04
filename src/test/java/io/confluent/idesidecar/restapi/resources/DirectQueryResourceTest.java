package io.confluent.idesidecar.restapi.resources;

import static io.confluent.idesidecar.restapi.testutil.QueryResourceUtil.assertQueryResponseMatches;
import static org.mockito.Mockito.when;

import io.confluent.idesidecar.restapi.connections.ConnectionStateManager;
import io.confluent.idesidecar.restapi.connections.DirectConnectionState;
import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType;
import io.confluent.idesidecar.restapi.models.ConnectionSpecBuilder;
import io.confluent.idesidecar.restapi.models.ConnectionSpecKafkaClusterConfigBuilder;
import io.confluent.idesidecar.restapi.models.ConnectionSpecSchemaRegistryConfigBuilder;
import io.confluent.idesidecar.restapi.models.ConnectionStatus;
import io.confluent.idesidecar.restapi.models.graph.DirectConnection;
import io.confluent.idesidecar.restapi.models.graph.DirectFetcher;
import io.confluent.idesidecar.restapi.models.graph.DirectKafkaCluster;
import io.confluent.idesidecar.restapi.models.graph.RealDirectFetcher;
import io.confluent.idesidecar.restapi.testutil.NoAccessFilterProfile;
import io.quarkiverse.wiremock.devservice.ConnectWireMock;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import io.smallrye.mutiny.Uni;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@QuarkusTest
@ConnectWireMock
@TestProfile(NoAccessFilterProfile.class)
public class DirectQueryResourceTest extends ConfluentQueryResourceTestBase {

  @BeforeEach
  void setup() {
    super.setup();
  }

  @AfterEach
  void afterEach() {
    super.afterEach();
  }

  @Test
  void shouldGetDirectConnections() {
    ccloudTestUtil.createConnection(
        "direct-1",
        "Direct 1",
        ConnectionType.DIRECT
    );

    assertQueryResponseMatches(
        "graph/real/direct-connections-simple-query.graphql",
        "graph/real/direct-connections-simple-expected.json"
    );
  }

  @Test
  void shouldGetDirectConnectionById() {
    ccloudTestUtil.createConnection(
        "direct-1",
        "Direct 1",
        ConnectionType.DIRECT
    );

    assertQueryResponseMatches(
        "graph/real/get-direct-connection-by-id-simple-query.graphql",
        "graph/real/get-direct-connection-by-id-simple-expected.json"
    );
  }

  @Test
  void shouldReturnFailureIfRequestedConnectionIsNotDirect() {
    ccloudTestUtil.createConnection(
        "direct-1",
        "Direct 1",
        ConnectionType.LOCAL
    );

    assertQueryResponseMatches(
        "graph/real/get-direct-connection-by-id-simple-query.graphql",
        "graph/real/get-direct-connection-by-id-simple-non-direct-expected.json"
    );
  }

  @Test
  void shouldReturnFailureIfRequestedConnectionDoesNotExist() {
    assertQueryResponseMatches(
        "graph/real/get-direct-connection-by-id-simple-query.graphql",
        "graph/real/get-direct-connection-by-id-simple-non-existing-expected.json"
    );
  }
}