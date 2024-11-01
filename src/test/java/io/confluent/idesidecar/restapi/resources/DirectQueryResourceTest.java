package io.confluent.idesidecar.restapi.resources;

import static io.confluent.idesidecar.restapi.testutil.QueryResourceUtil.assertQueryResponseMatches;

import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.KafkaClusterConfig;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.SchemaRegistryConfig;
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
public class DirectQueryResourceTest extends ConfluentQueryResourceTestBase {

  static final String CONNECTION_ID = "direct-1";
  static final String CONNECTION_NAME = "Direct 1";
  static final String KAFKA_CLUSTER_ID = "kafka-cluster-1";
  static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
  static final String SCHEMA_REGISTRY_ID = "schema-registry-1";
  static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";

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
    // When there is a direct connection
    createDirectConnection(true, true);

    // Then the response should contain the connection and its Kafka and Schema Registry clusters
    assertQueryResponseMatches(
        "graph/real/direct-connections-query.graphql",
        "graph/real/direct-connections-expected.json"
    );
  }

  @Test
  void shouldGetDirectConnectionsWithOnlyKafka() {
    // When there is a direct connection
    createDirectConnection(true, false);

    // Then the response should contain the connection and its Kafka cluster but no Schema Registry
    assertQueryResponseMatches(
        "graph/real/direct-connections-query.graphql",
        "graph/real/direct-connections-no-schema-registry-expected.json"
    );
  }

  @Test
  void shouldGetDirectConnectionsWithOnlySchemaRegistry() {
    // When there is a direct connection
    createDirectConnection(false, true);

    // Then the response should contain the connection and its Schema Registry and no Kafka cluster
    assertQueryResponseMatches(
        "graph/real/direct-connections-query.graphql",
        "graph/real/direct-connections-no-kafka-expected.json"
    );
  }

  @Test
  void shouldGetDirectConnectionsWithNoClusters() {
    // When there is a direct connection
    createDirectConnection(false, false);

    // Then the response should contain the connection without Kafka and Schema Registry clusters
    assertQueryResponseMatches(
        "graph/real/direct-connections-query.graphql",
        "graph/real/direct-connections-no-cluster-expected.json"
    );
  }

  @Test
  void shouldGetDirectConnectionsWithNoConnection() {
    // When there is a local but no direct connection
    ccloudTestUtil.createConnection(
        "local-1",
        "Local 1",
        ConnectionType.LOCAL
    );

    // Then the response should contain no direct connections
    assertQueryResponseMatches(
        "graph/real/direct-connections-query.graphql",
        "graph/real/direct-connections-no-connections-expected.json"
    );
  }

  ConnectionSpec createDirectConnection(boolean withKafka, boolean withSchemaRegistry) {
    KafkaClusterConfig kafkaConfig = null;
    if (withKafka) {
      kafkaConfig = new KafkaClusterConfig(
          KAFKA_CLUSTER_ID,
          KAFKA_BOOTSTRAP_SERVERS
      );
    }
    SchemaRegistryConfig schemaRegistryConfig = null;
    if (withSchemaRegistry) {
      schemaRegistryConfig = new SchemaRegistryConfig(
          SCHEMA_REGISTRY_ID,
          SCHEMA_REGISTRY_URL
      );
    }
    var spec = new ConnectionSpec(
        CONNECTION_ID,
        CONNECTION_NAME,
        ConnectionType.DIRECT,
        null,
        null,
        kafkaConfig,
        schemaRegistryConfig
    );
    return ccloudTestUtil.createConnection(spec);
  }
}
