package io.confluent.idesidecar.restapi.models.graph;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.confluent.idesidecar.restapi.connections.ConnectionStateManager;
import io.confluent.idesidecar.restapi.connections.DirectConnectionState;
import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.KafkaClusterConfig;
import io.confluent.idesidecar.restapi.models.ConnectionSpecBuilder;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.io.IOException;
import java.time.Duration;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.KafkaFuture;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class RealDirectFetcherTest {

  private static final String CONNECTION_ID = "connection-id";
  private static final String KAFKA_CLUSTER_ID = "cluster-1";
  private static final String KAFKA_BOOTSTRAP_SERVERS = "kafka_host:100";
  private static final String SR_CLUSTER_ID = "schema-registry-1";
  private static final String SR_URL = "http://localhost:123456";
  private static final Duration ONE_SECOND = Duration.ofSeconds(1);
  private static final ConnectionSpec KAFKA_AND_SR_SPEC = ConnectionSpecBuilder
      .builder()
      .id(CONNECTION_ID)
      .name("my connection")
      .type(ConnectionSpec.ConnectionType.DIRECT)
      .kafkaClusterConfig(new KafkaClusterConfig(
          KAFKA_BOOTSTRAP_SERVERS,
          null
      ))
      .schemaRegistryConfig(new ConnectionSpec.SchemaRegistryConfig(
          SR_CLUSTER_ID,
          SR_URL,
          null
      ))
      .build();

  private static final ConnectionSpec NO_KAFKA_SPEC = ConnectionSpecBuilder
      .builder()
      .id(CONNECTION_ID)
      .name("my connection")
      .type(ConnectionSpec.ConnectionType.DIRECT)
      .schemaRegistryConfig(new ConnectionSpec.SchemaRegistryConfig(
          SR_CLUSTER_ID,
          SR_URL,
          null
      ))
      .build();

  private static final ConnectionSpec NO_SR_SPEC = ConnectionSpecBuilder
      .builder()
      .id(CONNECTION_ID)
      .name("my connection")
      .type(ConnectionSpec.ConnectionType.DIRECT)
      .kafkaClusterConfig(new KafkaClusterConfig(
          KAFKA_BOOTSTRAP_SERVERS,
          null
      ))
      .build();

  @InjectMock
  ConnectionStateManager connections;

  @Inject
  RealDirectFetcher directFetcher;

  @Nested
  class FetchesKafkaCluster {

    @Test
    void shouldSkipFetchingKafkaClusterIfNotConnected() {
      // When there is a direct connection
      var connection = mock(DirectConnectionState.class);
      when(connections.getConnectionState(eq(CONNECTION_ID))).thenReturn(connection);

      // That is not connected to Kafka
      when(connection.isKafkaConnected()).thenReturn(false);

      // And we try to fetch the Kafka cluster
      Uni<DirectKafkaCluster> kafkaCluster = directFetcher.getKafkaCluster(CONNECTION_ID);

      // Then the Kafka cluster will be null
      assertNull(kafkaCluster.await().atMost(ONE_SECOND));
    }

    @Test
    void shouldFailToFetchKafkaClusterIfAdminClientFails() {
      // When there is a direct connection that thinks it has connected to Kafka but fails to create an admin client
      var connection = new DirectConnectionState(KAFKA_AND_SR_SPEC, null) {
        @Override
        protected AdminClient createAdminClient(ConnectionSpec.KafkaClusterConfig config) {
          throw new RuntimeException("Failed to create Admin client");
        }

        @Override
        public boolean isKafkaConnected() {
          return true;
        }
      };

      // And that connection is in the manager
      when(connections.getConnectionState(eq(CONNECTION_ID))).thenReturn(connection);

      // And we try to fetch the Kafka cluster
      Uni<DirectKafkaCluster> kafkaCluster = directFetcher.getKafkaCluster(CONNECTION_ID);

      // Then the Kafka cluster will be null
      assertNull(kafkaCluster.await().atMost(ONE_SECOND));
    }

    @Test
    void shouldFailToFetchKafkaClusterWhenAdminClientFailsToReturnsClusterId() {
      // When we have a mock admin client that returns the Kafka cluster ID
      var mockAdminClient = mock(AdminClient.class);
      var describeCluster = mock(DescribeClusterResult.class);
      when(mockAdminClient.describeCluster()).thenReturn(describeCluster);
      when(describeCluster.clusterId()).thenThrow(
          new RuntimeException("Failed to get the cluster ID")
      );

      // And a direct connection that is connected to Kafka and uses that mock admin client
      var connection = new DirectConnectionState(KAFKA_AND_SR_SPEC, null) {
        @Override
        protected AdminClient createAdminClient(ConnectionSpec.KafkaClusterConfig config) {
          return mockAdminClient;
        }

        @Override
        public boolean isKafkaConnected() {
          return true;
        }
      };

      // And that connection is in the manager
      when(connections.getConnectionState(eq(CONNECTION_ID))).thenReturn(connection);

      // And we try to fetch the Kafka cluster
      Uni<DirectKafkaCluster> kafkaCluster = directFetcher.getKafkaCluster(CONNECTION_ID);

      // Then the Kafka cluster will be null
      assertNull(kafkaCluster.await().atMost(ONE_SECOND));
    }

    @Test
    void shouldFetchKafkaClusterWhenConnectedAndAdminClientReturnsClusterId() {
      // When we have a mock admin client that returns the Kafka cluster ID
      var mockAdminClient = mock(AdminClient.class);
      var describeCluster = mock(DescribeClusterResult.class);
      when(mockAdminClient.describeCluster()).thenReturn(describeCluster);
      when(describeCluster.clusterId()).thenReturn(KafkaFuture.completedFuture(KAFKA_CLUSTER_ID));

      // And a direct connection that is connected to Kafka and uses that mock admin client
      var connection = new DirectConnectionState(KAFKA_AND_SR_SPEC, null) {
        @Override
        protected AdminClient createAdminClient(ConnectionSpec.KafkaClusterConfig config) {
          return mockAdminClient;
        }

        @Override
        public boolean isKafkaConnected() {
          return true;
        }
      };

      // And that connection is in the manager
      when(connections.getConnectionState(eq(CONNECTION_ID))).thenReturn(connection);

      // And we try to fetch the Kafka cluster
      Uni<DirectKafkaCluster> kafkaCluster = directFetcher.getKafkaCluster(CONNECTION_ID);

      // Then the Kafka cluster will be returned
      assertEquals(
          new DirectKafkaCluster(KAFKA_CLUSTER_ID, null, KAFKA_BOOTSTRAP_SERVERS, CONNECTION_ID),
          kafkaCluster.await().atMost(ONE_SECOND)
      );
    }

    @Test
    void shouldFetchNoKafkaClusterIfNoKafkaClusterIsConfigured() {
      // When there is a direct connection that thinks it has connected to Kafka but has no Kafka cluster configured
      var connection = new DirectConnectionState(NO_KAFKA_SPEC, null) {
        @Override
        public boolean isKafkaConnected() {
          return true;
        }
      };

      // And that connection is in the manager
      when(connections.getConnectionState(eq(CONNECTION_ID))).thenReturn(connection);

      // And we try to fetch the Kafka cluster
      Uni<DirectKafkaCluster> kafkaCluster = directFetcher.getKafkaCluster(CONNECTION_ID);

      // Then the Kafka cluster will be null
      assertNull(kafkaCluster.await().atMost(ONE_SECOND));
    }
  }

  @Nested
  class FetchesSchemaRegistry {

    @Test
    void shouldSkipFetchingSchemaRegistryIfNotConnected() {
      // When there is a direct connection
      var connection = mock(DirectConnectionState.class);
      when(connections.getConnectionState(eq(CONNECTION_ID))).thenReturn(connection);

      // That is not connected to Kafka
      when(connection.isSchemaRegistryConnected()).thenReturn(false);

      // And we try to fetch the Kafka cluster
      Uni<DirectSchemaRegistry> srCluster = directFetcher.getSchemaRegistry(CONNECTION_ID);

      // Then the Kafka cluster will be null
      assertNull(srCluster.await().atMost(ONE_SECOND));
    }

    @Test
    void shouldFailToFetchSchemaRegistryIfSrClientFails() {
      // When there is a direct connection that thinks it has connected to SR but fails to create an SR client
      var connection = new DirectConnectionState(NO_KAFKA_SPEC, null) {
        @Override
        public boolean isSchemaRegistryConnected() {
          return true;
        }

        @Override
        protected SchemaRegistryClient createSchemaRegistryClient(ConnectionSpec.SchemaRegistryConfig config) {
          throw new RuntimeException("Failed to create Schema Registry client");
        }
      };

      // And that connection is in the manager
      when(connections.getConnectionState(eq(CONNECTION_ID))).thenReturn(connection);

      // And we try to fetch the SR cluster
      Uni<DirectSchemaRegistry> srCluster = directFetcher.getSchemaRegistry(CONNECTION_ID);

      // Then the cluster will be null
      assertNull(srCluster.await().atMost(ONE_SECOND));
    }

    @Test
    void shouldFailToFetchSchemaRegistryWhenSrClientFailsToReturnsMode() throws IOException, RestClientException {
      // When we have an SR client that returns the SR cluster's mode
      var mockSrClient = mock(SchemaRegistryClient.class);
      when(mockSrClient.getMode()).thenThrow(
          new RuntimeException("Failed to get mode with Schema Registry client")
      );

      // And a direct connection that thinks it has connected to SR and returns that SR client
      var connection = new DirectConnectionState(NO_KAFKA_SPEC, null) {
        @Override
        public boolean isSchemaRegistryConnected() {
          return true;
        }

        @Override
        protected SchemaRegistryClient createSchemaRegistryClient(ConnectionSpec.SchemaRegistryConfig config) {
          return mockSrClient;
        }
      };

      // And that connection is in the manager
      when(connections.getConnectionState(eq(CONNECTION_ID))).thenReturn(connection);

      // And we try to fetch the SR cluster
      Uni<DirectSchemaRegistry> srCluster = directFetcher.getSchemaRegistry(CONNECTION_ID);

      // Then the cluster will be null
      assertNull(srCluster.await().atMost(ONE_SECOND));
    }

    @Test
    void shouldFetchSchemaRegistryWhenConnectedAndSrClientReturnsMode() throws IOException, RestClientException {
      // When we have an SR client that returns the SR cluster's mode
      var mockSrClient = new MockSchemaRegistryClient();
      mockSrClient.setMode("READWRITE");

      // And a direct connection that thinks it has connected to SR and returns that SR client
      var connection = new DirectConnectionState(NO_KAFKA_SPEC, null) {
        @Override
        public boolean isSchemaRegistryConnected() {
          return true;
        }

        @Override
        protected SchemaRegistryClient createSchemaRegistryClient(ConnectionSpec.SchemaRegistryConfig config) {
          return mockSrClient;
        }
      };

      // And that connection is in the manager
      when(connections.getConnectionState(eq(CONNECTION_ID))).thenReturn(connection);

      // And we try to fetch the SR cluster
      Uni<DirectSchemaRegistry> srCluster = directFetcher.getSchemaRegistry(CONNECTION_ID);

      // Then the cluster will be returned
      assertEquals(
          new DirectSchemaRegistry(SR_CLUSTER_ID, SR_URL, CONNECTION_ID),
          srCluster.await().atMost(ONE_SECOND)
      );
    }

    @Test
    void shouldFetchNoSchemaRegistryIfNoSchemaRegistryIsConfigured() {
      // When there is a direct connection that thinks it has connected to SR but has no SR configured
      var connection = new DirectConnectionState(NO_SR_SPEC, null) {
        @Override
        public boolean isSchemaRegistryConnected() {
          return true;
        }
      };

      // And that connection is in the manager
      when(connections.getConnectionState(eq(CONNECTION_ID))).thenReturn(connection);

      // And we try to fetch the SR cluster
      Uni<DirectSchemaRegistry> srCluster = directFetcher.getSchemaRegistry(CONNECTION_ID);

      // Then the cluster will be null
      assertNull(srCluster.await().atMost(ONE_SECOND));
    }
  }
}
