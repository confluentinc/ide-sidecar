package io.confluent.idesidecar.restapi.models.graph;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.idesidecar.restapi.cache.MockSchemaRegistryClient;
import io.confluent.idesidecar.restapi.clients.SchemaRegistryClient;
import io.confluent.idesidecar.restapi.connections.ConnectionStateManager;
import io.confluent.idesidecar.restapi.connections.DirectConnectionState;
import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import io.confluent.idesidecar.restapi.models.ConnectionSpecBuilder;
import io.confluent.idesidecar.restapi.models.ConnectionSpecKafkaClusterConfigBuilder;
import io.confluent.idesidecar.restapi.models.ConnectionSpecSchemaRegistryConfigBuilder;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.KafkaFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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
      .kafkaClusterConfig(
          ConnectionSpecKafkaClusterConfigBuilder
              .builder()
              .bootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
              .build()
      )
      .schemaRegistryConfig(
          ConnectionSpecSchemaRegistryConfigBuilder
              .builder()
              .uri(SR_URL)
              .id(SR_CLUSTER_ID)
              .build()
      )
      .build();

  private static final ConnectionSpec NO_KAFKA_SPEC = ConnectionSpecBuilder
      .builder()
      .id(CONNECTION_ID)
      .name("my connection")
      .type(ConnectionSpec.ConnectionType.DIRECT)
      .schemaRegistryConfig(
          ConnectionSpecSchemaRegistryConfigBuilder
              .builder()
              .uri(SR_URL)
              .id(SR_CLUSTER_ID)
              .build()
      )
      .build();

  private static final ConnectionSpec NO_SR_SPEC = ConnectionSpecBuilder
      .builder()
      .id(CONNECTION_ID)
      .name("my connection")
      .type(ConnectionSpec.ConnectionType.DIRECT)
      .kafkaClusterConfig(ConnectionSpecKafkaClusterConfigBuilder
          .builder()
          .bootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
          .build()
      )
      .build();

  @InjectMock
  ConnectionStateManager connections;

  @Inject
  RealDirectFetcher directFetcher;

  @BeforeEach
  void setUp() {
    directFetcher.clearByConnectionId(CONNECTION_ID);
  }

  @AfterEach
  void tearDown() {
    directFetcher.clearByConnectionId(CONNECTION_ID);
  }

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
    void shouldGetConnectionByID() throws Exception {
      // Given a connection spec in the manager
      var connection = new DirectConnectionState(KAFKA_AND_SR_SPEC, null);
      when(connections.getConnectionStates()).thenReturn(List.of(connection));

      // When we try to fetch the connection by ID
      DirectConnection result = directFetcher.getDirectConnectionByID(CONNECTION_ID);

      // Then the connection should be returned with correct ID and name
      assertNotNull(result);
      assertEquals(CONNECTION_ID, result.getId());
      assertEquals("my connection", result.getName());
    }

    @Test
    void shouldReturnCachedDirectConnection() throws Exception {
      // Given a unique connection ID to avoid cache pollution from other tests
      String uniqueConnectionId = "cache-test-connection-" + System.currentTimeMillis();
      var connectionSpec = ConnectionSpecBuilder
          .builder()
          .id(uniqueConnectionId)
          .name("my cached connection")
          .type(ConnectionSpec.ConnectionType.DIRECT)
          .kafkaClusterConfig(
              ConnectionSpecKafkaClusterConfigBuilder
                  .builder()
                  .bootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
                  .build()
          )
          .build();

      var connection = new DirectConnectionState(connectionSpec, null);
      when(connections.getConnectionStates()).thenReturn(List.of(connection));

      // When we fetch the connection by ID twice
      DirectConnection result1 = directFetcher.getDirectConnectionByID(uniqueConnectionId);
      DirectConnection result2 = directFetcher.getDirectConnectionByID(uniqueConnectionId);

      // Then both results should be the same object (cached)
      assertSame(result1, result2);
      assertEquals(uniqueConnectionId, result1.getId());
      assertEquals("my cached connection", result1.getName());

      // And the connection manager should only be called once (cache hit on second call)
      verify(connections, times(1)).getConnectionStates();
    }

    @Test
    void shouldReturnNullForNonexistentConnectionID() throws Exception {
      // Given an empty connection list
      when(connections.getConnectionStates()).thenReturn(List.of());

      // When we try to fetch a connection with a non-existent ID
      DirectConnection result = directFetcher.getDirectConnectionByID("nonexistent-id");

      // Then null should be returned
      assertNull(result);
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
        protected SchemaRegistryClient createSchemaRegistryClient(
            ConnectionSpec.SchemaRegistryConfig config) {
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
    void shouldFailToFetchSchemaRegistryWhenSrClientFailsToReturnsAllSubjects()
        throws IOException, RestClientException {
      // When we have an SR client that returns the SR cluster's mode
      var mockSrClient = mock(SchemaRegistryClient.class);
      when(mockSrClient.getSchemaTypes()).thenThrow(
          new RuntimeException("Failed to get schema types using Schema Registry client")
      );

      // And a direct connection that thinks it has connected to SR and returns that SR client
      var connection = new DirectConnectionState(NO_KAFKA_SPEC, null) {
        @Override
        public boolean isSchemaRegistryConnected() {
          return true;
        }

        @Override
        protected SchemaRegistryClient createSchemaRegistryClient(
            ConnectionSpec.SchemaRegistryConfig config) {
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
    void shouldFetchSchemaRegistryWhenConnectedAndSrClientReturnsMode()
        throws IOException, RestClientException {
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
        protected SchemaRegistryClient createSchemaRegistryClient(
            ConnectionSpec.SchemaRegistryConfig config) {
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

    @Test
    void shouldThrowExceptionForNonDirectConnection() {
      // Given a non-DIRECT connection spec in the manager
      var nonDirectSpec = ConnectionSpecBuilder
          .builder()
          .id("non-direct-id")
          .name("non direct connection")
          .type(ConnectionSpec.ConnectionType.CCLOUD)
          .build();
      var connection = new DirectConnectionState(nonDirectSpec, null);
      when(connections.getConnectionStates()).thenReturn(List.of(connection));

      // When we try to fetch the connection by ID
      // Then an exception should be thrown
      Exception exception = assertThrows(
          Exception.class,
          () -> directFetcher.getDirectConnectionByID("non-direct-id")
      );

      assertEquals(
          "Connection with ID=non-direct-id is not a direct connection.",
          exception.getMessage()
      );
    }
  }
}
