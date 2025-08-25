package io.confluent.idesidecar.restapi.clients;

import io.confluent.idesidecar.restapi.cache.ClusterCache;
import io.confluent.idesidecar.restapi.connections.ConnectionStateManager;
import io.confluent.idesidecar.restapi.connections.ConnectionStates;
import io.confluent.idesidecar.restapi.models.ConnectionSpec;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.KafkaClusterConfig;
import io.confluent.idesidecar.restapi.models.graph.DirectKafkaCluster;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@QuarkusTest
public class ClientConfiguratorTest {

  @Inject
  ClientConfigurator clientConfigurator;

  @InjectMock
  ConnectionStateManager connectionStateManager;

  @InjectMock
  ClusterCache clusterCache;

  static final String TEST_CONNECTION_ID = "test-connection-id";
  static final String TEST_CLUSTER_ID = "test-cluster-id";

  @BeforeEach
  void setup() {
    // Mock a direct connection with a Kafka cluster
    Mockito
        .when(connectionStateManager.getConnectionState(TEST_CONNECTION_ID))
        .thenReturn(
            ConnectionStates.from(
                ConnectionSpec.createDirect(
                    TEST_CONNECTION_ID,
                    TEST_CLUSTER_ID,
                    new KafkaClusterConfig("localhost:9092", null, null, null),
                    null
                ),
                null
            )
        );
    // Mock the cached Kafka cluster
    Mockito
        .when(clusterCache.getKafkaCluster(TEST_CONNECTION_ID, TEST_CLUSTER_ID))
        .thenReturn(
            new DirectKafkaCluster(
                TEST_CLUSTER_ID,
                null,
                "localhost:9092",
                TEST_CONNECTION_ID
            )
        );
  }

  @Test
  void getConsumerClientConfigShouldIncludeOnlyConsumerConfigProps() {
    var consumerClientConfig = clientConfigurator.getConsumerClientConfig(
        TEST_CONNECTION_ID,
        TEST_CLUSTER_ID,
        false
    ).asMap();

    // Assert that the consumer client config contains the consumer-specific client.id; note that
    // the sidecar version is set to unknown in the test profile
    Assertions.assertEquals(
        "Confluent for VS Code sidecar unknown - Consumer",
        consumerClientConfig.get("client.id")
    );
  }

  @Test
  void getConsumerClientConfigShouldIncludeClientIdPrefixIfSet() {
    // Update the connection state to include a client ID prefix
    Mockito
        .when(connectionStateManager.getConnectionState(TEST_CONNECTION_ID))
        .thenReturn(
            ConnectionStates.from(
                ConnectionSpec.createDirect(
                    TEST_CONNECTION_ID,
                    TEST_CLUSTER_ID,
                    new KafkaClusterConfig(
                        "localhost:9092",
                        null,
                        null,
                        " - client ID suffix"
                    ),
                    null
                ),
                null
            )
        );

    var consumerClientConfig = clientConfigurator.getConsumerClientConfig(
        TEST_CONNECTION_ID,
        TEST_CLUSTER_ID,
        false
    ).asMap();

    // Assert that the consumer client config contains the consumer-specific client.id including the
    // configured client ID suffix; the sidecar version is set to unknown in the test profile
    Assertions.assertEquals(
        "Confluent for VS Code sidecar unknown - Consumer - client ID suffix",
        consumerClientConfig.get("client.id")
    );
  }

  @Test
  void getProducerClientConfigShouldIncludeOnlyProducerConfigProps() {
    var producerClientConfig = clientConfigurator.getProducerClientConfig(
        TEST_CONNECTION_ID,
        TEST_CLUSTER_ID,
        false
    ).asMap();

    // Assert that the producer client config contains the producer-specific client.id; note that
    // the sidecar version is set to unknown in the test profile
    Assertions.assertEquals(
        "Confluent for VS Code sidecar unknown - Producer",
        producerClientConfig.get("client.id")
    );
  }

  @Test
  void getProducerClientConfigShouldIncludeClientIdPrefixIfSet() {
    // Update the connection state to include a client ID prefix
    Mockito
        .when(connectionStateManager.getConnectionState(TEST_CONNECTION_ID))
        .thenReturn(
            ConnectionStates.from(
                ConnectionSpec.createDirect(
                    TEST_CONNECTION_ID,
                    TEST_CLUSTER_ID,
                    new KafkaClusterConfig(
                        "localhost:9092",
                        null,
                        null,
                        " - client ID suffix"
                    ),
                    null
                ),
                null
            )
        );

    var producerClientConfig = clientConfigurator.getProducerClientConfig(
        TEST_CONNECTION_ID,
        TEST_CLUSTER_ID,
        false
    ).asMap();

    // Assert that the producer client config contains the producer-specific client.id including the
    // configured client ID suffix; the sidecar version is set to unknown in the test profile
    Assertions.assertEquals(
        "Confluent for VS Code sidecar unknown - Producer - client ID suffix",
        producerClientConfig.get("client.id")
    );
  }

  @Test
  void getAdminClientConfigShouldIncludeOnlyAdminConfigProps() {
    var adminClientConfig = clientConfigurator.getAdminClientConfig(
        TEST_CONNECTION_ID,
        TEST_CLUSTER_ID
    ).asMap();

    // Assert that the admin client config contains the admin-specific client.id; note that
    // the sidecar version is set to unknown in the test profile
    Assertions.assertEquals(
        "Confluent for VS Code sidecar unknown - Admin",
        adminClientConfig.get("client.id")
    );
  }

  @Test
  void getAdminClientConfigShouldIncludeClientIdPrefixIfSet() {
    // Update the connection state to include a client ID prefix
    Mockito
        .when(connectionStateManager.getConnectionState(TEST_CONNECTION_ID))
        .thenReturn(
            ConnectionStates.from(
                ConnectionSpec.createDirect(
                    TEST_CONNECTION_ID,
                    TEST_CLUSTER_ID,
                    new KafkaClusterConfig(
                        "localhost:9092",
                        null,
                        null,
                        " - client ID suffix"
                    ),
                    null
                ),
                null
            )
        );

    var adminClientConfig = clientConfigurator.getAdminClientConfig(
        TEST_CONNECTION_ID,
        TEST_CLUSTER_ID
    ).asMap();

    // Assert that the admin client config contains the admin-specific client.id including the
    // configured client ID suffix; the sidecar version is set to unknown in the test profile
    Assertions.assertEquals(
        "Confluent for VS Code sidecar unknown - Admin - client ID suffix",
        adminClientConfig.get("client.id")
    );
  }
}
