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

  @BeforeEach
  void setup() {
    // Mock a direct connection with a Kafka cluster
    Mockito
        .when(connectionStateManager.getConnectionState("test-connection-id"))
        .thenReturn(
            ConnectionStates.from(
                ConnectionSpec.createDirect(
                    "test-connection-id",
                    "test-cluster-id",
                    new KafkaClusterConfig("localhost:9092", null, null),
                    null
                ),
                null
            )
        );
    // Mock the cached Kafka cluster
    Mockito
        .when(clusterCache.getKafkaCluster("test-connection-id", "test-cluster-id"))
        .thenReturn(
            new DirectKafkaCluster(
                "test-cluster-id",
                null,
                "localhost:9092",
                "test-connection-id"
            )
        );
  }

  @Test
  void getConsumerClientConfigShouldIncludeOnlyConsumerConfigProps() {
    var consumerClientConfig = clientConfigurator.getConsumerClientConfig(
        "test-connection-id",
        "test-cluster-id",
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
  void getProducerClientConfigShouldIncludeOnlyProducerConfigProps() {
    var producerClientConfig = clientConfigurator.getProducerClientConfig(
        "test-connection-id",
        "test-cluster-id",
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
  void getAdminClientConfigShouldIncludeOnlyAdminConfigProps() {
    var adminClientConfig = clientConfigurator.getAdminClientConfig(
        "test-connection-id",
        "test-cluster-id"
    ).asMap();

    // Assert that the admin client config contains the admin-specific client.id; note that
    // the sidecar version is set to unknown in the test profile
    Assertions.assertEquals(
        "Confluent for VS Code sidecar unknown - Admin",
        adminClientConfig.get("client.id")
    );
  }
}
