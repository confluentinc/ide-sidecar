package io.confluent.idesidecar.restapi.cache;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.confluent.idesidecar.restapi.cache.ClusterCache;
import io.confluent.idesidecar.restapi.exceptions.ClusterNotFoundException;
import io.confluent.idesidecar.restapi.models.ClusterType;
import io.confluent.idesidecar.restapi.models.graph.Cluster;
import io.confluent.idesidecar.restapi.models.graph.KafkaCluster;
import io.confluent.idesidecar.restapi.models.graph.SchemaRegistry;

/**
 * Methods used to prepare a mock {@link ClusterCache} for expected operations.
 */
public class ClusterCacheExpectations {

  /**
   * Expect the {@link ClusterCache#getCluster(String, String, ClusterType)} method is called
   * and returns the cluster information with the given details.
   *
   * @param cache        the mock {@link ClusterCache}
   * @param connectionId the ID of the connection that should be in the cache
   * @param clusterId    the ID of the cluster to be found in the cache
   * @param clusterUrl   the URL of the cluster to be found in the cache
   * @param clusterType  the type of the cluster to be found in the cache
   */
  public static void expectClusterInCache(
      ClusterCache cache,
      String connectionId,
      String clusterId,
      String clusterUrl,
      ClusterType clusterType
  ) {
    Cluster mockCluster = switch (clusterType) {
      case KAFKA -> mockKafkaCluster(connectionId, clusterId, clusterUrl);
      case SCHEMA_REGISTRY -> mockSchemaRegistry(connectionId, clusterId, clusterUrl);
    };

    when(
        cache.getCluster(connectionId, clusterId, clusterType)
    ).thenReturn(
        mockCluster
    );
  }

  /**
   * Expect the {@link ClusterCache#getCluster(String, String, ClusterType)} method is called
   * and fails to find the cluster information with the given details.
   *
   * @param cache        the mock {@link ClusterCache}
   * @param connectionId the ID of the connection that should be in the cache
   * @param clusterId    the ID of the cluster NOT to be found in the cache
   * @param clusterType  the type of the cluster NOT to be found in the cache
   */
  public static void expectClusterNotInCache(
      ClusterCache cache,
      String connectionId,
      String clusterId,
      ClusterType clusterType
  ) {
    when(
        cache.getCluster(connectionId, clusterId, clusterType)
    ).thenThrow(
        new ClusterNotFoundException(
            "Cluster %s not found".formatted(clusterId)
        )
    );
  }

  /**
   * Expect the {@link ClusterCache#getKafkaCluster(String, String)} method is called
   * and returns the cluster information with the given details.
   * This method returns the mock {@link KafkaCluster}, in case the caller needs to pass it
   * to the {@link #expectSchemaRegistryForKafkaClusterInCache} method.
   *
   * @param cache        the mock {@link ClusterCache}
   * @param connectionId the ID of the connection that should be in the cache
   * @param clusterId    the ID of the cluster to be found in the cache
   * @param clusterUrl   the URL of the cluster to be found in the cache
   * @return the mock {@link KafkaCluster} that will be returned
   * @see #expectSchemaRegistryForKafkaClusterInCache
   */
  public static KafkaCluster expectKafkaClusterInCache(
      ClusterCache cache,
      String connectionId,
      String clusterId,
      String clusterUrl
  ) {
    var mockCluster = mockKafkaCluster(connectionId, clusterId, clusterUrl);
    when(
        cache.getKafkaCluster(connectionId, clusterId)
    ).thenReturn(
        mockCluster
    );
    return mockCluster;
  }

  /**
   * Expect the {@link ClusterCache#getKafkaCluster(String, String)} method is called
   * and fails to find the cluster information with the given details.
   *
   * @param cache        the mock {@link ClusterCache}
   * @param connectionId the ID of the connection that should be in the cache
   * @param clusterId    the ID of the cluster NOT to be found in the cache
   */
  public static void expectKafkaClusterNotInCache(
      ClusterCache cache,
      String connectionId,
      String clusterId
  ) {
    when(
        cache.getKafkaCluster(connectionId, clusterId)
    ).thenThrow(
        new ClusterNotFoundException(
            "Kafka Cluster %s not found in connection %s".formatted(clusterId, connectionId)
        )
    );
  }

  /**
   * Expect the {@link ClusterCache#getKafkaCluster(String, String)} method is called
   * and fails to find the connection with the specified ID.
   *
   * @param cache        the mock {@link ClusterCache}
   * @param connectionId the ID of the connection that should NOT be found in the cache
   */
  public static void expectKafkaClusterInCacheFailsWithNoConnection(
      ClusterCache cache,
      String connectionId
  ) {
    when(
        cache.forConnection(connectionId)
    ).thenThrow(
        new ClusterNotFoundException(
            "Connection %s does not exist".formatted(connectionId)
        )
    );
  }

  /**
   * Expect the {@link ClusterCache#getSchemaRegistry(String, String)} method is called
   * and returns the cluster information with the given details.
   *
   * @param cache        the mock {@link ClusterCache}
   * @param connectionId the ID of the connection that should be in the cache
   * @param clusterId    the ID of the schema registry to be found in the cache
   * @param clusterUrl   the URL of the schema registry to be found in the cache
   */
  public static void expectSchemaRegistryInCache(
      ClusterCache cache,
      String connectionId,
      String clusterId,
      String clusterUrl
  ) {
    var mockRegistry = mockSchemaRegistry(connectionId, clusterId, clusterUrl);
    when(
        cache.getSchemaRegistry(connectionId, clusterId)
    ).thenReturn(
        mockRegistry
    );
  }

  /**
   * Expect the {@link ClusterCache#getSchemaRegistryForKafkaCluster} method is called
   * and returns the cluster information with the given details.
   *
   * @param cache        the mock {@link ClusterCache}
   * @param connectionId the ID of the connection that should be in the cache
   * @param kafkaCluster the {@link KafkaCluster} instance that is expected to be passed to
   *                     the {@link ClusterCache#getSchemaRegistryForKafkaCluster} method
   * @param clusterId    the ID of the schema registry to be found in the cache
   * @param clusterUrl   the URL of the schema registry to be found in the cache
   */
  public static void expectSchemaRegistryForKafkaClusterInCache(
      ClusterCache cache,
      String connectionId,
      KafkaCluster kafkaCluster,
      String clusterId,
      String clusterUrl
  ) {
    var mockRegistry = mockSchemaRegistry(connectionId, clusterId, clusterUrl);
    when(
        cache.getSchemaRegistryForKafkaCluster(connectionId, kafkaCluster)
    ).thenReturn(
        mockRegistry
    );
  }

  /**
   * Expect the {@link ClusterCache#getKafkaCluster(String, String)} method is called
   * and fails to find the cluster information with the given details.
   *
   * @param cache        the mock {@link ClusterCache}
   * @param connectionId the ID of the connection that should be in the cache
   * @param kafkaCluster the {@link KafkaCluster} instance that is expected to be passed to
   *                     the {@link ClusterCache#getSchemaRegistryForKafkaCluster} method
   */
  public static void expectSchemaRegistryForKafkaClusterNotInCache(
      ClusterCache cache,
      String connectionId,
      KafkaCluster kafkaCluster
  ) {
    when(
        cache.getSchemaRegistryForKafkaCluster(connectionId, kafkaCluster)
    ).thenThrow(
        new ClusterNotFoundException(
            "Schema Registry not found for Kafka Cluster %s in connection %s".formatted(
                kafkaCluster.id(),
                connectionId
            )
        )
    );
  }

  protected static KafkaCluster mockKafkaCluster(
      String connectionId,
      String clusterId,
      String clusterUrl
  ) {
    // Create a mock cluster
    var mockCluster = mock(KafkaCluster.class);
    when(mockCluster.id()).thenReturn(clusterId);
    when(mockCluster.connectionId()).thenReturn(connectionId);
    when(mockCluster.uri()).thenReturn(clusterUrl);
    when(mockCluster.bootstrapServers()).thenReturn("localhost:9092");
    return mockCluster;
  }

  protected static SchemaRegistry mockSchemaRegistry(
      String connectionId,
      String clusterId,
      String clusterUrl
  ) {
    var mockCluster = mock(SchemaRegistry.class);
    when(mockCluster.id()).thenReturn(clusterId);
    when(mockCluster.connectionId()).thenReturn(connectionId);
    when(mockCluster.uri()).thenReturn(clusterUrl);
    return mockCluster;
  }
}
