package io.confluent.idesidecar.restapi.cache;

import io.confluent.idesidecar.restapi.cache.ClusterCache;
import io.confluent.idesidecar.restapi.connections.ConnectionState;
import io.confluent.idesidecar.restapi.exceptions.ClusterNotFoundException;
import io.confluent.idesidecar.restapi.exceptions.ConnectionNotFoundException;

import static org.junit.jupiter.api.Assertions.*;

public class ClusterCacheAssertions {

  public static void assertKafkaClusterNotCached(
      ClusterCache cache,
      ConnectionState connection,
      String clusterId
  ) {
    assertKafkaClusterNotCached(cache, connection.getId(), clusterId);
  }

  public static void assertKafkaClusterNotCached(
      ClusterCache cache,
      String connectionId,
      String clusterId
  ) {
    var clusters = cache.forConnection(connectionId);
    assertNotNull(clusters);
    assertThrows(
        ClusterNotFoundException.class,
        () -> clusters.getKafkaCluster(clusterId, false)
    );
  }

  public static void assertKafkaClusterCached(
      ClusterCache cache,
      ConnectionState connection,
      String clusterId
  ) {
    assertKafkaClusterCached(cache, connection.getId(), clusterId);
  }

  public static void assertKafkaClusterCached(
      ClusterCache cache,
      String connectionId,
      String clusterId
  ) {
    try {
      var clusters = cache.forConnection(connectionId);
      assertNotNull(clusters);
      var cluster = clusters.getKafkaCluster(clusterId, false);
      assertNotNull(cluster);
    } catch (ConnectionNotFoundException e) {
      // ok
    } catch (ClusterNotFoundException e) {
      fail("Cluster not found");
    }
  }

  public static void assertSchemaRegistryNotCached(
      ClusterCache cache,
      ConnectionState connection,
      String clusterId
  ) {
    assertSchemaRegistryNotCached(cache, connection.getId(), clusterId);
  }

  public static void assertSchemaRegistryNotCached(
      ClusterCache cache,
      String connectionId,
      String clusterId
  ) {
    var clusters = cache.forConnection(connectionId);
    assertNotNull(clusters);
    assertThrows(
        ClusterNotFoundException.class,
        () -> clusters.getSchemaRegistry(clusterId, false)
    );
  }

  public static void assertSchemaRegistryCached(
      ClusterCache cache,
      ConnectionState connection,
      String clusterId
  ) {
    assertSchemaRegistryCached(cache, connection.getId(), clusterId);
  }

  public static void assertSchemaRegistryCached(
      ClusterCache cache,
      String connectionId,
      String clusterId
  ) {
    try {
      var clusters = cache.forConnection(connectionId);
      assertNotNull(clusters);
      var cluster = clusters.getSchemaRegistry(clusterId, false);
      assertNotNull(cluster);
    } catch (ConnectionNotFoundException e) {
      // ok
    } catch (ClusterNotFoundException e) {
      fail("Cluster not found");
    }
  }
}
