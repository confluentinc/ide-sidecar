package io.confluent.idesidecar.restapi.cache;

import graphql.VisibleForTesting;
import io.confluent.idesidecar.restapi.connections.ConnectionState;
import io.confluent.idesidecar.restapi.events.ClusterKind;
import io.confluent.idesidecar.restapi.events.Lifecycle;
import io.confluent.idesidecar.restapi.exceptions.ClusterNotFoundException;
import io.confluent.idesidecar.restapi.exceptions.ConnectionNotFoundException;
import io.confluent.idesidecar.restapi.models.ClusterType;
import io.confluent.idesidecar.restapi.models.ConnectionSpec.ConnectionType;
import io.confluent.idesidecar.restapi.models.graph.CCloudKafkaCluster;
import io.confluent.idesidecar.restapi.models.graph.CCloudSchemaRegistry;
import io.confluent.idesidecar.restapi.models.graph.Cluster;
import io.confluent.idesidecar.restapi.models.graph.ClusterEvent;
import io.confluent.idesidecar.restapi.models.graph.KafkaCluster;
import io.confluent.idesidecar.restapi.models.graph.RealCCloudFetcher;
import io.confluent.idesidecar.restapi.models.graph.RealDirectFetcher;
import io.confluent.idesidecar.restapi.models.graph.RealLocalFetcher;
import io.confluent.idesidecar.restapi.models.graph.SchemaRegistry;
import io.confluent.idesidecar.restapi.util.TimeUtil;
import io.quarkus.logging.Log;
import io.smallrye.common.constraint.NotNull;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.ObservesAsync;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.Deque;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeoutException;

/**
 * A bean that tracks and caches information about clusters available to different connections. The
 * purpose of this cache is to allow processors to quickly find the information about a cluster
 * given the connection ID and cluster ID.
 *
 * <p>This class uses <a href="https://quarkus.io/guides/cdi#events-and-observers">CDI events</a>
 * to observe the changes in {@link ConnectionState} objects to know when connections are created,
 * updated, authenticated, disconnected and deleted. When a connection is disconnected or deleted,
 * this class drops any cached information for the connection.
 *
 * <p>This class also observes and caches the {@link Cluster} objects loaded for the connections
 * to CCloud, Confluent Platform and Local systems, and provides a quick lookup of those clusters by
 * connection ID and cluster ID.
 *
 * <p>Components can then use this cache to quickly obtain information about a {@link Cluster}
 * given the cluster ID and connection ID.
 */
@ApplicationScoped
public class ClusterCache {

  static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(5);

  @Inject
  RealLocalFetcher localFetcher;

  @Inject
  RealCCloudFetcher ccloudFetcher;

  @Inject
  RealDirectFetcher directFetcher;

  Duration loadTimeout = DEFAULT_TIMEOUT;

  final Map<String, Clusters> clustersByConnectionId = new ConcurrentHashMap<>();

  /**
   * Get the info for the {@link Cluster} with the given ID and type.
   *
   * @param connectionId the ID of the connection
   * @param clusterId    the ID of the cluster
   * @return the info for the matching cluster
   * @throws ConnectionNotFoundException if there is no connection with the given ID
   * @throws ClusterNotFoundException    if the cluster was not found
   */
  public Cluster getCluster(String connectionId, String clusterId, ClusterType type) {
    return switch (type) {
      case KAFKA -> getKafkaCluster(connectionId, clusterId);
      case SCHEMA_REGISTRY -> getSchemaRegistry(connectionId, clusterId);
    };
  }

  /**
   * Get the info for the Kafka cluster with the given ID using the specified connection.
   *
   * @param connectionId the ID of the connection
   * @param clusterId    the ID of the cluster
   * @return the info for the matching cluster, or null if none is found
   * @throws ConnectionNotFoundException if there is no connection with the given ID
   * @throws ClusterNotFoundException    if the cluster was not found
   */
  public KafkaCluster getKafkaCluster(String connectionId, String clusterId) {
    return forConnection(connectionId).getKafkaCluster(clusterId);
  }

  /**
   * Find the first Kafka cluster accessible over the specified connection. This is useful when it
   * is known that there is only one Kafka cluster per connection.
   *
   * @param connectionId the ID of the connection
   * @return the info for the first Kafka cluster, or null if none found
   */
  public Optional<KafkaCluster> getKafkaClusterForConnection(String connectionId) {
    return forConnection(connectionId)
        .kafkaClusters
        .values()
        .stream()
        .findFirst()
        .map(ClusterInfo::spec);
  }


  /**
   * Find the cluster info for the schema registry that is associated with the given Kafka cluster,
   * accessible over the specified connection. Typically, the caller will first find the cluster
   * info for a Kafka cluster with a given ID, and then can find the cluster info for the
   * corresponding schema registry cluster, if there is such a schema registry.
   *
   * <p>For CCloud, all Kafka clusters in an environment use the one schema registry in that same
   * environment.
   *
   * @param kafkaCluster the info for the kafka cluster
   * @return the info for the schema registry that has the same scope as the given Kafka cluster, or
   * null if there is none
   * @throws ConnectionNotFoundException if there is no connection with the given ID
   * @throws ClusterNotFoundException    if the cluster was not found
   */
  public SchemaRegistry getSchemaRegistryForKafkaCluster(
      String connectionId,
      KafkaCluster kafkaCluster
  ) {
    return forConnection(connectionId).getSchemaRegistryForKafkaCluster(kafkaCluster);
  }

  /**
   * Find the cluster info for the schema registry that is associated with the given Kafka cluster,
   * if one exists. Else, return an empty Optional.
   *
   * @param connectionId   the ID of the connection
   * @param kafkaClusterId the ID of the Kafka cluster
   * @return the info for the schema registry that is associated with the given Kafka cluster
   * @throws ConnectionNotFoundException if there is no connection with the given ID
   */
  public Optional<SchemaRegistry> maybeGetSchemaRegistryForKafkaClusterId(
      String connectionId, String kafkaClusterId
  ) {
    return forConnection(connectionId).maybeSchemaRegistryForKafkaCluster(kafkaClusterId, true);
  }

  /**
   * Get the info for the Schema Registry with the given ID using the specified connection.
   *
   * @param connectionId the ID of the connection
   * @param clusterId    the ID of the schema registry
   * @return the info for the matching cluster, or null if none is found
   * @throws ConnectionNotFoundException if there is no connection with the given ID
   * @throws ClusterNotFoundException    if the cluster was not found
   */
  public SchemaRegistry getSchemaRegistry(String connectionId, String clusterId) {
    return forConnection(connectionId).getSchemaRegistry(clusterId);
  }

  public void clear() {
    clustersByConnectionId.clear();
  }

  /**
   * Get the connection clusters for the connection with the given ID.
   *
   * @param connectionId the ID of the connection
   * @return the connection clusters, or null if none is found
   * @throws ConnectionNotFoundException if there is no connection with the given ID
   */
  Clusters forConnection(String connectionId) {
    var cache = clustersByConnectionId.get(connectionId);
    if (cache == null) {
      throw new ConnectionNotFoundException(
          "Connection %s does not exist".formatted(connectionId)
      );
    }
    return cache;
  }

  void onConnectionCreated(@ObservesAsync @Lifecycle.Created ConnectionState connection) {
    Log.infof("Created %s", connection.getSpec());
    var spec = connection.getSpec();
    createOrGetConnectionClusters(spec.id(), spec.type());
  }

  void onConnectionUpdated(@ObservesAsync @Lifecycle.Updated ConnectionState connection) {
    directFetcher.clearClusterCache(connection.getId());
    Log.infof("Updated %s", connection.getSpec());

    // Replace the existing cache for this connection with a new one
    clustersByConnectionId.put(
        connection.getId(),
        new Clusters(connection.getId(), connection.getType())
    );
  }

  void onConnectionEstablished(
      @ObservesAsync @Lifecycle.Connected ConnectionState connection
  ) {
    Log.debugf("Connected %s", connection.getSpec());
  }

  void onConnectionDisconnected(
      @ObservesAsync @Lifecycle.Disconnected ConnectionState connection
  ) {
    Log.debugf("Disconnected %s", connection.getSpec());
  }

  void onConnectionDeleted(@ObservesAsync @Lifecycle.Deleted ConnectionState connection) {
    directFetcher.clearClusterCache(connection.getId());
    Log.infof("Deleted %s", connection.getSpec());

    // Remove it, in case it was not disconnected first
    clustersByConnectionId.remove(connection.getSpec().id());
  }

  void onLoadingKafkaCluster(
      @ObservesAsync @ClusterKind.Kafka ClusterEvent clusterEvent
  ) {
    Log.infof("Loaded Kafka Cluster %s", clusterEvent);
    var clusters = createOrGetConnectionClusters(
        clusterEvent.connectionId(),
        clusterEvent.connectionType()
    );
    clusters.add(clusterEvent.cluster());
  }

  void onLoadingSchemaRegistry(
      @ObservesAsync @ClusterKind.SchemaRegistry ClusterEvent clusterEvent
  ) {
    Log.infof("Loaded Schema Registry %s", clusterEvent);
    var clusters = createOrGetConnectionClusters(
        clusterEvent.connectionId(),
        clusterEvent.connectionType()
    );
    clusters.add(clusterEvent.cluster());
  }

  protected Clusters createOrGetConnectionClusters(
      String connectionId,
      ConnectionType type
  ) {
    return clustersByConnectionId.putIfAbsent(
        connectionId,
        new Clusters(connectionId, type)
    );
  }

  record ClusterInfo<SpecT extends Cluster>(
      String id,
      ClusterType type,
      SpecT spec,
      String path
  ) {

    public static final String BLANK_PATH = "";
  }

  class Clusters {

    private final String connectionId;
    private final ConnectionType connectionType;

    /**
     * This cluster information will have no TTL, since it only holds immutable information. But it
     * needs to be thread-safe since it will be updated from many threads.
     */
    final Map<String, ClusterInfo<KafkaCluster>> kafkaClusters = new ConcurrentHashMap<>();
    final Deque<ClusterInfo<SchemaRegistry>> schemaRegistries = new ConcurrentLinkedDeque<>();

    Clusters(@NotNull String connectionId, @NotNull ConnectionType connectionType) {
      this.connectionId = connectionId;
      this.connectionType = connectionType;
    }

    /**
     * Get the info for the {@link KafkaCluster} with the given ID and type.
     *
     * @param kafkaClusterId the ID of the Kafka cluster
     * @return the info for the matching cluster, or null if none is found
     * @throws ClusterNotFoundException if the cluster was not found
     */
    protected KafkaCluster getKafkaCluster(String kafkaClusterId) {
      return getKafkaCluster(kafkaClusterId, true);
    }

    /**
     * Get the info for the {@link KafkaCluster} with the given ID and type.
     *
     * @param kafkaClusterId the ID of the Kafka cluster
     * @param loadIfMissing  true if the cluster information should be loaded if it's not found
     * @return the info for the matching cluster, or null if none is found
     * @throws ClusterNotFoundException if the cluster was not found
     */
    @VisibleForTesting
    KafkaCluster getKafkaCluster(String kafkaClusterId, boolean loadIfMissing) {
      return findKafkaCluster(kafkaClusterId, loadIfMissing).spec();
    }

    /**
     * Get the info for the {@link SchemaRegistry} with the given ID and type.
     *
     * @param registryId the ID of the Schema Registry
     * @return the info for the matching cluster, or null if none is found
     * @throws ClusterNotFoundException if the schema registry was not found
     */
    protected SchemaRegistry getSchemaRegistry(String registryId) {
      return getSchemaRegistry(registryId, true);
    }

    /**
     * Get the info for the {@link SchemaRegistry} with the given ID and type.
     *
     * @param registryId    the ID of the Schema Registry
     * @param loadIfMissing true if the cluster information should be loaded if it's not found
     * @return the info for the matching cluster, or null if none is found
     * @throws ClusterNotFoundException if the schema registry was not found
     */
    @VisibleForTesting
    SchemaRegistry getSchemaRegistry(
        String registryId,
        boolean loadIfMissing
    ) {
      return findFirstSchemaRegistryWithId(registryId, loadIfMissing).spec();
    }

    /**
     * Find the info for the {@link SchemaRegistry} that is associated with the given Kafka cluster.
     * Typically, the caller will first
     * {@link #getKafkaCluster(String) find the Kafka cluster with a given ID}, and then use this
     * method to find the corresponding schema registry cluster, if there is such a schema
     * registry.
     *
     * @param kafkaCluster the {@link KafkaCluster}
     * @return the info for the schema registry that has the same scope as the given Kafka cluster,
     * or null if there is none
     * @throws ClusterNotFoundException if the schema registry was not found
     */
    public SchemaRegistry getSchemaRegistryForKafkaCluster(
        KafkaCluster kafkaCluster
    ) {
      return getSchemaRegistryForKafkaCluster(kafkaCluster.id(), true);
    }

    /**
     * Find the info for the {@link SchemaRegistry} that is associated with the given Kafka cluster.
     * Typically, the caller will first
     * {@link #getKafkaCluster(String) find the Kafka cluster with a given ID}, and then use this
     * method to find the corresponding schema registry cluster, if there is such a schema
     * registry.
     *
     * @param kafkaClusterId the ID of the kafka cluster
     * @param loadIfMissing  true if the cluster information should be loaded if it's not found
     * @return the info for the schema registry that has the same scope as the given Kafka cluster,
     * or null if there is none
     */
    public SchemaRegistry getSchemaRegistryForKafkaCluster(
        String kafkaClusterId,
        boolean loadIfMissing
    ) {
      return maybeSchemaRegistryForKafkaCluster(kafkaClusterId, loadIfMissing).orElseThrow(() ->
          new ClusterNotFoundException(
              "Schema Registry not found for Kafka Cluster %s in connection %s".formatted(
                  kafkaClusterId,
                  connectionId
              )
          )
      );
    }

    public Optional<SchemaRegistry> maybeSchemaRegistryForKafkaCluster(
        String kafkaClusterId,
        boolean loadIfMissing
    ) {
      // Find the path for this Kafka cluster
      var kafkaCluster = findKafkaCluster(kafkaClusterId, loadIfMissing);
      // Find the schema registry that has the same path as the Kafka cluster
      var result = findFirstSchemaRegistryWithPath(kafkaCluster.path(), loadIfMissing);
      if (result == null) {
        return Optional.empty();
      }
      return Optional.of(result.spec());
    }

    protected ClusterInfo<KafkaCluster> findKafkaCluster(
        String kafkaClusterId,
        boolean loadIfMissing
    ) {
      var result = kafkaClusters.get(kafkaClusterId);
      if (result == null && loadIfMissing) {
        // It's not found, so try to load it
        var newCluster = loadKafkaCluster(kafkaClusterId, loadTimeout);
        if (newCluster != null) {
          return add(newCluster);
        }
      }
      if (result == null) {
        throw new ClusterNotFoundException(
            "Kafka Cluster %s not found in connection %s".formatted(kafkaClusterId, connectionId)
        );
      }
      return result;
    }

    protected ClusterInfo<SchemaRegistry> findFirstSchemaRegistryWithPath(
        String path,
        boolean loadIfMissing
    ) {
      var result = schemaRegistries
          .stream()
          .filter(info -> info.path().equalsIgnoreCase(path))
          .findFirst()
          .orElse(null);
      if (result == null && loadIfMissing) {
        // Try and load the schema registry for this connection
        var registry = loadSchemaRegistryWithPath(path, loadTimeout);
        if (registry != null) {
          return add(registry);
        }
      }
      return result;
    }

    protected ClusterInfo<SchemaRegistry> findFirstSchemaRegistryWithId(
        String id,
        boolean loadIfMissing
    ) {
      var result = schemaRegistries
          .stream()
          .filter(info -> info.id().equalsIgnoreCase(id))
          .findFirst()
          .orElse(null);
      if (result == null && loadIfMissing) {
        // Try and load the schema registry for this connection
        var registry = loadSchemaRegistryWithId(id, loadTimeout);
        if (registry != null) {
          return add(registry);
        }
      }
      if (result == null) {
        throw new ClusterNotFoundException(
            "Schema Registry %s not found in connection %s".formatted(id, connectionId)
        );
      }
      return result;
    }

    protected void add(Cluster cluster) {
      if (cluster instanceof KafkaCluster kafkaCluster) {
        add(kafkaCluster);
      } else if (cluster instanceof SchemaRegistry registry) {
        add(registry);
      }
    }

    protected ClusterInfo<SchemaRegistry> add(SchemaRegistry registry) {
      var path = ClusterInfo.BLANK_PATH;
      if (registry instanceof CCloudSchemaRegistry lsrc) {
        path = lsrc.environment().id();
      }
      var info = new ClusterInfo<SchemaRegistry>(
          registry.id(),
          ClusterType.SCHEMA_REGISTRY,
          registry,
          path
      );
      schemaRegistries.add(info);
      return info;
    }

    protected ClusterInfo<KafkaCluster> add(KafkaCluster kafkaCluster) {
      var path = ClusterInfo.BLANK_PATH;
      if (kafkaCluster instanceof CCloudKafkaCluster lkc) {
        path = lkc.environment().id();
      }
      var info = new ClusterInfo<KafkaCluster>(
          kafkaCluster.id(),
          ClusterType.KAFKA,
          kafkaCluster,
          path
      );
      kafkaClusters.put(kafkaCluster.id(), info);
      return info;
    }

    protected KafkaCluster loadKafkaCluster(String clusterId, Duration timeout) {
      try {
        return switch (connectionType) {
          case CCLOUD -> ccloudFetcher.findKafkaCluster(connectionId, clusterId)
              .await()
              .atMost(timeout);
          case LOCAL -> localFetcher.getKafkaCluster(connectionId)
              .await()
              .atMost(timeout);
          case DIRECT -> realDirectFetcher.getKafkaCluster(connectionId)
              .await()
              .atMost(timeout);
          case PLATFORM -> null;
        };
      } catch (CompletionException e) {
        if (e.getCause() instanceof TimeoutException) {
          Log.infof(
              "Timed out waiting %s to load Kafka cluster %s",
              TimeUtil.humanReadableDuration(timeout),
              clusterId
          );
        } else {
          Log.errorf(e, "Failed to load Kafka cluster %s", clusterId);
        }
      }
      return null;
    }

    protected SchemaRegistry loadSchemaRegistryWithId(String id, Duration timeout) {
      try {
        return switch (connectionType) {
          case CCLOUD -> ccloudFetcher.findSchemaRegistry(connectionId, id)
              .await()
              .atMost(timeout);
          case LOCAL -> localFetcher.getSchemaRegistry(connectionId)
              .await()
              .atMost(timeout);
          case DIRECT -> realDirectFetcher.getSchemaRegistry(connectionId)
              .await()
              .atMost(timeout);
          case PLATFORM -> null;
        };
      } catch (CompletionException e) {
        if (e.getCause() instanceof TimeoutException) {
          Log.infof(
              "Timed out waiting %s to load Schema Registry %s",
              TimeUtil.humanReadableDuration(timeout),
              id
          );
        } else {
          Log.errorf(e, "Failed to load Schema Registry %s", id);
        }
      }
      return null;
    }

    protected SchemaRegistry loadSchemaRegistryWithPath(String path, Duration timeout) {
      try {
        return switch (connectionType) {
          case CCLOUD -> ccloudFetcher.getSchemaRegistry(connectionId, path)
              .await()
              .atMost(timeout);
          case LOCAL -> localFetcher.getSchemaRegistry(connectionId)
              .await()
              .atMost(timeout);
          case DIRECT -> realDirectFetcher.getSchemaRegistry(connectionId)
              .await()
              .atMost(timeout);
          case PLATFORM -> null;
        };
      } catch (CompletionException e) {
        if (e.getCause() instanceof TimeoutException) {
          Log.infof(
              "Timed out waiting %s to load Schema Registry (path='%s')",
              TimeUtil.humanReadableDuration(timeout),
              path
          );
        } else {
          Log.errorf(e, "Failed to load Schema Registry (path='%s')", path);
        }
      }
      return null;
    }
  }
}
