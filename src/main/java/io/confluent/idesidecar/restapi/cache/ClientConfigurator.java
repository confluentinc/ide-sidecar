/*
 * Copyright [2024 - 2024] Confluent Inc.
 */

package io.confluent.idesidecar.restapi.cache;

import io.confluent.idesidecar.restapi.connections.ConnectionState;
import io.confluent.idesidecar.restapi.connections.ConnectionStateManager;
import io.confluent.idesidecar.restapi.credentials.Credentials;
import io.confluent.idesidecar.restapi.exceptions.ClusterNotFoundException;
import io.confluent.idesidecar.restapi.exceptions.ConnectionNotFoundException;
import io.confluent.idesidecar.restapi.models.graph.KafkaCluster;
import io.confluent.idesidecar.restapi.models.graph.SchemaRegistry;
import io.confluent.idesidecar.restapi.util.CCloud;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.LinkedHashMap;
import java.util.Map;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class ClientConfigurator {

  /**
   * Generate the Kafka client configuration for a given connection and cluster.
   * This can optionally redact sensitive values in properties, such as if generating a
   * sample configuration for display or logging.
   *
   * <p>If a {@link SchemaRegistry} parameter is provided, then the resulting configuration
   * will include the necessary properties for connecting to the Schema Registry,
   * though configuration properties for Avro, Protobuf, or JSON Schema (de)serializers
   * are not included.
   *
   * @param connection        the connection
   * @param cluster           the Kafka cluster to use
   * @param sr                the Schema Registry to use, or null if not needed
   * @param redact            whether to redact sensitive properties
   * @param defaultProperties the default properties to use; may be overridden by computed values
   * @return the Kafka client configuration properties
   */
  static Map<String, Object> getKafkaClientConfig(
      ConnectionState connection,
      KafkaCluster cluster,
      SchemaRegistry sr,
      boolean redact,
      Map<String, String> defaultProperties
  ) {

    // Set AdminClient configs provided by the sidecar
    var props = new LinkedHashMap<String, Object>(defaultProperties);

    // First set the bootstrap servers
    props.put("bootstrap.servers", cluster.bootstrapServers());

    // Second, add any connection properties for Kafka cluster credentials (if defined)
    var options = connection.getKafkaConnectionOptions(cluster.id()).withRedact(redact);
    connection
        .getKafkaCredentials(cluster.id())
        .flatMap(creds -> creds.kafkaClientProperties(options))
        .ifPresent(props::putAll);

    // Add any auth properties for Schema Registry to the Kafka client config,
    // with the "schema.registry." prefix (unless the property already starts with that)
    if (sr != null) {
      var additional = getSchemaRegistryClientConfig(connection, sr, redact);
      additional.forEach((k, v) -> {
        if (k.startsWith("schema.registry.")) {
          props.put(k, v);
        } else {
          props.put("schema.registry." + k, v);
        }
      });
    }

    return props;
  }

  /**
   * Generate the Schema Registry client configuration for a given connection and Schema Registry.
   * This can optionally redact sensitive values in properties, such as if generating a
   * sample configuration for display or logging.
   *
   * @param connection the connection
   * @param sr         the Schema Registry to use, or null if not needed
   * @param redact     whether to redact sensitive properties
   * @return the Schema Registry client configuration properties
   */
  static Map<String, Object> getSchemaRegistryClientConfig(
      ConnectionState connection,
      SchemaRegistry sr,
      boolean redact
  ) {
    // Find the cluster using the connection ID, and fail if either does not exist
    var props = new LinkedHashMap<String, Object>();

    // First set the schema registry URL
    props.put("schema.registry.url", sr.uri());

    // CCloud requires the logical cluster ID to be set in the properties
    var logicalId = sr.logicalId().map(CCloud.LsrcId::id).orElse(null);

    // Add any properties for SR credentials (if defined)
    var options = new Credentials.SchemaRegistryConnectionOptions(redact, logicalId);
    connection
        .getSchemaRegistryCredentials(sr.id())
        .flatMap(creds -> creds.schemaRegistryClientProperties(options))
        .ifPresent(props::putAll);
    return props;
  }

  @Inject
  ConnectionStateManager connections;

  @Inject
  ClusterCache clusterCache;

  @ConfigProperty(name = "ide-sidecar.admin-client-configs")
  Map<String, String> adminClientSidecarConfigs;

  /**
   * Get the AdminClient configuration for connection and Kafka cluster with the specified IDs.
   * This method looks up the {@link ConnectionState} and {@link KafkaCluster} objects,
   * and will throw exceptions if the connection or cluster does not exist.
   *
   * <p> This can optionally redact sensitive values in properties, such as if generating a
   * sample configuration for display or logging.
   *
   * @param connectionId the ID of the connection
   * @param clusterId    the ID of the Kafka cluster
   * @param redact       whether to redact sensitive properties
   * @return the AdminClient configuration properties
   * @throws ConnectionNotFoundException if the connection does not exist
   * @throws ClusterNotFoundException    if the cluster does not exist
   * @see #getKafkaClientConfig(ConnectionState, KafkaCluster, SchemaRegistry, boolean, Map)
   */
  public Map<String, Object> getAdminClientConfig(
      String connectionId,
      String clusterId,
      boolean redact
  ) throws ConnectionNotFoundException, ClusterNotFoundException {
    // Find the connection and cluster, or fail if either does not exist
    var connection = connections.getConnectionState(connectionId);
    var cluster = clusterCache.getKafkaCluster(connectionId, clusterId);
    // Return the AdminClient config
    return getKafkaClientConfig(
        connection,
        cluster,
        null,
        redact,
        adminClientSidecarConfigs
    );
  }

  /**
   * Get the KafkaConsumer configuration for connection and Kafka cluster with the specified IDs.
   * This method looks up the {@link ConnectionState} and {@link KafkaCluster} objects,
   * and will throw exceptions if the connection or cluster does not exist.
   *
   * <p> This can optionally redact sensitive values in properties, such as if generating a
   * sample configuration for display or logging.
   *
   * @param connectionId          the ID of the connection
   * @param clusterId             the ID of the Kafka cluster
   * @param includeSchemaRegistry whether to include configuration properties for Schema Registry
   * @param redact                whether to redact sensitive properties
   * @return the AdminClient configuration properties
   * @throws ConnectionNotFoundException if the connection does not exist
   * @throws ClusterNotFoundException    if the cluster does not exist
   * @see #getKafkaClientConfig(ConnectionState, KafkaCluster, SchemaRegistry, boolean, Map)
   */
  public Map<String, Object> getConsumerClientConfig(
      String connectionId,
      String clusterId,
      boolean includeSchemaRegistry,
      boolean redact
  ) throws ConnectionNotFoundException, ClusterNotFoundException {
    return getKafkaClientConfig(
        connectionId,
        clusterId,
        includeSchemaRegistry,
        redact,
        Map.of(
            "session.timeout.ms", "45000"
        )
    );
  }

  /**
   * Get the KafkaProducer configuration for connection and Kafka cluster with the specified IDs.
   * This method looks up the {@link ConnectionState} and {@link KafkaCluster} objects,
   * and will throw exceptions if the connection or cluster does not exist.
   *
   * <p> This can optionally redact sensitive values in properties, such as if generating a
   * sample configuration for display or logging.
   *
   * @param connectionId          the ID of the connection
   * @param clusterId             the ID of the Kafka cluster
   * @param includeSchemaRegistry whether to include configuration properties for Schema Registry
   * @param redact                whether to redact sensitive properties
   * @return the AdminClient configuration properties
   * @throws ConnectionNotFoundException if the connection does not exist
   * @throws ClusterNotFoundException    if the cluster does not exist
   * @see #getKafkaClientConfig(ConnectionState, KafkaCluster, SchemaRegistry, boolean, Map)
   */
  public Map<String, Object> getProducerClientConfig(
      String connectionId,
      String clusterId,
      boolean includeSchemaRegistry,
      boolean redact
  ) throws ConnectionNotFoundException, ClusterNotFoundException {
    return getKafkaClientConfig(
        connectionId,
        clusterId,
        includeSchemaRegistry,
        redact,
        Map.of("acks", "all")
    );
  }

  protected Map<String, Object> getKafkaClientConfig(
      String connectionId,
      String clusterId,
      boolean includeSchemaRegistry,
      boolean redact,
      Map<String, String> defaultProperties
  ) throws ConnectionNotFoundException, ClusterNotFoundException {
    // Find the connection and cluster, or fail if either does not exist
    var connection = connections.getConnectionState(connectionId);
    var cluster = clusterCache.getKafkaCluster(connectionId, clusterId);

    // Maybe look up the SR for the Kafka cluster
    SchemaRegistry sr = null;
    if (includeSchemaRegistry) {
      sr = clusterCache.getSchemaRegistryForKafkaCluster(connectionId, cluster);
    }

    // Get the basic producer config
    return getKafkaClientConfig(
        connection,
        cluster,
        sr,
        redact,
        defaultProperties
    );
  }

  /**
   * Get the client configuration for connection and Schema Registry with the specified IDs.
   * This method looks up the {@link ConnectionState} and {@link KafkaCluster} objects,
   * and will throw exceptions if the connection or cluster does not exist.
   *
   * <p> This can optionally redact sensitive values in properties, such as if generating a
   * sample configuration for display or logging.
   *
   * @param connectionId     the ID of the connection
   * @param schemaRegistryId the ID of the Schema Registry cluster
   * @param redact           whether to redact sensitive properties
   * @return the AdminClient configuration properties
   * @throws ConnectionNotFoundException if the connection does not exist
   * @throws ClusterNotFoundException    if the cluster does not exist
   * @see #getSchemaRegistryClientConfig(ConnectionState, SchemaRegistry, boolean)
   */
  public Map<String, Object> getSchemaRegistryClientConfig(
      String connectionId,
      String schemaRegistryId,
      boolean redact
  ) throws ConnectionNotFoundException, ClusterNotFoundException {
    // Find the cluster using the connection ID, and fail if either does not exist
    var sr = clusterCache.getSchemaRegistry(connectionId, schemaRegistryId);
    var connection = connections.getConnectionState(connectionId);
    return getSchemaRegistryClientConfig(connection, sr, redact);
  }
}
