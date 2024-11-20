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
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;
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
   * @param bootstrapServers  the bootstrap servers of the Kafka cluster to use
   * @param srUri             the URI of the Schema Registry to use, or null if not needed
   * @param redact            whether to redact sensitive properties
   * @param timeout           the timeout for calls to the cluster
   * @param defaultProperties the default properties to use; may be overridden by computed values
   * @return the Kafka client configuration properties
   */
  public static Map<String, Object> getKafkaClientConfig(
      ConnectionState connection,
      String bootstrapServers,
      String srUri,
      boolean redact,
      Duration timeout,
      Map<String, String> defaultProperties
  ) {

    // Set AdminClient configs provided by the sidecar
    var props = new LinkedHashMap<String, Object>(defaultProperties);

    // First set the bootstrap servers
    props.put("bootstrap.servers", bootstrapServers);

    // Second, add any connection properties for Kafka cluster credentials (if defined)
    var options = connection.getKafkaConnectionOptions().withRedact(redact);
    connection
        .getKafkaCredentials()
        .flatMap(creds -> creds.kafkaClientProperties(options))
        .ifPresent(props::putAll);

    // Add any auth properties for Schema Registry to the Kafka client config,
    // with the "schema.registry." prefix (unless the property already starts with that)
    if (srUri != null) {
      var additional = getSchemaRegistryClientConfig(connection, srUri, redact, timeout);
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
   * @param connection     the connection
   * @param srUri          the URI of the Schema Registry to use
   * @param redact         whether to redact sensitive properties
   * @param defaultTimeout the timeout for calls to the Schema Registry
   * @return the Schema Registry client configuration properties
   */
  public static Map<String, Object> getSchemaRegistryClientConfig(
      ConnectionState connection,
      String srUri,
      boolean redact,
      Duration defaultTimeout
  ) {
    // Find the cluster using the connection ID, and fail if either does not exist
    var props = new LinkedHashMap<String, Object>();

    // First set the schema registry URL
    props.put("schema.registry.url", srUri);

    if (defaultTimeout != null) {
      props.put("schema.registry.request.timeout.ms", defaultTimeout.toMillis());
    }

    // CCloud requires the logical cluster ID to be set in the properties, so examine the URL
    var logicalId = CCloud.SchemaRegistryIdentifier
        .parse(srUri)
        .map(CCloud.LsrcId.class::cast)
        .map(CCloud.LsrcId::id)
        .orElse(null);

    // Add any properties for SR credentials (if defined)
    var options = new Credentials.SchemaRegistryConnectionOptions(redact, logicalId);
    connection
        .getSchemaRegistryCredentials()
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

  public static class Configuration {
    final Supplier<Map<String, Object>> configSupplier;
    final Supplier<Map<String, Object>> redactedSupplier;
    final Map<String, Object> overrides = new LinkedHashMap<>();

    Configuration(
        Supplier<Map<String, Object>> configSupplier,
        Supplier<Map<String, Object>> redactedSupplier
    ) {
      this.configSupplier = configSupplier;
      this.redactedSupplier = redactedSupplier;
    }

    public Map<String, Object> asMap() {
      var result = configSupplier.get();
      result.putAll(overrides);
      return result;
    }

    public Map<String, Object> asRedacted() {
      var result = redactedSupplier.get();
      result.putAll(overrides);
      return result;
    }

    public Configuration put(String key, Object value) {
      overrides.put(key, value);
      return this;
    }

    public String toString() {
      return toString("  ");
    }

    public String toString(String prefix) {
      return asRedacted()
          .toString()
          .replaceAll(",\\s*", ",\n" + prefix)
          .replaceAll("[{}\\[\\]]", "");
    }
  }

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
   * @return the AdminClient configuration properties
   * @throws ConnectionNotFoundException if the connection does not exist
   * @throws ClusterNotFoundException    if the cluster does not exist
   * @see #getKafkaClientConfig
   */
  public Configuration getAdminClientConfig(
      String connectionId,
      String clusterId
  ) throws ConnectionNotFoundException, ClusterNotFoundException {
    // Find the connection and cluster, or fail if either does not exist
    var connection = connections.getConnectionState(connectionId);
    var cluster = clusterCache.getKafkaCluster(connectionId, clusterId);
    // Return the AdminClient config
    return new Configuration(
        () -> getKafkaClientConfig(
            connection,
            cluster.bootstrapServers(),
            null,
            false,
            null,
            adminClientSidecarConfigs
        ),
        () -> getKafkaClientConfig(
            connection,
            cluster.bootstrapServers(),
            null,
            true,
            null,
            adminClientSidecarConfigs
        )
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
   * @return the AdminClient configuration properties
   * @throws ConnectionNotFoundException if the connection does not exist
   * @throws ClusterNotFoundException    if the cluster does not exist
   * @see #getKafkaClientConfig
   */
  public Configuration getConsumerClientConfig(
      String connectionId,
      String clusterId,
      boolean includeSchemaRegistry
  ) throws ConnectionNotFoundException, ClusterNotFoundException {
    var defaults = Map.of(
        "session.timeout.ms", "45000"
    );
    return new Configuration(
        () -> getKafkaClientConfig(
            connectionId,
            clusterId,
            includeSchemaRegistry,
            false,
            defaults
        ),
        () -> getKafkaClientConfig(
            connectionId,
            clusterId,
            includeSchemaRegistry,
            true,
            defaults
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
   * @return the AdminClient configuration properties
   * @throws ConnectionNotFoundException if the connection does not exist
   * @throws ClusterNotFoundException    if the cluster does not exist
   * @see #getKafkaClientConfig
   */
  public Configuration getProducerClientConfig(
      String connectionId,
      String clusterId,
      boolean includeSchemaRegistry
  ) throws ConnectionNotFoundException, ClusterNotFoundException {
    var defaults = Map.of(
        "acks", "all"
    );
    return new Configuration(
        () -> getKafkaClientConfig(
            connectionId,
            clusterId,
            includeSchemaRegistry,
            false,
            defaults
        ),
        () -> getKafkaClientConfig(
            connectionId,
            clusterId,
            includeSchemaRegistry,
            true,
            defaults
        )
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
      if (sr != null) {
        Log.debugf("Using Schema Registry %s for Kafka cluster %s", sr.id(), cluster.id());
      } else {
        Log.debugf("Found no Schema Registry for Kafka cluster %s", cluster.id());
      }
    } else {
      Log.debugf("Not using Schema Registry for Kafka cluster %s", cluster.id());
    }

    // Get the basic producer config
    return getKafkaClientConfig(
        connection,
        cluster.bootstrapServers(),
        sr != null ? sr.uri() : null,
        redact,
        null,
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
   * @return the AdminClient configuration properties
   * @throws ConnectionNotFoundException if the connection does not exist
   * @throws ClusterNotFoundException    if the cluster does not exist
   * @see #getSchemaRegistryClientConfig
   */
  public Configuration getSchemaRegistryClientConfig(
      String connectionId,
      String schemaRegistryId
  ) throws ConnectionNotFoundException, ClusterNotFoundException {
    // Find the cluster using the connection ID, and fail if either does not exist
    var sr = clusterCache.getSchemaRegistry(connectionId, schemaRegistryId);
    var connection = connections.getConnectionState(connectionId);
    return new Configuration(
        () -> getSchemaRegistryClientConfig(
            connection,
            sr.uri(),
            false,
            null
        ),
        () -> getSchemaRegistryClientConfig(
            connection,
            sr.uri(),
            true,
            null
        )
    );
  }
}
