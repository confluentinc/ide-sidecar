package io.confluent.idesidecar.restapi.clients;

import io.confluent.idesidecar.restapi.cache.ClusterCache;
import io.confluent.idesidecar.restapi.connections.ConnectionState;
import io.confluent.idesidecar.restapi.connections.ConnectionStateManager;
import io.confluent.idesidecar.restapi.exceptions.ClusterNotFoundException;
import io.confluent.idesidecar.restapi.exceptions.ConnectionNotFoundException;
import io.confluent.idesidecar.restapi.kafkarest.SchemaManager;
import io.confluent.idesidecar.restapi.models.graph.KafkaCluster;
import io.confluent.idesidecar.restapi.models.graph.SchemaRegistry;
import io.confluent.idesidecar.restapi.util.ConfigUtil;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.kafka.clients.CommonClientConfigs;

@ApplicationScoped
public class ClientConfigurator {

  public static final Map<String, String> DEFAULT_ADMIN_CLIENT_CONFIGS = ConfigUtil
      .asMap("ide-sidecar.admin-client-configs");

  public static final Map<String, String> DEFAULT_CONSUMER_CLIENT_CONFIGS = ConfigUtil
      .asMap("ide-sidecar.consumer-client-configs");

  public static final Map<String, String> DEFAULT_PRODUCER_CLIENT_CONFIGS = ConfigUtil
      .asMap("ide-sidecar.producer-client-configs");

  private static final Map<String, String> SERDE_CONFIGS = ConfigUtil
      .asMap("ide-sidecar.serde-configs");

  /**
   * An object that has a Kafka or SR client configuration, which can be retrieved as a map or as a
   * redacted map.
   */
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

    /**
     * Obtain the raw configuration properties as a map.
     *
     * @return the configuration properties
     */
    public Map<String, Object> asMap() {
      var result = configSupplier.get();
      result.putAll(overrides);
      return result;
    }

    /**
     * Obtain the configuration properties, with all secrets redacted, as a map.
     *
     * @return the redacted configuration properties
     */
    public Map<String, Object> asRedacted() {
      var result = redactedSupplier.get();
      result.putAll(overrides);
      return result;
    }

    public Configuration put(String key, Object value) {
      overrides.put(key, value);
      return this;
    }

    /**
     * Display the redacted, multi-line form of the configuration, suitable for logging or display.
     *
     * @return the string representation of the redacted configuration
     */
    public String toString() {
      return toString("  ");
    }

    /**
     * Display the redacted, multi-line form of the configuration, suitable for logging or display.
     *
     * @param prefix the prefix to use for all but the first line
     * @return the string representation of the redacted configuration
     */
    public String toString(String prefix) {
      return asRedacted()
          .toString()
          .replaceAll(",\\s*", ",\n" + prefix)
          .replaceAll("[{}\\[\\]]", "");
    }
  }

  /**
   * Generate the Kafka admin client configuration for a given connection and cluster. This can
   * optionally redact sensitive values in properties, such as if generating a sample configuration
   * for display or logging.
   *
   * @param connection       the connection with the Kafka endpoint information
   * @param bootstrapServers the bootstrap servers of the Kafka cluster to use
   * @param timeout          the timeout for calls to the cluster
   * @return the Kafka admin client configuration
   */
  public static Configuration getKafkaAdminClientConfig(
      ConnectionState connection,
      String bootstrapServers,
      Duration timeout
  ) {
    return new Configuration(
        () -> getKafkaClientConfig(
            connection,
            bootstrapServers,
            null,
            false,
            timeout,
            DEFAULT_ADMIN_CLIENT_CONFIGS
        ),
        () -> getKafkaClientConfig(
            connection,
            bootstrapServers,
            null,
            true,
            timeout,
            DEFAULT_ADMIN_CLIENT_CONFIGS
        )
    );
  }

  /**
   * Generate the Kafka consumer configuration for a given connection and cluster. This can
   * optionally redact sensitive values in properties, such as if generating a sample configuration
   * for display or logging.
   *
   * <p>If a {@link SchemaRegistry} parameter is provided, then the resulting configuration
   * will include the necessary properties for connecting to the Schema Registry, though
   * configuration properties for Avro, Protobuf, or JSON Schema (de)serializers are not included.
   *
   * @param connection       the connection with the Kafka endpoint information
   * @param bootstrapServers the bootstrap servers of the Kafka cluster to use
   * @param srUri            the URI of the Schema Registry to use, or null if not needed
   * @param timeout          the timeout for calls to the cluster
   * @return the Kafka consumer configuration
   */
  public static Configuration getKafkaConsumerConfig(
      ConnectionState connection,
      String bootstrapServers,
      String srUri,
      Duration timeout
  ) {
    return new Configuration(
        () -> getKafkaClientConfig(
            connection,
            bootstrapServers,
            srUri,
            false,
            timeout,
            DEFAULT_CONSUMER_CLIENT_CONFIGS
        ),
        () -> getKafkaClientConfig(
            connection,
            bootstrapServers,
            srUri,
            true,
            timeout,
            DEFAULT_CONSUMER_CLIENT_CONFIGS
        )
    );
  }

  /**
   * Generate the Kafka producer configuration for a given connection and cluster. This can
   * optionally redact sensitive values in properties, such as if generating a sample configuration
   * for display or logging.
   *
   * <p>If a {@link SchemaRegistry} parameter is provided, then the resulting configuration
   * will include the necessary properties for connecting to the Schema Registry, though
   * configuration properties for Avro, Protobuf, or JSON Schema (de)serializers are not included.
   *
   * @param connection       the connection with the Kafka endpoint information
   * @param bootstrapServers the bootstrap servers of the Kafka cluster to use
   * @param srUri            the URI of the Schema Registry to use, or null if not needed
   * @param timeout          the timeout for calls to the cluster
   * @return the Kafka producer configuration
   */
  public static Configuration getKafkaProducerConfig(
      ConnectionState connection,
      String bootstrapServers,
      String srUri,
      Duration timeout
  ) {
    return new Configuration(
        () -> getKafkaClientConfig(
            connection,
            bootstrapServers,
            srUri,
            false,
            timeout,
            DEFAULT_PRODUCER_CLIENT_CONFIGS
        ),
        () -> getKafkaClientConfig(
            connection,
            bootstrapServers,
            srUri,
            true,
            timeout,
            DEFAULT_PRODUCER_CLIENT_CONFIGS
        )
    );
  }

  /**
   * Generate the Kafka client configuration for a given connection and cluster. This can optionally
   * redact sensitive values in properties, such as if generating a sample configuration for display
   * or logging.
   *
   * <p>If a {@link SchemaRegistry} parameter is provided, then the resulting configuration
   * will include the necessary properties for connecting to the Schema Registry, though
   * configuration properties for Avro, Protobuf, or JSON Schema (de)serializers are not included.
   *
   * @param connection        the connection with the Kafka endpoint information
   * @param bootstrapServers  the bootstrap servers of the Kafka cluster to use
   * @param srUri             the URI of the Schema Registry to use, or null if not needed
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
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

    // Second, add any connection properties for Kafka cluster credentials (if defined)
    var options = connection.getKafkaConnectionOptions().withRedact(redact);

    var tlsConfig = connection.getKafkaTLSConfig();
    if (tlsConfig.isPresent() && tlsConfig.get().enabled()) {
      // This may be overridden based on the type of credentials used
      props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SSL");
      tlsConfig.get().getProperties(redact).ifPresent(props::putAll);
      options = options.withTlsConfig(tlsConfig.get());
    }

    if (connection.getKafkaCredentials().isPresent()) {
      var finalOptions = options;
      connection
          .getKafkaCredentials()
          .flatMap(creds -> creds.kafkaClientProperties(finalOptions))
          .ifPresent(props::putAll);
    }

    // Add any auth properties for Schema Registry to the Kafka client config,
    // with the "schema.registry." prefix (unless the property already starts with that)
    if (srUri != null) {
      var additional = getSchemaRegistryClientConfig(connection, srUri, redact, timeout);
      additional.forEach((k, v) -> {
        if (k.startsWith(SchemaRegistryClientConfig.CLIENT_NAMESPACE)) {
          props.put(k, v);
        } else {
          props.put(SchemaRegistryClientConfig.CLIENT_NAMESPACE + k, v);
        }
      });
    }

    return props;
  }

  /**
   * Generate the Schema Registry client configuration for a given connection and Schema Registry.
   * This can optionally redact sensitive values in properties, such as if generating a sample
   * configuration for display or logging.
   *
   * @param connection     the connection with the Schema Registry endpoint information
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
    props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, srUri);

    if (defaultTimeout != null) {
      props.put("schema.registry.request.timeout.ms", defaultTimeout.toMillis());
    }

    // Add any properties for SR credentials (if defined)
    var options = connection
        .getSchemaRegistryOptions()
        .withRedact(redact);

    var tlsConfig = connection.getSchemaRegistryTLSConfig();
    if (tlsConfig.isPresent() && tlsConfig.get().enabled()) {
      tlsConfig.get().getProperties(redact).ifPresent(props::putAll);
      options = options.withTlsConfig(tlsConfig.get());
    }

    if (connection.getSchemaRegistryCredentials().isPresent()) {
      var finalOptions = options;
      connection
          .getSchemaRegistryCredentials()
          .flatMap(creds -> creds.schemaRegistryClientProperties(finalOptions))
          .ifPresent(props::putAll);
    }

    return props;
  }

  @Inject
  ConnectionStateManager connections;

  @Inject
  ClusterCache clusterCache;

  /**
   * Get the AdminClient configuration for connection and Kafka cluster with the specified IDs. This
   * method looks up the {@link ConnectionState} and {@link KafkaCluster} objects, and will throw
   * exceptions if the connection or cluster does not exist.
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
    return getKafkaAdminClientConfig(
        connection,
        cluster.bootstrapServers(),
        null
    );
  }

  /**
   * Get the KafkaConsumer configuration for connection and Kafka cluster with the specified IDs.
   * This method looks up the {@link ConnectionState} and {@link KafkaCluster} objects, and will
   * throw exceptions if the connection or cluster does not exist.
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
    // Find the connection and cluster, or fail if either does not exist
    var connection = connections.getConnectionState(connectionId);
    var cluster = clusterCache.getKafkaCluster(connectionId, clusterId);

    // Maybe look up the SR for the Kafka cluster
    SchemaRegistry sr;
    if (includeSchemaRegistry) {
      sr = clusterCache.getSchemaRegistryForKafkaCluster(connectionId, cluster);
      if (sr != null) {
        Log.debugf("Using Schema Registry %s for Kafka cluster %s", sr.id(), cluster.id());
      } else {
        Log.debugf("Found no Schema Registry for Kafka cluster %s", cluster.id());
      }
    } else {
      sr = null;
      Log.debugf("Not using Schema Registry for Kafka cluster %s", cluster.id());
    }

    return getKafkaConsumerConfig(
        connection,
        cluster.bootstrapServers(),
        sr != null ? sr.uri() : null,
        null
    );
  }

  /**
   * Get the KafkaProducer configuration for connection and Kafka cluster with the specified IDs.
   * This method looks up the {@link ConnectionState} and {@link KafkaCluster} objects, and will
   * throw exceptions if the connection or cluster does not exist.
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
    // Find the connection and cluster, or fail if either does not exist
    var connection = connections.getConnectionState(connectionId);
    var cluster = clusterCache.getKafkaCluster(connectionId, clusterId);

    // Maybe look up the SR for the Kafka cluster
    SchemaRegistry sr;
    if (includeSchemaRegistry) {
      sr = clusterCache.getSchemaRegistryForKafkaCluster(connectionId, cluster);
      if (sr != null) {
        Log.debugf("Using Schema Registry %s for Kafka cluster %s", sr.id(), cluster.id());
      } else {
        Log.debugf("Found no Schema Registry for Kafka cluster %s", cluster.id());
      }
    } else {
      sr = null;
      Log.debugf("Not using Schema Registry for Kafka cluster %s", cluster.id());
    }

    return getKafkaProducerConfig(
        connection,
        cluster.bootstrapServers(),
        sr != null ? sr.uri() : null,
        null
    );
  }

  /**
   * Get the client configuration for connection and Schema Registry with the specified IDs. This
   * method looks up the {@link ConnectionState} and {@link KafkaCluster} objects, and will throw
   * exceptions if the connection or cluster does not exist.
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

  /**
   * Get the Kafka Serializer/Deserializer configuration for a given
   * {@link SchemaManager.RegisteredSchema}, or the default configuration if no schema is provided.
   *
   * @param schema the schema to use, if present
   * @param isKey  whether the schema is for a key or value
   * @return the Serde configuration properties as a map
   */
  public Map<String, String> getSerdeConfigs(
      Optional<SchemaManager.RegisteredSchema> schema,
      boolean isKey
  ) {
    if (schema.isEmpty()) {
      return SERDE_CONFIGS;
    }

    var configs = new LinkedHashMap<>(SERDE_CONFIGS);
    configs.put(
        (isKey ? "key.subject.name.strategy" : "value.subject.name.strategy"),
        schema.get().subjectNameStrategy().className()
    );

    // No need to pass SR auth properties since it will hit
    // the SR REST proxy in the sidecar, which handles any
    // necessary auth.
    return configs;
  }
}
