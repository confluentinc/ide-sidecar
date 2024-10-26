package io.confluent.idesidecar.restapi.cache;

import com.github.benmanes.caffeine.cache.CaffeineSpec;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionRequest;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/**
 * Utilities to obtain and cache KafkaConsumer instances for a given connection and cluster ID.
 * For the client id (id by which consumer instances are cached), we
 * construct a composite key of the cluster ID and a hash of the
 * config overrides to cache consumers per cluster and configuration.
 * These config overrides are informed by the consume request body from the
 * {@link SimpleConsumeMultiPartitionRequest#consumerConfigOverrides()}
 * method and may include the following properties:
 * <ul>
 *  <li>{@code max.poll.records}</li>
 *  <li>{@code fetch.max.bytes}</li>
 * </ul>
 * Caching the consumers this way allows for its reuse when a user is consuming
 * topic messages using the extension's Message Viewer without changing any request parameters.
 * Conversely, if the user changes the request parameters, a new consumer instance will be created
 * with the provided parameters. <b>In short, we'd always create (or reuse if cached)
 * a consumer instance that respects the request parameters.</b>
 */
@ApplicationScoped
public class KafkaConsumerClients extends Clients<KafkaConsumer<byte[], byte[]>> {
  // Evict cached Consumer instances after 5 minutes of inactivity
  private static final String CAFFEINE_SPEC = "expireAfterAccess=5m";

  private static final ByteArrayDeserializer BYTE_ARRAY_DESERIALIZER = new ByteArrayDeserializer();

  @Inject
  ClusterCache clusterCache;

  public KafkaConsumerClients() {
    super(CaffeineSpec.parse(CAFFEINE_SPEC));
  }

  /**
   * Get a KafkaConsumer for the given connection and cluster ID. If the consumer does not
   * already exist, it will be created using the provided factory. See the class-level
   * documentation for more information on how the cache key is determined.
   * @param connectionId     the ID of the connection
   * @param clusterId        the ID of the Kafka cluster
   * @param configOverrides  additional configuration properties to apply to the consumer based
   *                         on the request parameters
   * @return                 a KafkaConsumer instance
   */
  public KafkaConsumer<byte[], byte[]> getClient(
      String connectionId,
      String clusterId,
      Properties configOverrides
  ) {
    String cacheKey = clusterId;
    if (!configOverrides.isEmpty()) {
      // Cache the consumer by the cluster ID and the hash of the config overrides
      cacheKey = clusterId + "-" + Integer.toHexString(configOverrides.hashCode());
    }

    // Get the consumer config for the given connection and cluster
    var config = getConsumerConfig(connectionId, clusterId);
    // And apply the overrides
    config.putAll(configOverrides);

    return getClient(
        connectionId,
        cacheKey,
        () -> new KafkaConsumer<>(config, BYTE_ARRAY_DESERIALIZER, BYTE_ARRAY_DESERIALIZER)
    );
  }

  private Properties getConsumerConfig(String connectionId, String kafkaClusterId) {
    var kafkaCluster = clusterCache.getKafkaCluster(connectionId, kafkaClusterId);
    var props = new Properties();
    props.put("bootstrap.servers", kafkaCluster.bootstrapServers());
    return props;
  }
}
