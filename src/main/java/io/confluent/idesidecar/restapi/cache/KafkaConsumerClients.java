package io.confluent.idesidecar.restapi.cache;

import com.github.benmanes.caffeine.cache.CaffeineSpec;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

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
