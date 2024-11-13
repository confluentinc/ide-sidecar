package io.confluent.idesidecar.restapi.clients;

import com.github.benmanes.caffeine.cache.CaffeineSpec;
import io.confluent.idesidecar.restapi.cache.Clients;
import io.confluent.idesidecar.restapi.cache.ClusterCache;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

@ApplicationScoped
public class KafkaProducerClients extends Clients<KafkaProducer<byte[], byte[]>> {
  // Evict cached Producer instances after 5 minutes of inactivity
  private static final String CAFFEINE_SPEC = "expireAfterAccess=5m";

  private static final ByteArraySerializer BYTE_ARRAY_SERIALIZER = new ByteArraySerializer();

  @Inject
  ClusterCache clusterCache;

  public KafkaProducerClients() {
    super(CaffeineSpec.parse(CAFFEINE_SPEC));
  }

  public KafkaProducer<byte[], byte[]> getClient(String connectionId, String clusterId) {
    return getClient(
        connectionId,
        clusterId,
        () -> new KafkaProducer<>(
            getProducerConfig(connectionId, clusterId),
            BYTE_ARRAY_SERIALIZER,
            BYTE_ARRAY_SERIALIZER
        )
    );
  }

  private Properties getProducerConfig(String connectionId, String kafkaClusterId) {
    var kafkaCluster = clusterCache.getKafkaCluster(connectionId, kafkaClusterId);
    var props = new Properties();
    props.put("bootstrap.servers", kafkaCluster.bootstrapServers());
    clusterCache.maybeGetSchemaRegistryForKafkaClusterId(connectionId, kafkaClusterId)
        .ifPresent(sr -> props.put("schema.registry.url", sr.uri()));
    return props;
  }
}
