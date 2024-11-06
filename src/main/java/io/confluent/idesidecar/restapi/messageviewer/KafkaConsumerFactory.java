package io.confluent.idesidecar.restapi.messageviewer;

import io.confluent.idesidecar.restapi.cache.ClusterCache;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

@ApplicationScoped
public class KafkaConsumerFactory {
  private static final ByteArrayDeserializer BYTE_ARRAY_DESERIALIZER = new ByteArrayDeserializer();

  @Inject
  ClusterCache clusterCache;

  /**
   * Create a new Kafka consumer client for the given connection and cluster, with the given
   * configuration overrides. The caller is responsible for closing the client when done.
   * @param connectionId    the ID of the connection to use
   * @param clusterId       the ID of the Kafka cluster to connect to
   * @param configOverrides additional configuration properties to apply
   * @return a new Kafka consumer client
   */
  public KafkaConsumer<byte[], byte[]> getClient(
      String connectionId,
      String clusterId,
      Properties configOverrides
  ) {
    // Get the consumer config for the given connection and cluster
    var config = getConsumerConfig(connectionId, clusterId);
    // And apply the overrides
    config.putAll(configOverrides);
    return new KafkaConsumer<>(config, BYTE_ARRAY_DESERIALIZER, BYTE_ARRAY_DESERIALIZER);
  }

  private Properties getConsumerConfig(String connectionId, String kafkaClusterId) {
    var kafkaCluster = clusterCache.getKafkaCluster(connectionId, kafkaClusterId);
    var props = new Properties();
    props.put("bootstrap.servers", kafkaCluster.bootstrapServers());
    return props;
  }
}
