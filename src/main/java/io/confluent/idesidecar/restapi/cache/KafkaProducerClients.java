package io.confluent.idesidecar.restapi.cache;

import com.github.benmanes.caffeine.cache.CaffeineSpec;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

@ApplicationScoped
public class KafkaProducerClients extends Clients<KafkaProducer<byte[], byte[]>> {
  // Evict cached Producer instances after 5 minutes of inactivity
  private static final String CAFFEINE_SPEC = "expireAfterAccess=5m";

  private static final ByteArraySerializer BYTE_ARRAY_SERIALIZER = new ByteArraySerializer();

  @Inject
  ClientConfigurator configurator;

  public KafkaProducerClients() {
    super(CaffeineSpec.parse(CAFFEINE_SPEC));
  }

  public KafkaProducer<byte[], byte[]> getClient(String connectionId, String clusterId) {
    return getClient(
        connectionId,
        clusterId,
        () -> {
          // Generate the Kafka producer configuration
          var config = configurator.getProducerClientConfig(
              connectionId,
              clusterId,
              // Don't include SR config since we're
              // passing the ByteArraySerializer for keys and values
              false
          );
          // Create the producer
          Log.debugf(
              "Creating producer client for connection %s and cluster %s with configuration:\n  %s",
              connectionId,
              clusterId,
              config
          );
          return new KafkaProducer<>(config.asMap(), BYTE_ARRAY_SERIALIZER, BYTE_ARRAY_SERIALIZER);
        }
    );
  }
}
