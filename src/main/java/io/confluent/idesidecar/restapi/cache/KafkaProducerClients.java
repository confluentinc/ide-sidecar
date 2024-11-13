package io.confluent.idesidecar.restapi.cache;

import com.github.benmanes.caffeine.cache.CaffeineSpec;
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
              true,
              false
          );
          // Create the producer
          return new KafkaProducer<>(config, BYTE_ARRAY_SERIALIZER, BYTE_ARRAY_SERIALIZER);
        }
    );
  }
}
