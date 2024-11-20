package io.confluent.idesidecar.restapi.messageviewer;

import io.confluent.idesidecar.restapi.clients.ClientConfigurator;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

@ApplicationScoped
public class KafkaConsumerFactory {
  private static final ByteArrayDeserializer BYTE_ARRAY_DESERIALIZER = new ByteArrayDeserializer();

  @Inject
  ClientConfigurator configurator;

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
    // Generate the Kafka consumer configuration
    var config = configurator.getConsumerClientConfig(
        connectionId,
        clusterId,
        false
    );
    // And apply the overrides
    configOverrides.forEach((key, value) -> config.put(key.toString(), value.toString()));

    Log.debugf(
        "Creating consumer for connection %s and cluster %s with configuration:\n  %s",
        connectionId,
        clusterId,
        config
    );
    // And create the consumer
    return new KafkaConsumer<>(config.asMap(), BYTE_ARRAY_DESERIALIZER, BYTE_ARRAY_DESERIALIZER);
  }
}
