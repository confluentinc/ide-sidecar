package io.confluent.idesidecar.restapi.messageviewer.strategy;

import io.confluent.idesidecar.restapi.messageviewer.KafkaConsumerFactory;
import io.confluent.idesidecar.restapi.clients.SchemaRegistryClients;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionRequest;
import io.confluent.idesidecar.restapi.proxy.KafkaRestProxyContext;
import io.confluent.idesidecar.restapi.messageviewer.RecordDeserializer;
import io.confluent.idesidecar.restapi.messageviewer.SimpleConsumer;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionResponse;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Optional;

/**
 * Handles consuming from a Kafka topic for the message viewer API, using the
 * Kafka native consumer.
 */
@ApplicationScoped
public class NativeConsumeStrategy implements ConsumeStrategy {
  @Inject
  public Vertx vertx;

  @Inject
  KafkaConsumerFactory consumerFactory;

  @Inject
  RecordDeserializer recordDeserializer;

  @Inject
  SchemaRegistryClients schemaRegistryClients;

  @Override
  public Future<KafkaRestProxyContext
      <SimpleConsumeMultiPartitionRequest, SimpleConsumeMultiPartitionResponse>>
  execute(KafkaRestProxyContext
              <SimpleConsumeMultiPartitionRequest, SimpleConsumeMultiPartitionResponse> context) {
    return vertx.executeBlocking(() -> consumeMessages(context));
  }

  public KafkaRestProxyContext
      <SimpleConsumeMultiPartitionRequest, SimpleConsumeMultiPartitionResponse>
  consumeMessages(KafkaRestProxyContext<SimpleConsumeMultiPartitionRequest,
      SimpleConsumeMultiPartitionResponse> context) {
    var request = context.getRequest();
    var topic = context.getTopicName();
    var schemaRegistryClient = Optional
        .ofNullable(context.getSchemaRegistryInfo())
        .map(info -> schemaRegistryClients.getClient(
            context.getConnectionId(), info.id()))
        .orElse(null);

    var consumer = consumerFactory.getClient(
        context.getConnectionId(),
        context.getClusterId(),
        request.consumerConfigOverrides()
    );
    try (consumer) {
      var simpleConsumer = new SimpleConsumer(
          consumer,
          schemaRegistryClient,
          recordDeserializer,
          context
      );
      var consumedData = simpleConsumer.consume(topic, request);
      context.setResponse(new SimpleConsumeMultiPartitionResponse(
          context.getClusterId(), topic, consumedData)
      );
    }
    return context;
  }
}