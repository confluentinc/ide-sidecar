package io.confluent.idesidecar.restapi.messageviewer.strategy;

import io.confluent.idesidecar.restapi.cache.KafkaConsumerClients;
import io.confluent.idesidecar.restapi.cache.SchemaRegistryClients;
import io.confluent.idesidecar.restapi.exceptions.ProcessorFailedException;
import io.confluent.idesidecar.restapi.messageviewer.MessageViewerContext;
import io.confluent.idesidecar.restapi.messageviewer.RecordDeserializer;
import io.confluent.idesidecar.restapi.messageviewer.SimpleConsumer;
import io.confluent.idesidecar.restapi.messageviewer.data.ConsumeResponse;
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
  KafkaConsumerClients consumerClients;

  @Inject
  RecordDeserializer recordDeserializer;

  @Inject
  SchemaRegistryClients schemaRegistryClients;

  @Override
  public Future<MessageViewerContext> execute(MessageViewerContext context) {
    if (context.getKafkaClusterInfo() == null
        || context.getKafkaClusterInfo().bootstrapServers().isEmpty()) {
      return Future.failedFuture(
          new ProcessorFailedException(
              context.fail(500,
                  "Internal Server Error: Unable to retrieve bootstrap servers "
                      + "for the Kafka cluster %s".formatted(context.getClusterId())
              )
          )
      );
    }
    return vertx.executeBlocking(() -> consumeMessages(context));
  }

  public MessageViewerContext consumeMessages(MessageViewerContext context) {
    var request = context.getConsumeRequest();
    var topic = context.getTopicName();
    var schemaRegistryClient = Optional
        .ofNullable(context.getSchemaRegistryInfo())
        .map(info -> schemaRegistryClients.getClient(context.getConnectionId(), info.id()))
        .orElse(null);

    var consumer = consumerClients.getClient(
        context.getConnectionId(),
        context.getClusterId(),
        request.consumerConfigOverrides()
    );
    var simpleConsumer = new SimpleConsumer(
        consumer,
        schemaRegistryClient,
        recordDeserializer
    );
    var consumedData = simpleConsumer.consume(topic, request);
    context.setConsumeResponse(new ConsumeResponse(context.getClusterId(), topic, consumedData));
    return context;
  }
}