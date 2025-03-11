package io.confluent.idesidecar.restapi.messageviewer.strategy;

import io.confluent.idesidecar.restapi.clients.SchemaRegistryClients;
import io.confluent.idesidecar.restapi.exceptions.ProcessorFailedException;
import io.confluent.idesidecar.restapi.messageviewer.KafkaConsumerFactory;
import io.confluent.idesidecar.restapi.messageviewer.RecordDeserializer;
import io.confluent.idesidecar.restapi.messageviewer.SimpleConsumer;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionRequest;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionResponse;
import io.confluent.idesidecar.restapi.proxy.KafkaRestProxyContext;
import io.quarkus.logging.Log;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.Response.Status;
import java.util.Optional;
import java.util.regex.Pattern;
import org.apache.kafka.common.KafkaException;

/**
 * Handles consuming from a Kafka topic for the message viewer API, using the Kafka native
 * consumer.
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

  static final Pattern SNAPPY_CONSUME_FAILURE_PATTERN = Pattern.compile(
      "Received exception when fetching the next record from snappy-\\d+\\. "
          + "If needed, please seek past the record to continue consumption\\."
  );

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
    } catch (KafkaException e) {
      handleKafkaException(e, context);
    }
    return context;
  }

  /**
   * Handles {@link KafkaException}s that were thrown when consuming records. Return a meaningful
   * error message if the exception was thrown because the records were compressed with Snappy.
   * Otherwise, re-throw the exception.
   *
   * @param e       the KafkaException that was thrown during consumption
   * @param context the KafkaRestProxyContext containing the request and response information
   * @throws ProcessorFailedException if consuming records compressed with Snappy has failed
   * @throws KafkaException           if the exception is not related to Snappy compression
   */
  void handleKafkaException(
      KafkaException e,
      KafkaRestProxyContext<SimpleConsumeMultiPartitionRequest,
          SimpleConsumeMultiPartitionResponse> context
  ) {
    // Return an error if we tried consuming a record that was compressed with snappy
    // Otherwise, re-throw the KafkaException
    if (SNAPPY_CONSUME_FAILURE_PATTERN.matcher(e.getMessage()).matches()) {
      Log.errorf(
          "Failed to consume records from topic %s due to snappy compression.",
          context.getTopicName()
      );
      throw new ProcessorFailedException(
          context.failf(
              Status.INTERNAL_SERVER_ERROR.getStatusCode(),
              "At the moment, we don't support consuming records that were compressed with snappy. "
                  + "Please visit https://github.com/confluentinc/ide-sidecar/issues/304 to learn "
                  + "more."
          )
      );
    } else {
      throw new KafkaException(e.getMessage(), e.getCause());
    }
  }
}