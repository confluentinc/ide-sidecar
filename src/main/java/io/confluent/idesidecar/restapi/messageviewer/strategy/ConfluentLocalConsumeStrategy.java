package io.confluent.idesidecar.restapi.messageviewer.strategy;

import io.confluent.idesidecar.restapi.exceptions.ProcessorFailedException;
import io.confluent.idesidecar.restapi.messageviewer.MessageViewerContext;
import io.confluent.idesidecar.restapi.messageviewer.SimpleConsumer;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionRequest;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionResponse;
import io.confluent.idesidecar.restapi.util.WebClientFactory;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.eclipse.microprofile.config.ConfigProvider;

/**
 * Handles consuming from a Confluent Local Kafka topic for the message viewer API.
 */
@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
@ApplicationScoped
public class ConfluentLocalConsumeStrategy implements ConsumeStrategy {
  @Inject
  public Vertx vertx;

  @Inject
  WebClientFactory webClientFactory;

  private static final String CONFLUENT_LOCAL_KAFKA_TOPIC_REST_URI = ConfigProvider
      .getConfig()
      .getOptionalValue(
          "outpost.connections.confluent-local.resources.kafka-topic-uri",
          String.class)
      .orElse("http://localhost:8082/v3/clusters/%s/topics/%s");

  @Override
  public Future<MessageViewerContext> execute(MessageViewerContext context) {
    if (context.getKafkaClusterInfo() == null
        || context.getKafkaClusterInfo().bootstrapServers().isEmpty()) {
      return Future.failedFuture(
          new ProcessorFailedException(
              context.fail(500,
                  "Internal Server Error: Unable to retrieve bootstrap servers "
                      + "for the local Kafka cluster.")));
    }

    String url = CONFLUENT_LOCAL_KAFKA_TOPIC_REST_URI.formatted(
        context.getClusterId(),
        context.getTopicName()
    );
    return webClientFactory
        .getWebClient()
        .getAbs(url)
        .send()
        .compose(
          response -> {
            if (response.statusCode() >= 400) {
              return Future.failedFuture(
                  new ProcessorFailedException(
                      context.fail(response.statusCode(),
                              response.bodyAsString())));
            }
            return vertx.executeBlocking(() -> consumeMessagesFromLocalKafka(context));
          },
          throwable -> {
            var failure = context
                .error("proxy_error", throwable.getMessage())
                .failf(500, "Something went wrong while proxying request");
            return Future.failedFuture(new ProcessorFailedException(failure));
          }
        );
  }

  public MessageViewerContext consumeMessagesFromLocalKafka(
      MessageViewerContext context) {
    SimpleConsumeMultiPartitionRequest request = context.getConsumeRequest();
    String topic = context.getTopicName();
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
        context.getKafkaClusterInfo().bootstrapServers());
    props.put(
        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
        context.getSchemaRegistryInfo().uri());

    SimpleConsumer simpleConsumer = new SimpleConsumer(props);
    SimpleConsumeMultiPartitionResponse simpleConsumeResp =
        new SimpleConsumeMultiPartitionResponse(
        context.getClusterId(),
        topic,
        simpleConsumer.consumeFromMultiplePartitions(
            topic,
            getRequestOffsets(request),
            request.fromBeginning(),
            request.timestamp(),
            request.maxPollRecords(),
            request.fetchMaxBytes(),
            request.messageMaxBytes()
        )
    );
    context.setConsumeResponse(simpleConsumeResp);
    return context;
  }

  private Map<Integer, Long> getRequestOffsets(final SimpleConsumeMultiPartitionRequest request) {
    return request.partitionOffsets() == null ? null :
        request.partitionOffsets().stream()
            .collect(Collectors.toMap(
                SimpleConsumeMultiPartitionRequest.PartitionOffset::partitionId,
                SimpleConsumeMultiPartitionRequest.PartitionOffset::offset
            ));
  }
}