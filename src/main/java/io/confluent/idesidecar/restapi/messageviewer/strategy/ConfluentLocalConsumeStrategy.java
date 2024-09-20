package io.confluent.idesidecar.restapi.messageviewer.strategy;

import io.confluent.idesidecar.restapi.exceptions.ProcessorFailedException;
import io.confluent.idesidecar.restapi.messageviewer.MessageViewerContext;
import io.confluent.idesidecar.restapi.messageviewer.SchemaRegistryClients;
import io.confluent.idesidecar.restapi.messageviewer.SimpleConsumer;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionRequest;
import io.confluent.idesidecar.restapi.messageviewer.data.SimpleConsumeMultiPartitionResponse;
import io.confluent.idesidecar.restapi.util.RequestHeadersConstants;
import io.confluent.idesidecar.restapi.util.WebClientFactory;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Arrays;
import java.util.Collections;
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

  @Inject
  SchemaRegistryClients schemaRegistryClients;

  private static final String DEFAULT_LOCAL_SCHEMA_REGISTRY_ID = "lsrc-local";

  private static final int SR_CACHE_SIZE = 10;

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

    // Initialize Schema Registry Client
    var schemaRegistryClient = schemaRegistryClients.getClient(
        context.getConnectionId(),
        DEFAULT_LOCAL_SCHEMA_REGISTRY_ID,
        () -> createSchemaRegistryClient(context)
    );

    SimpleConsumer simpleConsumer = new SimpleConsumer(props, schemaRegistryClient);
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

  /**
   * Creates a SchemaRegistryClient instance based on the MessageViewerContext.
   *
   * @param context The MessageViewerContext.
   * @return The created SchemaRegistryClient instance, or null if SchemaRegistryInfo is not
   *         available.
   */
  private SchemaRegistryClient createSchemaRegistryClient(MessageViewerContext context) {
    if (context.getSchemaRegistryInfo() == null) {
      return null;
    }
    var schemaRegistryUrl = context.getSchemaRegistryInfo().uri();
    if (schemaRegistryUrl == null || schemaRegistryUrl.isEmpty()) {
      return null;
    }

    var extraHeaders = Map.of(
        RequestHeadersConstants.CONNECTION_ID_HEADER, context.getConnectionId(),
        RequestHeadersConstants.CLUSTER_ID_HEADER, context.getSchemaRegistryInfo().id()
    );

    return new CachedSchemaRegistryClient(
        // We use the SR Rest Proxy provided by the sidecar itself
        Collections.singletonList(schemaRegistryUrl),
        SR_CACHE_SIZE,
        Arrays.asList(
            new ProtobufSchemaProvider(),
            new AvroSchemaProvider(),
            new JsonSchemaProvider()
        ),
        Collections.emptyMap(),
        extraHeaders
    );
  }
}