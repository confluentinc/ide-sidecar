package io.confluent.idesidecar.restapi.kafkarest;

import static io.confluent.idesidecar.restapi.util.MutinyUtil.uniStage;
import static io.confluent.idesidecar.restapi.util.RequestHeadersConstants.CONNECTION_ID_HEADER;

import io.confluent.idesidecar.restapi.cache.AdminClients;
import io.confluent.idesidecar.restapi.kafkarest.model.CreateTopicRequestData;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpServerRequest;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

/**
 * RequestScoped bean for managing Kafka topics. Creating the bean as {@link RequestScoped} allows
 * us to inject the {@link HttpServerRequest} which is used to get the connection ID from the
 * request headers.
 */
@RequestScoped
public class TopicManagerImpl implements TopicManager {
  @Inject
  AdminClients adminClients;

  @Inject
  HttpServerRequest request;

  Supplier<String> connectionId = () -> request.getHeader(CONNECTION_ID_HEADER);

  @Inject
  ClusterManager clusterManager;

  @Override
  public Uni<TopicDescription> createKafkaTopic(
      String clusterId,
      CreateTopicRequestData createTopicRequestData
  ) {
    return clusterManager.getKafkaCluster(clusterId)
        .chain(ignored -> uniStage(
            adminClients
                .getClient(connectionId.get(), clusterId)
                .createTopics(List.of(new NewTopic(
                    createTopicRequestData.getTopicName(),
                    Optional.ofNullable(createTopicRequestData.getPartitionsCount())
                        .orElse(1),
                    Optional.ofNullable(createTopicRequestData.getReplicationFactor())
                        .orElse(1).shortValue())
                )).all().toCompletionStage()))
        .chain(v -> getKafkaTopic(
            clusterId,
            createTopicRequestData.getTopicName(),
            false
            )
            // The topic may not be immediately available after creation, so we retry a few times
            // This is also recommended in the Javadoc for UnknownTopicOrPartitionException
            // "This exception is retryable because the topic or partition might
            // subsequently be created."
            .onFailure(UnknownTopicOrPartitionException.class)
            .retry()
            // Exponential backoff with a max of 3 retries
            // Initial delay of 150ms, max delay of 1s
            .withBackOff(Duration.ofMillis(150), Duration.ofMillis(1000))
            .atMost(3)
        );
  }

  @Override
  public Uni<Void> deleteKafkaTopic(String clusterId, String topicName) {
    return clusterManager.getKafkaCluster(clusterId).chain(ignored ->
        uniStage(
            adminClients.getClient(connectionId.get(), clusterId)
                .deleteTopics(List.of(topicName))
                .all()
                .toCompletionStage())
    );
  }

  @Override
  public Uni<TopicDescription> getKafkaTopic(
      String clusterId, String topicName, Boolean includeAuthorizedOperations
  ) {
    return clusterManager
        .getKafkaCluster(clusterId)
        .chain(ignored ->
            uniStage(
                adminClients
                    .getClient(connectionId.get(), clusterId)
                    .describeTopics(
                        List.of(topicName), getDescribeTopicsOptions(includeAuthorizedOperations))
                    .allTopicNames()
                    .toCompletionStage()
            ).map(topicDescriptions -> topicDescriptions.values().iterator().next())
        );
  }

  private static DescribeTopicsOptions getDescribeTopicsOptions(
      Boolean includeAuthorizedOperations
  ) {
    return new DescribeTopicsOptions()
        .includeAuthorizedOperations(
            Optional.ofNullable(includeAuthorizedOperations).orElse(false)
        );
  }

  public Uni<List<TopicDescription>> listKafkaTopics(
      String clusterId, Boolean includeAuthorizedOperations
  ) {
    return clusterManager
        .getKafkaCluster(clusterId)
        .chain(ignored -> uniStage(
            adminClients
                .getClient(connectionId.get(), clusterId).listTopics().names().toCompletionStage()
        ))
        .chain(topicNames -> uniStage(
            adminClients
                .getClient(connectionId.get(), clusterId)
                .describeTopics(topicNames, getDescribeTopicsOptions(includeAuthorizedOperations))
                .allTopicNames()
                .toCompletionStage())
        ).map(topicDescriptions -> topicDescriptions.values().stream().toList());
  }
}
