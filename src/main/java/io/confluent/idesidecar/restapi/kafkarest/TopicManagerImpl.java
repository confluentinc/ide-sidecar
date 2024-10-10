package io.confluent.idesidecar.restapi.kafkarest;

import static io.confluent.idesidecar.restapi.kafkarest.RelationshipUtil.forPartitionReassignments;
import static io.confluent.idesidecar.restapi.kafkarest.RelationshipUtil.forPartitions;
import static io.confluent.idesidecar.restapi.kafkarest.RelationshipUtil.forTopic;
import static io.confluent.idesidecar.restapi.kafkarest.RelationshipUtil.forTopicConfigs;
import static io.confluent.idesidecar.restapi.kafkarest.RelationshipUtil.forTopics;
import static io.confluent.idesidecar.restapi.util.MutinyUtil.uniItem;
import static io.confluent.idesidecar.restapi.util.MutinyUtil.uniStage;

import io.confluent.idesidecar.restapi.kafkarest.model.CreateTopicRequestData;
import io.confluent.idesidecar.restapi.kafkarest.model.ResourceCollectionMetadata;
import io.confluent.idesidecar.restapi.kafkarest.model.ResourceMetadata;
import io.confluent.idesidecar.restapi.kafkarest.model.TopicData;
import io.confluent.idesidecar.restapi.kafkarest.model.TopicDataList;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpServerRequest;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.internals.Topic;

/**
 * RequestScoped bean for managing Kafka topics. Creating the bean as {@link RequestScoped} allows
 * us to inject the {@link HttpServerRequest} which is used to get the connection ID from the
 * request headers.
 */
@RequestScoped
public class TopicManagerImpl extends Manager implements TopicManager {

  @Inject
  ClusterManagerImpl clusterManager;

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
        ));
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

  public Uni<List<TopicDescription>> listKafkaTopics(String clusterId, Boolean includeAuthorizedOperations) {
    return clusterManager
        .getKafkaCluster(clusterId)
        .chain(ignored -> uniStage(
            adminClients.getClient(connectionId.get(), clusterId).listTopics().names().toCompletionStage()
        ))
        .chain(topicNames -> uniStage(
            adminClients.getClient(connectionId.get(), clusterId)
                .describeTopics(topicNames, getDescribeTopicsOptions(includeAuthorizedOperations))
                .allTopicNames()
                .toCompletionStage())
        ).map(topicDescriptions -> topicDescriptions.values().stream().toList());
  }
}
