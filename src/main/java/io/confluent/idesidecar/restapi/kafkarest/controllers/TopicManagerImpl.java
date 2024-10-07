package io.confluent.idesidecar.restapi.kafkarest.controllers;

import static io.confluent.idesidecar.restapi.kafkarest.controllers.RelationshipUtil.forPartitionReassignments;
import static io.confluent.idesidecar.restapi.kafkarest.controllers.RelationshipUtil.forPartitions;
import static io.confluent.idesidecar.restapi.kafkarest.controllers.RelationshipUtil.forTopic;
import static io.confluent.idesidecar.restapi.kafkarest.controllers.RelationshipUtil.forTopicConfigs;
import static io.confluent.idesidecar.restapi.kafkarest.controllers.RelationshipUtil.forTopics;
import static io.confluent.idesidecar.restapi.kafkarest.controllers.RelationshipUtil.getTopicCrn;
import static io.confluent.idesidecar.restapi.util.MutinyUtil.uniItem;
import static io.confluent.idesidecar.restapi.util.MutinyUtil.uniStage;

import io.confluent.idesidecar.restapi.cache.AdminClients;
import io.confluent.idesidecar.restapi.kafkarest.model.CreateTopicRequestData;
import io.confluent.idesidecar.restapi.kafkarest.model.ResourceCollectionMetadata;
import io.confluent.idesidecar.restapi.kafkarest.model.ResourceMetadata;
import io.confluent.idesidecar.restapi.kafkarest.model.TopicData;
import io.confluent.idesidecar.restapi.kafkarest.model.TopicDataList;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;

@ApplicationScoped
public class TopicManagerImpl implements TopicManager {
  @Inject
  ClusterManager clusterManager;

  @Inject
  AdminClients adminClients;

  @Override
  public Uni<TopicData> createKafkaTopic(String connectionId, String clusterId,
      CreateTopicRequestData createTopicRequestData) {
    return clusterManager.getKafkaCluster(connectionId, clusterId)
        .chain(ignored -> uniStage(
            adminClients.getAdminClient(connectionId, clusterId).createTopics(List.of(new NewTopic(
            createTopicRequestData.getTopicName(),
            Optional.ofNullable(createTopicRequestData.getPartitionsCount())
                .orElse(1),
            Optional.ofNullable(createTopicRequestData.getReplicationFactor())
                .orElse(1).shortValue()
        ))).all().toCompletionStage()))
        .chain(v -> getKafkaTopic(
            connectionId,
            clusterId,
            createTopicRequestData.getTopicName(),
            false
            ));
  }

  @Override
  public Uni<Void> deleteKafkaTopic(String connectionId, String clusterId, String topicName) {
    return clusterManager.getKafkaCluster(connectionId, clusterId).chain(ignored ->
        uniStage(
        adminClients
            .getAdminClient(connectionId, clusterId)
            .deleteTopics(List.of(topicName))
            .all()
            .toCompletionStage())
    );
  }

  @Override
  public Uni<TopicData> getKafkaTopic(
      String connectionId, String clusterId, String topicName, Boolean includeAuthorizedOperations
  ) {
    var describeTopicsOptions = new DescribeTopicsOptions()
        .includeAuthorizedOperations(
            Optional.ofNullable(includeAuthorizedOperations).orElse(false)
        );
    return clusterManager.getKafkaCluster(connectionId, clusterId).chain(ignored -> uniStage(
        adminClients.getAdminClient(connectionId, clusterId)
            .describeTopics(List.of(topicName), describeTopicsOptions)
            .allTopicNames()
            .toCompletionStage()
    )
        .map(topicDescriptions -> topicDescriptions.values().iterator().next())
        .onItem().transform(topicDescription -> fromTopicDescription(clusterId, topicDescription)));
  }

  @Override
  public Uni<TopicDataList> listKafkaTopics(String connectionId, String clusterId) {
    return clusterManager.getKafkaCluster(connectionId, clusterId).chain(ignored -> uniStage(
        adminClients.getAdminClient(connectionId, clusterId).listTopics().names().toCompletionStage()
    ).chain(topicNames -> uniStage(
            adminClients
                .getAdminClient(connectionId, clusterId)
                .describeTopics(topicNames)
                .allTopicNames()
                .toCompletionStage())
        ).onItem()
        .transformToUni(topicDescriptionMap -> uniItem(TopicDataList
            .builder()
            .kind("KafkaTopicList")
            .metadata(ResourceCollectionMetadata
                .builder()
                .next(null)
                .self(forTopics(clusterId).getRelated())
                .build()
            )
            .data(topicDescriptionMap
                .values()
                .stream()
                .map(t -> fromTopicDescription(clusterId, t))
                .toList()
            ).build())
        ));
  }

  private TopicData fromTopicDescription(String clusterId, TopicDescription topicDescription) {
    return TopicData
        .builder()
        .kind("KafkaTopic")
        .topicName(topicDescription.name())
        .clusterId(clusterId)
        .partitionsCount(topicDescription.partitions().size())
        .replicationFactor(topicDescription.partitions().getFirst().replicas().size())
        .isInternal(topicDescription.isInternal())
        .authorizedOperations(
            Optional.ofNullable(topicDescription.authorizedOperations()).orElse(Set.of())
                .stream().map(Enum::name).toList()
        )
        .partitionReassignments(forPartitionReassignments(clusterId, topicDescription.name()))
        .partitions(forPartitions(clusterId, topicDescription.name()))
        .configs(forTopicConfigs(clusterId, topicDescription.name()))
        .metadata(getTopicMetadata(clusterId, topicDescription.name())).build();
  }

  private ResourceMetadata getTopicMetadata(String clusterId, String topicName) {
    return ResourceMetadata
        .builder()
        .resourceName(getTopicCrn(clusterId, topicName).toString())
        .self(forTopic(clusterId, topicName).getRelated())
        .build();
  }
}
