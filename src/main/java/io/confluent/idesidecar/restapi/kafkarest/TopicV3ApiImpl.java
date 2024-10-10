package io.confluent.idesidecar.restapi.kafkarest;

import io.confluent.idesidecar.restapi.kafkarest.api.TopicV3Api;
import io.confluent.idesidecar.restapi.kafkarest.model.*;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import org.apache.kafka.clients.admin.TopicDescription;

import java.util.Optional;
import java.util.Set;

import static io.confluent.idesidecar.restapi.kafkarest.RelationshipUtil.*;
import static io.confluent.idesidecar.restapi.kafkarest.RelationshipUtil.forTopic;
import static io.confluent.idesidecar.restapi.util.MutinyUtil.uniItem;

@RequestScoped
public class TopicV3ApiImpl implements TopicV3Api {

  @Inject
  TopicManagerImpl topicManager;

  @Override
  public Uni<TopicData> createKafkaTopic(
      String clusterId, CreateTopicRequestData createTopicRequestData
  ) {
    return topicManager
        .createKafkaTopic(clusterId, createTopicRequestData)
        .onItem()
        .transform(topicDescription -> fromTopicDescription(clusterId, topicDescription));
  }

  @Override
  public Uni<Void> deleteKafkaTopic(String clusterId, String topicName) {
    return topicManager.deleteKafkaTopic(clusterId, topicName);
  }

  @Override
  public Uni<TopicData> getKafkaTopic(
      String clusterId, String topicName, Boolean includeAuthorizedOperations
  ) {
    return topicManager
        .getKafkaTopic(clusterId, topicName, includeAuthorizedOperations)
        .onItem()
        .transform(topicDescription -> fromTopicDescription(clusterId, topicDescription));
  }

  @Override
  public Uni<TopicDataList> listKafkaTopics(String clusterId, Boolean includeAuthorizedOperations) {
    return topicManager.listKafkaTopics(clusterId, includeAuthorizedOperations)
            .onItem().transformToUni(topicDescriptionMap -> uniItem(TopicDataList
            .builder()
            .kind("KafkaTopicList")
            .metadata(ResourceCollectionMetadata
                .builder()
                .next(null)
                .self(forTopics(clusterId).getRelated())
                .build()
            )
            .data(topicDescriptionMap
                .stream()
                .map(t -> fromTopicDescription(clusterId, t))
                .toList()
            ).build())
        );
  }

  @Override
  public Uni<TopicData> updatePartitionCountKafkaTopic(
      String clusterId,
      String topicName,
      UpdatePartitionCountRequestData updatePartitionCountRequestData
  ) {
    throw new UnsupportedOperationException("Not implemented yet");
  }


  private static TopicData fromTopicDescription(String clusterId, TopicDescription topicDescription) {
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

  private static ResourceMetadata getTopicMetadata(String clusterId, String topicName) {
    return ResourceMetadata
        .builder()
        // TODO: Construct resource name based on the connection/cluster type
        .resourceName(null)
        .self(forTopic(clusterId, topicName).getRelated())
        .build();
  }
}
