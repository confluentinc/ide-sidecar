package io.confluent.idesidecar.restapi.kafkarest;

import static io.confluent.idesidecar.restapi.kafkarest.RelationshipUtil.forPartitionReassignments;
import static io.confluent.idesidecar.restapi.kafkarest.RelationshipUtil.forPartitions;
import static io.confluent.idesidecar.restapi.kafkarest.RelationshipUtil.forTopic;
import static io.confluent.idesidecar.restapi.kafkarest.RelationshipUtil.forTopicConfigs;
import static io.confluent.idesidecar.restapi.kafkarest.RelationshipUtil.forTopics;
import static io.confluent.idesidecar.restapi.util.MutinyUtil.uniItem;
import static io.confluent.idesidecar.restapi.util.MutinyUtil.uniStage;
import static io.confluent.idesidecar.restapi.util.RequestHeadersConstants.CONNECTION_ID_HEADER;

import io.confluent.idesidecar.restapi.cache.AdminClients;
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
import java.util.function.Supplier;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;

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
  ClusterManagerImpl clusterManager;

  @Inject
  HttpServerRequest request;

  Supplier<String> connectionId = () -> request.getHeader(CONNECTION_ID_HEADER);

  @Override
  public Uni<TopicData> createKafkaTopic(String clusterId,
      CreateTopicRequestData createTopicRequestData) {
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
  public Uni<TopicData> getKafkaTopic(
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
            )
                .map(topicDescriptions -> topicDescriptions.values().iterator().next())
                .onItem()
                .transform(topicDescription -> fromTopicDescription(clusterId, topicDescription))
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

  @Override
  public Uni<TopicDataList> listKafkaTopics(String clusterId, Boolean includeAuthorizedOperations) {
    return clusterManager.getKafkaCluster(clusterId).chain(ignored -> uniStage(
        adminClients
            .getClient(connectionId.get(), clusterId).listTopics().names().toCompletionStage()
    ).chain(topicNames -> uniStage(
            adminClients.getClient(connectionId.get(), clusterId)
                .describeTopics(topicNames, getDescribeTopicsOptions(includeAuthorizedOperations))
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
        // TODO: Construct resource name based on the connection/cluster type
        .resourceName(null)
        .self(forTopic(clusterId, topicName).getRelated())
        .build();
  }

}
