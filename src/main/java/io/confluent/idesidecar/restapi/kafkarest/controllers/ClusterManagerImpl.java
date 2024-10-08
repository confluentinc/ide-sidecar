package io.confluent.idesidecar.restapi.kafkarest.controllers;

import static io.confluent.idesidecar.restapi.kafkarest.controllers.RelationshipUtil.forAcls;
import static io.confluent.idesidecar.restapi.kafkarest.controllers.RelationshipUtil.forAllPartitionReassignments;
import static io.confluent.idesidecar.restapi.kafkarest.controllers.RelationshipUtil.forBrokerConfigs;
import static io.confluent.idesidecar.restapi.kafkarest.controllers.RelationshipUtil.forBrokers;
import static io.confluent.idesidecar.restapi.kafkarest.controllers.RelationshipUtil.forClusters;
import static io.confluent.idesidecar.restapi.kafkarest.controllers.RelationshipUtil.forConsumerGroups;
import static io.confluent.idesidecar.restapi.kafkarest.controllers.RelationshipUtil.forController;
import static io.confluent.idesidecar.restapi.kafkarest.controllers.RelationshipUtil.forPartitionReassignments;
import static io.confluent.idesidecar.restapi.kafkarest.controllers.RelationshipUtil.forPartitions;
import static io.confluent.idesidecar.restapi.kafkarest.controllers.RelationshipUtil.forTopic;
import static io.confluent.idesidecar.restapi.kafkarest.controllers.RelationshipUtil.forTopicConfigs;
import static io.confluent.idesidecar.restapi.kafkarest.controllers.RelationshipUtil.forTopics;
import static io.confluent.idesidecar.restapi.kafkarest.controllers.RelationshipUtil.getClusterCrn;
import static io.confluent.idesidecar.restapi.kafkarest.controllers.RelationshipUtil.getTopicCrn;
import static io.confluent.idesidecar.restapi.util.MutinyUtil.uniItem;
import static io.confluent.idesidecar.restapi.util.MutinyUtil.uniStage;

import io.confluent.idesidecar.restapi.exceptions.ClusterNotFoundException;
import io.confluent.idesidecar.restapi.kafkarest.model.ClusterData;
import io.confluent.idesidecar.restapi.kafkarest.model.ClusterDataList;
import io.confluent.idesidecar.restapi.kafkarest.model.CreateTopicRequestData;
import io.confluent.idesidecar.restapi.kafkarest.model.ResourceCollectionMetadata;
import io.confluent.idesidecar.restapi.kafkarest.model.ResourceMetadata;
import io.confluent.idesidecar.restapi.kafkarest.model.TopicData;
import io.confluent.idesidecar.restapi.kafkarest.model.TopicDataList;
import io.smallrye.mutiny.Uni;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;

public class ClusterManagerImpl {

  private final AdminClient adminClient;

  public ClusterManagerImpl(Properties adminClientConfig) {
    this.adminClient = AdminClient.create(adminClientConfig);
  }

  public Uni<ClusterData> getKafkaCluster(String clusterId) {
    return describeCluster()
        .chain(cid -> {
          if (!cid.id().equals(clusterId)) {
            return Uni.createFrom().failure(new ClusterNotFoundException(
                "Kafka cluster '%s' not found.".formatted(clusterId)
            ));
          }
          return uniItem(cid);
        })
        .map(this::fromClusterId);
  }

  public Uni<ClusterDataList> listKafkaClusters() {
    return describeCluster()
        .chain(cluster -> uniItem(ClusterDataList
            .builder()
            .metadata(ResourceCollectionMetadata
                .builder()
                .self(forClusters().getRelated())
                // We don't support pagination
                .next(null)
                .build()
            )
            .kind("KafkaClusterList")
            .data(List.of(fromClusterId(cluster)))
            .build())
        );
  }

  private Uni<ClusterDescribe> describeCluster() {
      var describeCluster = adminClient.describeCluster();
      return uniItem(new ClusterDescribe())
          .chain(
              cid -> uniStage(describeCluster.clusterId().toCompletionStage()).map(cid::withId))
          .chain(cid -> uniStage(describeCluster.controller().toCompletionStage()).map(Node::id)
              .map(cid::withControllerId)
          ).chain(cid -> uniStage(describeCluster.nodes().toCompletionStage())
              .map(cid::withNodes)
          );
  }

  private ClusterData fromClusterId(ClusterDescribe cluster) {
    return ClusterData
        .builder()
        .kind("KafkaCluster")
        .metadata(ResourceMetadata
            .builder()
            .self(getClusterCrn(cluster.id()).toString())
            .resourceName(cluster.id())
            .build()
        )
        .clusterId(cluster.id())
        .acls(forAcls(cluster.id()))
        .brokerConfigs(forBrokerConfigs(cluster.id()))
        .brokers(forBrokers(cluster.id()))
        .controller(forController(cluster.id(), cluster.controllerId()))
        .consumerGroups(forConsumerGroups(cluster.id()))
        .topics(forTopics(cluster.id()))
        .partitionReassignments(forAllPartitionReassignments(cluster.id()))
        .build();
  }

  public Uni<TopicData> createKafkaTopic(String clusterId,
      CreateTopicRequestData createTopicRequestData) {
    return getKafkaCluster(clusterId)
        .chain(ignored -> uniStage(
            adminClient.createTopics(List.of(new NewTopic(
                createTopicRequestData.getTopicName(),
                Optional.ofNullable(createTopicRequestData.getPartitionsCount())
                    .orElse(1),
                Optional.ofNullable(createTopicRequestData.getReplicationFactor())
                    .orElse(1).shortValue()
            ))).all().toCompletionStage()))
        .chain(v -> getKafkaTopic(
            clusterId,
            createTopicRequestData.getTopicName(),
            false
        ));
  }

  public Uni<Void> deleteKafkaTopic(String clusterId, String topicName) {
    return getKafkaCluster(clusterId).chain(ignored ->
        uniStage(
            adminClient
                .deleteTopics(List.of(topicName))
                .all()
                .toCompletionStage())
    );
  }

  public Uni<TopicData> getKafkaTopic(
      String clusterId, String topicName, Boolean includeAuthorizedOperations
  ) {
    var describeTopicsOptions = new DescribeTopicsOptions()
        .includeAuthorizedOperations(
            Optional.ofNullable(includeAuthorizedOperations).orElse(false)
        );
    return getKafkaCluster(clusterId).chain(ignored -> uniStage(
        adminClient
            .describeTopics(List.of(topicName), describeTopicsOptions)
            .allTopicNames()
            .toCompletionStage()
    )
        .map(topicDescriptions -> topicDescriptions.values().iterator().next())
        .onItem().transform(topicDescription -> fromTopicDescription(clusterId, topicDescription)));
  }

  public Uni<TopicDataList> listKafkaTopics(String clusterId) {
    return getKafkaCluster(clusterId).chain(ignored -> uniStage(
        adminClient.listTopics().names().toCompletionStage()
    ).chain(topicNames -> uniStage(
            adminClient
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

  private record ClusterDescribe(String id, Integer controllerId, Collection<Node> nodes) {
    ClusterDescribe() {
      this(null, null, null);
    }

    ClusterDescribe withId(String id) {
      return new ClusterDescribe(id, controllerId, nodes);
    }

    ClusterDescribe withControllerId(Integer controllerId) {
      return new ClusterDescribe(id, controllerId, nodes);
    }

    ClusterDescribe withNodes(Collection<Node> nodes) {
      return new ClusterDescribe(id, controllerId, nodes);
    }
  }
}
