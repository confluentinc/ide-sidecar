package io.confluent.idesidecar.restapi.kafkarest.controllers;

import static io.confluent.idesidecar.restapi.kafkarest.controllers.RelationshipUtil.forAcls;
import static io.confluent.idesidecar.restapi.kafkarest.controllers.RelationshipUtil.forAllPartitionReassignments;
import static io.confluent.idesidecar.restapi.kafkarest.controllers.RelationshipUtil.forBrokerConfigs;
import static io.confluent.idesidecar.restapi.kafkarest.controllers.RelationshipUtil.forBrokers;
import static io.confluent.idesidecar.restapi.kafkarest.controllers.RelationshipUtil.forClusters;
import static io.confluent.idesidecar.restapi.kafkarest.controllers.RelationshipUtil.forConsumerGroups;
import static io.confluent.idesidecar.restapi.kafkarest.controllers.RelationshipUtil.forController;
import static io.confluent.idesidecar.restapi.kafkarest.controllers.RelationshipUtil.forTopics;
import static io.confluent.idesidecar.restapi.kafkarest.controllers.RelationshipUtil.getClusterCrn;
import static io.confluent.idesidecar.restapi.util.MutinyUtil.uniItem;
import static io.confluent.idesidecar.restapi.util.MutinyUtil.uniStage;

import io.confluent.idesidecar.restapi.exceptions.ClusterNotFoundException;
import io.confluent.idesidecar.restapi.kafkarest.model.ClusterData;
import io.confluent.idesidecar.restapi.kafkarest.model.ClusterDataList;
import io.confluent.idesidecar.restapi.kafkarest.model.ResourceCollectionMetadata;
import io.confluent.idesidecar.restapi.kafkarest.model.ResourceMetadata;
import io.smallrye.mutiny.Uni;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.Node;

public class ClusterManagerImpl implements ClusterManager {

  private final AdminClient adminClient;

  public ClusterManagerImpl(Properties adminClientConfig) {
    this.adminClient = AdminClient.create(adminClientConfig);
  }

  public ClusterManagerImpl(AdminClient adminClient) {
    this.adminClient = adminClient;
  }

  @Override
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

  @Override
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
          ).invoke(ignored -> adminClient.close());
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
